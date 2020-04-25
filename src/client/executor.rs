use super::{Client, ClientSession};

use std::{collections::HashSet, sync::Arc};

use bson::Document;
use lazy_static::lazy_static;
use time::PreciseTime;

use crate::{
    cmap::{Connection, StreamDescription},
    error::{ErrorKind, Result},
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::Operation,
    options::SelectionCriteria,
    sdam::{Server, SessionSupportStatus},
};

lazy_static! {
    static ref REDACTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("authenticate");
        hash_set.insert("saslstart");
        hash_set.insert("saslcontinue");
        hash_set.insert("getnonce");
        hash_set.insert("createuser");
        hash_set.insert("updateuser");
        hash_set.insert("copydbgetnonce");
        hash_set.insert("copydbsaslstart");
        hash_set.insert("copydb");
        hash_set
    };
}

impl Client {
    /// Execute the given operation.
    ///
    /// Server selection will performed using the criteria specified on the operation, if any, and
    /// an implicit session will be created if the operation and write concern are compatible with
    /// sessions.
    pub(crate) async fn execute_operation<T: Operation>(&self, op: T) -> Result<T::O> {
        let mut implicit_session = self.start_implicit_session(&op).await?;
        self.get_connection_and_execute_operation(op, implicit_session.as_mut())
            .await
    }

    /// Execute the given operation with an implicit session, returning an error if one cannot be
    /// created or the topology doesn't support them.
    ///
    /// Server selection will performed using the criteria specified on the operation, if any, and
    /// an implicit session will be created if the operation and write concern are compatible with
    /// sessions.
    ///
    /// If an implicit session was created, it will be returned as part of the result.
    pub(crate) async fn execute_operation_with_implicit_sesssion<T: Operation>(
        &self,
        op: T,
    ) -> Result<(T::O, ClientSession)> {
        let implicit_session = self.start_implicit_session(&op).await?;

        match implicit_session {
            Some(mut implicit_session) => self
                .get_connection_and_execute_operation(op, Some(&mut implicit_session))
                .await
                .map(|result| (result, implicit_session)),
            None => Err(ErrorKind::InternalError {
                message: "failed to start an implicit session".to_string(),
            }
            .into()),
        }
    }

    #[allow(dead_code)]
    /// Execute the given operation with the given session.
    /// Server selection will performed using the criteria specified on the operation
    pub(crate) async fn execute_operation_with_session<T: Operation>(
        &self,
        op: T,
        session: &mut ClientSession,
    ) -> Result<T::O> {
        if !session.is_implicit() {
            if !op.supports_sessions() {
                return Err(ErrorKind::ArgumentError {
                    message: format!("{} does not support sessions", op.name()),
                }
                .into());
            }

            if !op.is_acknowledged() {
                return Err(ErrorKind::ArgumentError {
                    message: "Cannot use ClientSessions with unacknowledged write concern"
                        .to_string(),
                }
                .into());
            }
        }

        self.get_connection_and_execute_operation(op, Some(session))
            .await
    }

    /// Checks out a connection and executes the given operation, using the provided session
    /// if present.
    async fn get_connection_and_execute_operation<T: Operation>(
        &self,
        op: T,
        session: Option<&mut ClientSession>,
    ) -> Result<T::O> {
        let server = self.select_server(op.selection_criteria()).await?;

        let mut conn = match server.checkout_connection().await {
            Ok(conn) => conn,
            Err(err) => {
                self.inner
                    .topology
                    .handle_pre_handshake_error(err.clone(), server.address.clone())
                    .await;
                return Err(err);
            }
        };

        match self
            .execute_operation_on_connection(op, &mut conn, session)
            .await
        {
            Ok(result) => Ok(result),
            Err(err) => {
                self.inner
                    .topology
                    .handle_post_handshake_error(err.clone(), conn, server)
                    .await;
                Err(err)
            }
        }
    }

    /// Executes an operation on a given connection, using the provided session if present.
    async fn execute_operation_on_connection<T: Operation>(
        &self,
        op: T,
        connection: &mut Connection,
        session: Option<&mut ClientSession>,
    ) -> Result<T::O> {
        let stream_description: StreamDescription = connection.stream_description()?.clone();

        let mut cmd = op.build(&stream_description)?;
        self.inner
            .topology
            .update_command_with_read_pref(connection.address(), &mut cmd, op.selection_criteria())
            .await;

        // let mut implicit_session: Option<ClientSession> = None;
        // let mut session: Option<&mut ClientSession> = None;

        // // sessions cannot be used with unacknowledged writes.
        // let wc_supports_sessions =
        //     op.write_concern().map(WriteConcern::is_acknowledged) != Some(false);

        // if let Some(explicit_session) = explicit_session {
        //     if !op.supports_sessions() {
        //         return Err(ErrorKind::ArgumentError {
        //             message: format!("{} does not support sessions", cmd.name)
        //         }.into())
        //     }

        //     if !wc_supports_sessions {
        //         return Err(ErrorKind::ArgumentError {
        //             message: "Cannot use ClientSessions with unacknowledged write \
        //                       concern"
        //                 .to_string(),
        //         }.into())
        //     }

        //     session = Some(explicit_session);
        // } else {
        //     let status = self.get_session_support_status().await?;
        //     match status {
        //         SessionSupportStatus::Supported {
        //             logical_session_timeout,
        //         } if op.supports_sessions() && wc_supports_sessions => {
        //             implicit_session =
        // Some(self.start_session_with_timeout(logical_session_timeout).await);
        // session = Some(implicit_session.as_mut().unwrap());         },
        //         _ => {}
        //     }
        // }

        // if op.supports_sessions() {
        //     if let SessionSupportStatus::Supported {
        //         logical_session_timeout,
        //     } = self.get_session_support_status().await?
        //     {
        //         // We also verify that the server we're sending the command to supports sessions
        //         // just in case its monitor hasn't updated the topology description
        //         // yet.
        //         if let Some(server_timeout) = stream_description.logical_session_timeout {
        //             // sessions cannot be used with unacknowledged writes.
        //             let wc_supports_sessions =
        //                 op.write_concern().map(WriteConcern::is_acknowledged) != Some(false);

        //             match op.session() {
        //                 Some(_) if !wc_supports_sessions => {
        //                     return Err(ErrorKind::ArgumentError {
        //                         message: "Cannot use ClientSessions with unacknowledged write \
        //                                   concern"
        //                             .to_string(),
        //                     }
        //                     .into())
        //                 }
        //                 Some(explicit_session) => {
        //                     session = Some(explicit_session);
        //                 }
        //                 None if wc_supports_sessions => {
        //                     let timeout = std::cmp::min(server_timeout, logical_session_timeout);
        //                     implicit_session =
        // Some(self.start_session_with_timeout(timeout).await);                     session
        // = Some(implicit_session.as_mut().unwrap());                 }
        //                 None => {}
        //             }
        //         }
        //     }
        // }

        if let Some(ref session) = session {
            cmd.set_session(session);
        }

        let session_cluster_time = session.as_ref().and_then(|session| session.cluster_time());
        let client_cluster_time = self.inner.topology.cluster_time().await;
        let max_cluster_time = std::cmp::max(session_cluster_time, client_cluster_time.as_ref());
        if let Some(cluster_time) = max_cluster_time {
            cmd.set_cluster_time(cluster_time);
        }

        let connection_info = connection.info();
        let request_id = crate::cmap::conn::next_request_id();

        self.emit_command_event(|handler| {
            let should_redact = REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());

            let command_body = if should_redact {
                Document::new()
            } else {
                cmd.body.clone()
            };
            let command_started_event = CommandStartedEvent {
                command: command_body,
                db: cmd.target_db.clone(),
                command_name: cmd.name.clone(),
                request_id,
                connection: connection_info.clone(),
            };

            handler.handle_command_started_event(command_started_event);
        });

        let start_time = PreciseTime::now();

        let response_result = connection
            .send_command(cmd.clone(), request_id)
            .await
            .and_then(|response| {
                response.validate()?;
                Ok(response)
            });

        let end_time = PreciseTime::now();
        let duration = start_time.to(end_time).to_std()?;

        match response_result {
            Err(error) => {
                self.emit_command_event(|handler| {
                    let command_failed_event = CommandFailedEvent {
                        duration,
                        command_name: cmd.name,
                        failure: error.clone(),
                        request_id,
                        connection: connection_info,
                    };

                    handler.handle_command_failed_event(command_failed_event);
                });
                Err(error)
            }
            Ok(response) => {
                self.emit_command_event(|handler| {
                    let should_redact =
                        REDACTED_COMMANDS.contains(cmd.name.to_lowercase().as_str());
                    let reply = if should_redact {
                        Document::new()
                    } else {
                        response.raw_response.clone()
                    };

                    let command_succeeded_event = CommandSucceededEvent {
                        duration,
                        reply,
                        command_name: cmd.name.clone(),
                        request_id,
                        connection: connection_info,
                    };
                    handler.handle_command_succeeded_event(command_succeeded_event);
                });
                if let Some(cluster_time) = response.cluster_time() {
                    self.inner.topology.update_cluster_time(cluster_time).await;
                    if let Some(session) = session {
                        session.advance_cluster_time(cluster_time)
                    }
                }
                op.handle_response(response)
            }
        }
    }

    /// Start an implicit session if the operation and write concern are compatible with sesssions.
    async fn start_implicit_session<T: Operation>(&self, op: &T) -> Result<Option<ClientSession>> {
        match self.get_session_support_status().await? {
            SessionSupportStatus::Supported {
                logical_session_timeout,
            } if op.supports_sessions() && op.is_acknowledged() => Ok(Some(
                self.start_session_with_timeout(logical_session_timeout)
                    .await,
            )),
            _ => Ok(None),
        }
    }

    async fn get_session_support_status(&self) -> Result<SessionSupportStatus> {
        let initial_status = self.inner.topology.session_support_status().await;

        // Need to guarantee that we're connected to at least one server that can determine if
        // sessions are supported or not.
        match initial_status {
            SessionSupportStatus::Undetermined => {
                let criteria = SelectionCriteria::Predicate(Arc::new(move |server_info| {
                    server_info.server_type().is_data_bearing()
                }));
                let _: Arc<Server> = self.select_server(Some(&criteria)).await?;
                Ok(self.inner.topology.session_support_status().await)
            }
            _ => Ok(initial_status),
        }
    }
}
