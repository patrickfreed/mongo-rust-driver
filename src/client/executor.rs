use super::{Client, ClientSession};

use std::{collections::HashSet, sync::Arc};

use bson::Document;
use lazy_static::lazy_static;
use time::PreciseTime;

use crate::{
    cmap::{Connection, StreamDescription},
    error::{ErrorKind, Result},
    event::command::{CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent},
    operation::{Operation, OperationContext},
    options::{SelectionCriteria, WriteConcern},
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
    /// Executes an operation and returns the connection used to do so along with the result of the
    /// operation. This will be used primarily for the opening of exhaust cursors.
    #[allow(dead_code)]
    pub(crate) async fn execute_exhaust_operation<T: Operation>(
        &self,
        op: T,
    ) -> Result<(T::O, Connection)> {
        let server = self.select_server(op.selection_criteria()).await?;
        let mut conn = server.checkout_connection().await?;
        self.execute_operation_on_connection(op, &mut conn)
            .await
            .map(|r| (r, conn))
    }

    /// Execute the given operation, optionally specifying a connection used to do so.
    /// If no connection is provided, server selection will performed using the criteria specified
    /// on the operation, if any.
    pub(crate) async fn execute_operation<T: Operation>(&self, op: T) -> Result<T::O> {
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

        match self.execute_operation_on_connection(op, &mut conn).await {
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

    /// Executes an operation on a given connection.
    async fn execute_operation_on_connection<T: Operation>(
        &self,
        mut op: T,
        connection: &mut Connection,
    ) -> Result<T::O> {
        let stream_description: StreamDescription = connection.stream_description()?.clone();

        let mut cmd = op.build(&stream_description)?;
        self.inner
            .topology
            .update_command_with_read_pref(connection.address(), &mut cmd, op.selection_criteria())
            .await;

        let mut implicit_session: Option<ClientSession> = None;
        let mut session: Option<&mut ClientSession> = None;

        if op.supports_sessions() {
            if let SessionSupportStatus::Supported {
                logical_session_timeout,
            } = self.get_session_support_status().await?
            {
                // We also verify that the server we're sending the command to supports sessions
                // just in case its monitor hasn't updated the topology description
                // yet.
                if let Some(server_timeout) = stream_description.logical_session_timeout {
                    // sessions cannot be used with unacknowledged writes.
                    let wc_supports_sessions =
                        op.write_concern().map(WriteConcern::is_acknowledged) != Some(false);

                    match op.session() {
                        Some(_) if !wc_supports_sessions => {
                            return Err(ErrorKind::ArgumentError {
                                message: "Cannot use ClientSessions with unacknowledged write \
                                          concern"
                                    .to_string(),
                            }
                            .into())
                        }
                        Some(explicit_session) => {
                            session = Some(explicit_session);
                        },
                        None if wc_supports_sessions => {
                            let timeout = std::cmp::min(server_timeout, logical_session_timeout);
                            implicit_session = Some(self.start_session_with_timeout(timeout).await);
                            session = Some(implicit_session.as_mut().unwrap());
                        },
                        None => {}
                    }
                }
            }
        }

        if let Some(ref session) = session {
            cmd.set_session(session);
        }
        
        let session_cluster_time = session.as_ref().and_then(|session| session.cluster_time());
        let client_cluster_time = self.inner.cluster_time.read().await;
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
                    self.update_cluster_time(cluster_time).await;
                    if let Some(ref mut session) = session {
                        session.advance_cluster_time(cluster_time.clone())
                    }
                }
                op.handle_response(response, OperationContext { implicit_session })
            }
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
