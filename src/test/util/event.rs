use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::sync::{
    broadcast::{RecvError, SendError},
    RwLockReadGuard,
};

use super::TestClient;
use crate::{
    bson::doc,
    event::{
        cmap::{CmapEventHandler, PoolClearedEvent, PoolReadyEvent},
        command::{
            CommandEventHandler,
            CommandFailedEvent,
            CommandStartedEvent,
            CommandSucceededEvent,
        },
    },
    options::ClientOptions,
    test::{CLIENT_OPTIONS, LOCK},
    RUNTIME,
};

pub type EventQueue<T> = Arc<RwLock<VecDeque<T>>>;
pub type CmapEvent = crate::cmap::test::event::Event;

#[derive(Clone, Debug, From)]
pub enum Event {
    CmapEvent(CmapEvent),
    CommandEvent(CommandEvent),
}

#[derive(Clone, Debug)]
pub enum CommandEvent {
    CommandStartedEvent(CommandStartedEvent),
    CommandSucceededEvent(CommandSucceededEvent),
    CommandFailedEvent(CommandFailedEvent),
}

impl CommandEvent {
    pub fn command_name(&self) -> &str {
        match self {
            CommandEvent::CommandStartedEvent(event) => event.command_name.as_str(),
            CommandEvent::CommandFailedEvent(event) => event.command_name.as_str(),
            CommandEvent::CommandSucceededEvent(event) => event.command_name.as_str(),
        }
    }

    pub fn is_command_started(&self) -> bool {
        matches!(self, CommandEvent::CommandStartedEvent(_))
    }

    fn request_id(&self) -> i32 {
        match self {
            CommandEvent::CommandStartedEvent(event) => event.request_id,
            CommandEvent::CommandFailedEvent(event) => event.request_id,
            CommandEvent::CommandSucceededEvent(event) => event.request_id,
        }
    }

    fn as_command_started(&self) -> Option<&CommandStartedEvent> {
        match self {
            CommandEvent::CommandStartedEvent(e) => Some(e),
            _ => None,
        }
    }

    fn as_command_succeeded(&self) -> Option<&CommandSucceededEvent> {
        match self {
            CommandEvent::CommandSucceededEvent(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventHandler {
    command_events: EventQueue<CommandEvent>,
    pub pool_cleared_events: EventQueue<PoolClearedEvent>,
    event_broadcaster: tokio::sync::broadcast::Sender<Event>,
}

impl EventHandler {
    pub fn new() -> Self {
        let (event_broadcaster, _) = tokio::sync::broadcast::channel(500);
        Self {
            command_events: Default::default(),
            pool_cleared_events: Default::default(),
            event_broadcaster,
        }
    }

    fn handle(&self, event: impl Into<Event>) {
        // this only errors if no receivers are listening which isn't a concern here.
        let _: std::result::Result<usize, SendError<Event>> =
            self.event_broadcaster.send(event.into());
    }

    fn subscribe(&self) -> EventSubscriber {
        EventSubscriber {
            _handler: self,
            receiver: self.event_broadcaster.subscribe(),
        }
    }
}

impl CmapEventHandler for EventHandler {
    fn handle_pool_cleared_event(&self, event: PoolClearedEvent) {
        self.handle(CmapEvent::ConnectionPoolCleared(event.clone()));
        self.pool_cleared_events.write().unwrap().push_back(event);
    }

    fn handle_pool_ready_event(&self, event: PoolReadyEvent) {
        self.handle(CmapEvent::ConnectionPoolReady(event))
    }
}

impl CommandEventHandler for EventHandler {
    fn handle_command_started_event(&self, event: CommandStartedEvent) {
        self.handle(CommandEvent::CommandStartedEvent(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::CommandStartedEvent(event))
    }

    fn handle_command_failed_event(&self, event: CommandFailedEvent) {
        self.handle(CommandEvent::CommandFailedEvent(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::CommandFailedEvent(event))
    }

    fn handle_command_succeeded_event(&self, event: CommandSucceededEvent) {
        self.handle(CommandEvent::CommandSucceededEvent(event.clone()));
        self.command_events
            .write()
            .unwrap()
            .push_back(CommandEvent::CommandSucceededEvent(event))
    }
}

pub struct EventSubscriber<'a> {
    /// A reference to the handler this subscriber is receiving events from.
    /// Stored here to ensure this subscriber cannot outlive the handler that is generating its
    /// events.
    _handler: &'a EventHandler,
    receiver: tokio::sync::broadcast::Receiver<Event>,
}

impl EventSubscriber<'_> {
    pub async fn wait_for_event<F>(&mut self, timeout: Duration, filter: F) -> Option<Event>
    where
        F: Fn(&Event) -> bool,
    {
        RUNTIME
            .timeout(timeout, async {
                loop {
                    match self.receiver.recv().await {
                        Ok(event) if filter(&event) => return event.into(),
                        // the channel hit capacity and the channnel will skip a few to catch up.
                        Err(RecvError::Lagged(_)) => continue,
                        Err(_) => return None,
                        _ => continue,
                    }
                }
            })
            .await
            .ok()
            .flatten()
    }
}

#[derive(Clone)]
pub struct EventClient {
    client: TestClient,
    handler: EventHandler,
}

impl std::ops::Deref for EventClient {
    type Target = TestClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl std::ops::DerefMut for EventClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl EventClient {
    pub async fn new() -> Self {
        EventClient::with_options(None).await
    }

    pub async fn with_options(options: impl Into<Option<ClientOptions>>) -> Self {
        let handler = EventHandler::new();
        let client = TestClient::with_handler(Some(handler.clone()), options).await;

        // clear events from commands used to set up client.
        handler.command_events.write().unwrap().clear();

        Self { client, handler }
    }

    pub async fn with_additional_options(
        options: Option<ClientOptions>,
        heartbeat_freq: Option<Duration>,
        use_multiple_mongoses: Option<bool>,
    ) -> Self {
        let mut options = match options {
            Some(mut options) => {
                options.hosts = CLIENT_OPTIONS.hosts.clone();
                options.merge(CLIENT_OPTIONS.clone());
                options
            }
            None => CLIENT_OPTIONS.clone(),
        };
        options.heartbeat_freq_test = heartbeat_freq;
        if TestClient::new().await.is_sharded() && use_multiple_mongoses != Some(true) {
            options.hosts = options.hosts.iter().cloned().take(1).collect();
        }
        EventClient::with_options(options).await
    }

    /// Gets the first started/succeeded pair of events for the given command name, popping off all
    /// events before and between them.
    ///
    /// Panics if the command failed or could not be found in the events.
    pub fn get_successful_command_execution(
        &self,
        command_name: &str,
    ) -> (CommandStartedEvent, CommandSucceededEvent) {
        let mut command_events = self.handler.command_events.write().unwrap();

        let mut started: Option<CommandStartedEvent> = None;

        while let Some(event) = command_events.pop_front() {
            if event.command_name() == command_name {
                match started {
                    None => {
                        let event = event
                            .as_command_started()
                            .unwrap_or_else(|| {
                                panic!("first event not a command started event {:?}", event)
                            })
                            .clone();
                        started = Some(event);
                        continue;
                    }
                    Some(started) if event.request_id() == started.request_id => {
                        let succeeded = event
                            .as_command_succeeded()
                            .expect("second event not a command succeeded event")
                            .clone();

                        return (started, succeeded);
                    }
                    _ => continue,
                }
            }
        }
        panic!("could not find event for {} command", command_name);
    }

    pub fn topology(&self) -> String {
        if self.client.is_sharded() {
            String::from("sharded")
        } else if self.client.is_replica_set() {
            String::from("replicaset")
        } else {
            String::from("single")
        }
    }

    /// Gets all of the command started events for a specified command name.
    pub fn get_command_started_events(&self, command_name: &str) -> Vec<CommandStartedEvent> {
        let events = self.handler.command_events.read().unwrap();
        events
            .iter()
            .filter_map(|event| match event {
                CommandEvent::CommandStartedEvent(event) => {
                    if event.command_name == command_name {
                        Some(event.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect()
    }

    pub fn get_command_events(&self, command_names: &[&str]) -> Vec<CommandEvent> {
        self.handler
            .command_events
            .write()
            .unwrap()
            .drain(..)
            .filter(|event| command_names.contains(&event.command_name()))
            .collect()
    }

    pub fn get_pool_cleared_events(&self) -> Vec<PoolClearedEvent> {
        self.handler
            .pool_cleared_events
            .write()
            .unwrap()
            .drain(..)
            .collect()
    }

    pub fn subscribe_to_events(&self) -> EventSubscriber<'_> {
        self.handler.subscribe()
    }
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn command_started_event_count() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let coll = client.database("foo").collection("bar");

    for i in 0..10 {
        coll.insert_one(doc! { "x": i }, None).await.unwrap();
    }

    assert_eq!(client.get_command_started_events("insert").len(), 10);
}
