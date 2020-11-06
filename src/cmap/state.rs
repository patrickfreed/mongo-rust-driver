use tokio::sync::watch;

/// Struct containing information about the latest known state of a
/// connection pool.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PoolState {
    pub(crate) total_connection_count: u32,
    pub(crate) available_connection_count: u32,
    pub(crate) wait_queue_length: u32,
}

/// Struct used to publish updates to the pool state.
#[derive(Debug)]
pub(super) struct PoolStatePublisher {
    sender: watch::Sender<PoolState>,
}

impl PoolStatePublisher {
    pub(super) fn new() -> (Self, PoolStateListener) {
        let (sender, receiver) = watch::channel(PoolState {
            available_connection_count: 0,
            total_connection_count: 0,
            wait_queue_length: 0,
        });

        (Self { sender }, PoolStateListener { receiver })
    }

    pub(super) fn publish(&self, new_state: PoolState) {
        // returns an error if no one is listening which can be ignored.
        let _: std::result::Result<_, _> = self.sender.broadcast(new_state);
    }
}

/// Struct used to get the latest published view of the pool's state.
#[derive(Debug, Clone)]
pub(super) struct PoolStateListener {
    receiver: watch::Receiver<PoolState>,
}

impl PoolStateListener {
    pub(super) fn latest(&self) -> PoolState {
        *self.receiver.borrow()
    }
}
