use rdma_cm::{PeerConnectionData, QueuePair, RdmaMemory, VolatileRdmaMemory};

use crate::RECV_BUFFERS;
use std::array::IntoIter;

#[allow(unused_imports)]
use tracing::{debug, info, span, trace, Level};

pub struct ControlFlow {
    /// Amount of allocated buffers left for receive requests.
    remaining_receive_window: u64,
    /// Amount of allocated buffers left on the other side. This is our local variable used
    /// for our own internal tracking. The other side will actually write to
    /// `volatile_send_window` when updating their send windows.
    pub remaining_send_window: u64,
    /// Our peer will write to this value when it allocates new receive buffers.
    /// We can read from this local value to see if the other side has allocated new recv buffers
    /// yet.
    volatile_send_window: VolatileRdmaMemory<u64, 1>,
    /// This is our peer's `volatile_send_window` which we write to when we have allocated
    /// new receive windows.
    other_side: PeerConnectionData<u64, 1>,
    /// Queue pair for this connection. Used for RDMA reads and writes.
    qp: QueuePair,
    /// One sided RDMA requires somewhere to read and write from. We use this memory.
    memory: Option<RdmaMemory<u64, 1>>,
    pub(crate) batch_size: u64,
}

impl Drop for ControlFlow {
    fn drop(&mut self) {
        debug!("{}", crate::function_name!());
    }
}

impl ControlFlow {
    pub fn new(
        qp: QueuePair,
        memory: RdmaMemory<u64, 1>,
        volatile_receive_window: VolatileRdmaMemory<u64, 1>,
        other_side: PeerConnectionData<u64, 1>,
    ) -> ControlFlow {
        ControlFlow {
            remaining_receive_window: 0,
            // Other side will allocate same number of buffers we do.
            remaining_send_window: 0,
            volatile_send_window: volatile_receive_window,
            other_side,
            qp,
            memory: Some(memory),
            batch_size: RECV_BUFFERS,
        }
    }

    pub fn other_side_recv_windows(&self) -> u64 {
        self.volatile_send_window.read()[0]
    }

    pub fn ack_peer_recv_windows(&mut self) {
        self.volatile_send_window.write(&[0]);
    }

    pub fn remaining_send_windows(&self) -> u64 {
        self.remaining_send_window
    }

    pub fn remaining_receive_windows(&self) -> u64 {
        self.remaining_receive_window
    }

    pub fn subtract_recv_windows(&mut self, how_many: u64) {
        assert!(self.remaining_receive_window >= how_many);
        self.remaining_receive_window -= how_many;
    }

    pub fn subtract_remaining_send_windows(&mut self, how_many: u64) {
        assert!(self.remaining_send_window >= how_many);
        self.remaining_send_window -= how_many;
    }

    /// Receive windows should only be added when we hit zero?
    pub fn add_recv_windows(&mut self, how_many: u64) {
        tracing::info!("add_recv_windows(how_many={})!", how_many);
        self.remaining_receive_window += how_many;

        let mut memory = self.memory.take().unwrap();
        memory.as_mut_slice(1)[0] = how_many;
        // This work_id number does not matter. The completion queue coroutine will not look at it.
        let wr = [(0, memory)];
        // We don't really care when this gets done... So no need for the completion queue to
        // inform us when it happens.
        self.qp
            .post_send(wr.iter(), self.other_side.as_rdma_write());

        // Give us back our memory.
        let (_, memory) = IntoIter::new(wr).next().unwrap();
        self.memory = Some(memory);
    }
}
