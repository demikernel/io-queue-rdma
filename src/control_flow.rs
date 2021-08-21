use rdma_cm::{PeerConnectionData, VolatileRdmaMemory};

use crate::RECV_BUFFERS;

pub struct ControlFlow {
    /// Amount of allocated buffers left for receive requests.
    remaining_receive_window: u64,
    /// Amount of allocated buffers left on the other side.
    remaining_send_window: u64,
    /// Used to inform other side about when we allocate new recv buffers.
    volatile_receive_window: VolatileRdmaMemory<u64, 1>,
    /// Information required to communicate with other side.
    other_side: PeerConnectionData<u64, 1>,
    pub(crate) batch_size: u64,
}

impl ControlFlow {
    pub fn new(
        volatile_receive_window: VolatileRdmaMemory<u64, 1>,
        their_conn_data: PeerConnectionData<u64, 1>,
    ) -> ControlFlow {
        ControlFlow {
            remaining_receive_window: 0,
            // Other side will allocate same number of buffers we do.
            remaining_send_window: RECV_BUFFERS,
            volatile_receive_window,
            other_side: their_conn_data,
            batch_size: RECV_BUFFERS,
        }
    }

    fn other_side_recv_windows(&self) -> u64 {
        unimplemented!()
        // self.other_side_recv_windows()
    }

    pub fn remaining_send_window(&self) -> u64 {
        self.remaining_send_window
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
        tracing::info!("{} new receive windows allocated!", how_many);
        self.remaining_receive_window += how_many;

        // Other side should have read the previous value and set it to zero.
        // assert_eq!(self.volatile_receive_window.read(), 0);
        // Update receive window for other side.
        // self.volatile_receive_window
        //     .write(self.remaining_send_window);
    }
}
