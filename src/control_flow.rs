use rdma_cm::{CommunicationManager, ProtectionDomain, RegisteredMemory};

use crate::RECV_BUFFERS;

/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
#[derive(Clone, Copy, Debug)]
pub struct PeerConnectionData {
    remote_address: *mut u64,
    rkey: u32,
}

/// Used to inform other side about when we allocate new recv buffers.
pub struct VolatileRecvWindow {
    memory: RegisteredMemory<u64, 1>,
}

impl VolatileRecvWindow {
    /// Several steps are necessary to connect both sides for one-sided RDMA control flow.
    /// This is the first step. Registers memory the other side will write to.
    pub fn new(pd: &mut ProtectionDomain) -> VolatileRecvWindow {
        let mut volatile_send_window: Box<[u64; 1]> = Box::new([0]);

        let memory = pd.register_array(volatile_send_window);
        VolatileRecvWindow { memory }
    }

    pub fn as_connection_data(&mut self) -> PeerConnectionData {
        PeerConnectionData {
            remote_address: self.memory.memory.as_mut_ptr(),
            rkey: self.memory.get_rkey(),
        }
    }

    pub fn read(&mut self) -> u64 {
        unsafe { std::ptr::read_volatile(self.memory.memory.as_mut_ptr()) }
    }

    pub fn write(&mut self, new_value: u64) {
        let ptr = self.memory.memory.as_mut_ptr();
        unsafe { std::ptr::write_volatile(ptr, new_value) };
    }
}

pub struct ControlFlow {
    /// Amount of allocated buffers left for receive requests.
    remaining_receive_window: u64,
    /// Amount of allocated buffers left on the other side.
    remaining_send_window: u64,
    /// Used to inform other side about when we allocate new recv buffers.
    volatile_receive_window: VolatileRecvWindow,
    /// Information required to communicate with other side.
    other_side: PeerConnectionData,
    pub(crate) batch_size: u64,
}

impl ControlFlow {
    pub fn new(
        volatile_receive_window: VolatileRecvWindow,
        their_conn_data: PeerConnectionData,
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
