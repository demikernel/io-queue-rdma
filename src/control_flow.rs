use rdma_cm::{CommunicationManager, ProtectionDomain, RegisteredMemory};

use crate::RECV_BUFFERS;

/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
pub type PrivateData = (u64, u32);

/// TODO: Move to own module. This should only be created from ControlFlow::create_connection_data
/// Since the volatile_send_window should be registered.
pub struct ConnectionData {
    memory: RegisteredMemory<[u64; 1]>,
}

impl ConnectionData {
    /// Several steps are necessary to connect both sides for one-sided RDMA control flow.
    /// This is the first step. Registers memory the other side will write to.
    /// TODO: Move this method to ConnectionData?
    pub fn new(pd: &mut ProtectionDomain) -> ConnectionData {
        let mut volatile_send_window: Box<[u64; 1]> = Box::new([0]);

        let memory = unsafe { pd.register_array(volatile_send_window) };
        ConnectionData { memory }
    }

    pub fn as_private_data(&self) -> PrivateData {
        unimplemented!()
        // (
        //     self.volatile_send_window.as_ref() as *const _ as u64,
        //     self.memory.get_rkey(),
        // )
    }
}

pub struct ControlFlow {
    /// Amount of allocated buffers left for receive requests.
    remaining_receive_window: u64,
    /// Amount of allocated buffers left on the other side.
    remaining_send_window: u64,
    /// Communication variable from other side.
    /// This variable is registered with the RDMA device and may be changed by the peer at any time.
    /// Probably needs to be made volatile in Rust... otherwise some UB might happen.
    /// Heap allocated to ensure it doesn't move?
    //volatile_send_window: RegisteredMemory<[u64; 1]>,
    /// Information required to communicate with other side.
    other_side: PrivateData,
    pub(crate) batch_size: u64,
}

impl ControlFlow {
    pub fn new(our_conn_data: ConnectionData, their_conn_data: PrivateData) -> ControlFlow {
        ControlFlow {
            remaining_receive_window: 0,
            // Other side will allocate same number of buffers we do.
            remaining_send_window: RECV_BUFFERS,
            // volatile_send_window: todo!(),
            // mr: our_conn_data.memory,
            other_side: their_conn_data,
            batch_size: RECV_BUFFERS,
        }
    }

    fn other_side_has_room(&self) -> bool {
        self.remaining_send_window > 0
    }

    /// Returns true if remaining send windows is less that 1/4 its original size.
    pub fn subtract_remaining_send_windows(&mut self) -> bool {
        self.remaining_send_window += 1;
        self.remaining_send_window < RECV_BUFFERS / 4
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

    pub fn add_recv_windows(&mut self, how_many: u64) {
        tracing::info!("{} new receive windows allocated!", how_many);
        self.remaining_receive_window += how_many;
    }
}
