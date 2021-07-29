use rdma_cm::{CommunicatioManager, MemoryRegion, ProtectionDomain};

use crate::RECV_BUFFERS;

/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
pub type PrivateData = (u64, u32);

/// TODO: Move to own module. This should only be created from ControlFlow::create_connection_data
/// Since the volatile_send_window should be registered.
pub struct ConnectionData {
    volatile_send_window: Box<u64>,
    mr: MemoryRegion,
}

impl ConnectionData {
    /// Several steps are necessary to connect both sides for one-sided RDMA control flow.
    /// This is the first step. Registers memory the other side will write to.
    /// TODO: Move this method to ConnectionData?
    pub fn new(pd: &mut ProtectionDomain) -> ConnectionData {
        let mut volatile_send_window = Box::new(0);

        let mr = unsafe { CommunicatioManager::register_memory(pd, &mut volatile_send_window) };
        ConnectionData {
            volatile_send_window,
            mr,
        }
    }

    pub fn as_private_data(&self) -> PrivateData {
        (
            self.volatile_send_window.as_ref() as *const _ as u64,
            self.mr.get_rkey(),
        )
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
    volatile_send_window: Box<u64>,
    /// The `volatile_send_window` is registered with the RDMA device. This is the memory region
    /// corresponding to that registration.
    mr: MemoryRegion,
    /// Information required to communicate with other side.
    other_side: PrivateData,
}

impl ControlFlow {
    pub fn new(our_conn_data: ConnectionData, their_conn_data: PrivateData) -> ControlFlow {
        ControlFlow {
            remaining_receive_window: RECV_BUFFERS,
            // Other side will allocate same number of buffers we do.
            remaining_send_window: RECV_BUFFERS,
            volatile_send_window: our_conn_data.volatile_send_window,
            mr: our_conn_data.mr,
            other_side: their_conn_data,
        }
    }

    fn other_side_has_room(&self) -> bool {
        self.remaining_send_window > 0
    }

    pub fn get_avaliable_entries(&self) -> u64 {
        self.remaining_send_window
    }
}
