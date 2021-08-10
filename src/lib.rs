use std::ptr::null_mut;

use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm;
use rdma_cm::{
    CommunicationManager, CompletionQueue, ProtectionDomain, RdmaCmEvent, RegisteredMemory,
};

use crate::executor::{Executor, QueueToken, TaskHandle};
use control_flow::{ControlFlow, PeerConnectionData, VolatileRecvWindow};
use std::ffi::c_void;

mod control_flow;
mod executor;
mod utils;
mod waker;

use tracing::{debug, info, trace, Level};

/// Number of receive buffers to allocate per connection. This constant is also used when allocating
/// new buffers.
const RECV_BUFFERS: u64 = 100;
const SIZE: usize = 1000;

pub struct QueueDescriptor {
    cm: rdma_cm::CommunicationManager,
    // TODO a better API could avoid having these as options
    scheduler_handle: Option<TaskHandle>,
}

pub struct IoQueue {
    executor: executor::Executor<{ RECV_BUFFERS as usize }, { SIZE }>,
}

impl IoQueue {
    pub fn new() -> IoQueue {
        info!("{}", function_name!());
        IoQueue {
            executor: Executor::new(),
        }
    }
    /// Initializes RDMA by fetching the device?
    /// Allocates memory regions?
    pub fn socket(&self) -> QueueDescriptor {
        info!("{}", function_name!());

        let cm = rdma_cm::CommunicationManager::new().expect("TODO");

        QueueDescriptor {
            cm,
            scheduler_handle: None,
        }
    }

    pub fn bind(&mut self, qd: &mut QueueDescriptor, socket_address: &SockAddr) -> Result<(), ()> {
        info!("{}", function_name!());
        qd.cm.bind(socket_address);
        Ok(())
    }

    /// There is a lot of setup require for connecting. This function:
    /// 1) resolves address of connection.
    /// 2) resolves route.
    /// 3) Creates protection domain, completion queue, and queue pairs.
    /// 4) Establishes receive window communication.
    pub fn connect(&mut self, qd: &mut QueueDescriptor, address: InetAddr) {
        info!("{}", function_name!());

        IoQueue::resolve_address(qd, address);

        // Resolve route
        qd.cm.resolve_route(0);
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
        event.ack();

        // Allocate pd, cq, and qp.
        let mut pd = qd.cm.allocate_protection_domain().expect("TODO");
        let mut cq = qd.cm.create_cq(100).expect("TODO");
        let mut qp = qd.cm.create_qp(&pd, &cq);

        let mut our_recv_window = VolatileRecvWindow::new(&mut pd);
        let our_private_data = &our_recv_window.as_connection_data();
        // dbg!(our_private_data);
        qd.cm.connect::<PeerConnectionData>(Some(our_private_data));

        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());

        // Server sent us its send_window. Let's save it somewhere.
        let peer: PeerConnectionData = event.get_private_data().expect("Private data missing!");
        dbg!(peer);

        let cf = ControlFlow::new(our_recv_window, peer);
        qd.scheduler_handle = Some(self.executor.add_new_connection(cf, qp, pd, cq));
    }

    fn resolve_address(qd: &mut QueueDescriptor, address: InetAddr) {
        info!("{}", function_name!());

        // Get address info and resolve route!
        let addr_info = CommunicationManager::get_address_info(address).expect("TODO");
        let mut current = addr_info;

        // TODO: This will fail if the address is never found.
        let mut address_resolved = false;
        while current != null_mut() {
            match qd.cm.resolve_address((unsafe { *current }).ai_dst_addr) {
                Ok(_) => {
                    address_resolved = true;
                    break;
                }
                Err(_) => {}
            }

            unsafe {
                current = (*current).ai_next;
            }
        }
        if !address_resolved {
            panic!("Unable to resolve address {:?}", address);
        }
        // Ack address resolution.
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
        event.ack();
    }

    pub fn listen(&mut self, qd: &mut QueueDescriptor) {
        info!("{}", function_name!());

        qd.cm.listen();
    }

    /// NOTE: Accept allocates a protection domain and queue descriptor internally for this id.
    /// And acks establishes connection.
    pub fn accept(&mut self, qd: &mut QueueDescriptor) -> QueueDescriptor {
        info!("{}", function_name!());

        // Block until connection request arrives.
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());

        // New connection established! Use this  connection for RDMA communication.
        let mut connected_id = event.get_connection_request_id();
        let client_private_data = event
            .get_private_data::<PeerConnectionData>()
            .expect("Missing private data!");
        dbg!(client_private_data);
        event.ack();

        let mut pd = connected_id.allocate_protection_domain().expect("TODO");
        let cq = connected_id.create_cq(100).expect("TODO");
        let qp = connected_id.create_qp(&pd, &cq);

        // Now send our connection data to client.
        let mut volatile_recv_window = VolatileRecvWindow::new(&mut pd);
        let connection_data = volatile_recv_window.as_connection_data();

        // dbg!(our_private_data);
        connected_id.accept(Some(&connection_data));
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());
        event.ack();

        let control_flow = ControlFlow::new(volatile_recv_window, client_private_data);
        let scheduler_handle = self.executor.add_new_connection(control_flow, qp, pd, cq);

        QueueDescriptor {
            cm: connected_id,
            scheduler_handle: Some(scheduler_handle),
        }
    }

    /// Fetch a buffer from our pre-allocated memory pool.
    /// TODO: This function should only be called once the protection domain has been allocated.
    pub fn malloc(&mut self, qd: &mut QueueDescriptor) -> RegisteredMemory<u8, SIZE> {
        trace!("{}", function_name!());

        // TODO Do proper error handling. This expect means the connection was never properly
        // established via accept or connect. So we never added it to the executor.
        self.executor
            .malloc(qd.scheduler_handle.expect("Missing executor handle."))
    }

    pub fn free(&mut self, qd: &mut QueueDescriptor, memory: RegisteredMemory<u8, SIZE>) {
        trace!("{}", function_name!());
        // TODO Do proper error handling. This expect means the connection was never properly
        // established via accept or connect. So we never added it to the executor.
        self.executor.free(
            qd.scheduler_handle.expect("Missing executor handle."),
            memory,
        );
    }

    /// We will need to use the lower level ibverbs interface to register UserArrays with
    /// RDMA on behalf of the user.
    /// TODO: If user drops QueueToken we will be pointing to dangling memory... We should reference
    /// count he memory ourselves...
    pub fn push(
        &mut self,
        qd: &mut QueueDescriptor,
        mem: RegisteredMemory<u8, SIZE>,
    ) -> QueueToken {
        trace!("{}", function_name!());

        self.executor.push(qd.scheduler_handle.unwrap(), mem)
    }

    /// TODO: Bad things will happen if queue token is dropped as the memory registered with
    /// RDMA will be deallocated.
    pub fn pop(&mut self, qd: &mut QueueDescriptor) -> QueueToken {
        trace!("{}", function_name!());
        self.executor.pop(qd.scheduler_handle.unwrap())
    }

    pub fn wait(&mut self, qt: QueueToken) -> RegisteredMemory<u8, SIZE> {
        trace!("{}", function_name!());
        loop {
            self.executor.service_completion_queue(qt);
            match self.executor.wait(qt) {
                None => {
                    // TODO Have scheduler schedule relevant tasks.
                }
                Some(memory) => return memory,
            }
        }
    }

    pub fn disconnect(&mut self, mut qd: QueueDescriptor) {
        qd.cm.disconnect().unwrap();
        let event = qd.cm.get_cm_event().unwrap();
        assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
        event.ack();
    }
}
