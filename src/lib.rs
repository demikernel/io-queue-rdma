use nix::sys::socket::SockAddr;
use rdma_cm;
use rdma_cm::{CommunicatioManager, CompletionQueue, MemoryRegion, ProtectionDomain, RdmaCmEvent};
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;

mod scheduler;

/// Number of receive buffers to allocate per connection. This constant is also used when allocating
/// new buffers.
const RECV_BUFFERS: u32 = 1024;

pub struct QueueDescriptor {
    cm: rdma_cm::CommunicatioManager,

    // TODO a better API could avoid having these as options
    protection_domain: Option<rdma_cm::ProtectionDomain>,
    queue_pair: Option<rdma_cm::QueuePair>,
    completion_queue: Option<rdma_cm::CompletionQueue>,
    /// Initialized only when connection is made.
    control_flow: Option<ControlFlow>,
}

/// Connection data transmitted through the private data struct fields by our `connect` and `accept`
/// function to set up one-sided RDMA. (u64, u32) are (address of volatile_send_window, rkey).
type ConnectionData = (u64, u32);

struct ControlFlow {
    /// Amount of allocated buffers left for receive requests.
    remaining_receive_window: u32,
    /// Amount of allocated buffers left on the other side.
    remaining_send_window: u32,
    /// Communication variable from other side.
    /// This variable is registered with the RDMA device and may be changed by the peer at any time.
    /// Probably needs to be made volatile in Rust... otherwise some UB might happen.
    /// Heap allocated to ensure it doesn't move?
    volatile_send_window: Box<u64>,
    /// The `volatile_send_window` is registered with the RDMA device. This is the memory region
    /// corresponding to that registration.
    mr: MemoryRegion,
}

impl ControlFlow {
    fn new(pd: &mut ProtectionDomain) -> ControlFlow {
        let mut volatile_send_window = Box::new(0);

        let mr = unsafe { CommunicatioManager::register_memory(pd, &mut volatile_send_window) };
        let mut cf = ControlFlow {
            remaining_receive_window: RECV_BUFFERS,
            remaining_send_window: RECV_BUFFERS,
            volatile_send_window,
            mr,
        };

        cf
    }

    fn get_connection_data(&self) -> ConnectionData {
        (
            self.volatile_send_window.as_ref() as *const _ as u64,
            self.mr.get_rkey(),
        )
    }
}

pub struct IoQueueMemory {
    mr: MemoryRegion,
    mem: Box<[u8]>,
}

impl Deref for IoQueueMemory {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.mem.deref()
    }
}

impl DerefMut for IoQueueMemory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mem.deref_mut()
    }
}

pub struct IoQueue {
    scheduler: (),
}

impl IoQueue {
    pub fn new() -> IoQueue {
        IoQueue { scheduler: () }
    }
    /// Initializes RDMA by fetching the device?
    /// Allocates memory regions?
    pub fn socket(&self) -> QueueDescriptor {
        let cm = rdma_cm::CommunicatioManager::new();

        QueueDescriptor {
            cm,
            protection_domain: None,
            queue_pair: None,
            completion_queue: None,
            control_flow: None,
        }
    }

    pub fn bind(&mut self, qd: &mut QueueDescriptor, socket_address: &SockAddr) -> Result<(), ()> {
        qd.cm.bind(socket_address);
        Ok(())
    }

    /// There is a lot of setup require for connecting. This function:
    /// 1) resolves address of connection.
    /// 2) resolves route.
    /// 3) Creates protection domain, completion queue, and queue pairs.
    /// 4) Establishes receive window communication.
    pub fn connect(&mut self, qd: &mut QueueDescriptor) {
        IoQueue::resolve_address(qd);

        // Resolve route
        qd.cm.resolve_route(0);
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
        event.ack();

        // Allocate pd, cq, and qp.
        let pd = qd.cm.allocate_pd();
        let mut cq = qd.cm.create_cq();
        let mut qp = qd.cm.create_qp(&pd, &cq);

        // TODO assert these fields haven't been set in struct.
        qd.protection_domain = Some(pd);
        qd.completion_queue = Some(cq);
        qd.queue_pair = Some(qp);

        qd.control_flow = Some(ControlFlow::new(qd.protection_domain.as_mut().unwrap()));

        let client_conn_data = &qd.control_flow.as_ref().unwrap().get_connection_data();
        dbg!(client_conn_data);

        qd.cm.connect::<ConnectionData>(Some(client_conn_data));
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::Established, event.get_event());

        // Server sent us its send_window. Let's save it somewhere.
        let server_conn_data = event.get_private_data::<ConnectionData>();
        dbg!(server_conn_data);
    }

    fn resolve_address(qd: &mut QueueDescriptor) {
        // Get address info and resolve route!
        let addr_info = CommunicatioManager::get_addr_info();
        let mut current = addr_info;

        while current != null_mut() {
            println!("Client: Resolving address...");
            let ret = qd.cm.resolve_addr(None, (unsafe { *current }).ai_dst_addr);

            if ret == 0 {
                println!("Client: Address resolved.");
                break;
            }
            unsafe {
                current = (*current).ai_next;
            }
        }

        // Ack address resolution.
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
        event.ack();
    }

    pub fn listen(&mut self, qd: &mut QueueDescriptor) {
        qd.cm.listen();
    }

    /// NOTE: Accept allocates a protection domain and queue descriptor internally for this id.
    /// And acks establishes connection.
    pub fn accept(&mut self, qd: &mut QueueDescriptor) -> QueueDescriptor {
        // Block until connection request arrives.
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());

        // New connection established! Use this  connection for RDMA communication.
        let mut connected_id = event.get_connection_request_id();
        let client_conn_data = event.get_private_data::<ConnectionData>();
        dbg!(client_conn_data);
        event.ack();

        let mut pd = connected_id.allocate_pd();
        let cq = connected_id.create_cq();
        let qp = connected_id.create_qp(&pd, &cq);

        let mut control_flow = ControlFlow::new(&mut pd);

        // Now send our connection data to client.
        let server_conn_data = &control_flow.get_connection_data();
        dbg!(server_conn_data);
        connected_id.accept(Some(server_conn_data));
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::Established, event.get_event());
        event.ack();

        QueueDescriptor {
            cm: connected_id,
            protection_domain: Some(pd),
            queue_pair: Some(qp),
            completion_queue: Some(cq),
            control_flow: Some(control_flow),
        }
    }

    /// Allocate memory and register it with RDMA.
    /// TODO: This function should only be called once the protection domain has been allocated.
    pub fn malloc(&self, qd: &mut QueueDescriptor, elements: usize) -> IoQueueMemory {
        let mut mem: Box<[u8]> = vec![0; elements].into_boxed_slice();
        let mr = qd
            .cm
            .register_memory_buffer(qd.protection_domain.as_mut().unwrap(), &mut mem);

        IoQueueMemory { mr, mem }
    }

    /// We will need to use the lower level ibverbs interface to register UserArrays with
    /// RDMA on behalf of the user.
    pub fn push<'a>(&self, qd: &'a mut QueueDescriptor, mem: IoQueueMemory) -> QueueToken<'a> {
        // rebind mem to make it mutable, yuck.
        let mut mem = mem;
        qd.queue_pair.as_mut().unwrap().post_send(&mut mem.mr, 0);

        QueueToken {
            mem,
            completion_queue: qd.completion_queue.as_mut().unwrap(),
            work_request_id: 0,
        }
    }

    /// TODO: Bad things will happen if queue token is dropped as the memory registered with
    /// RDMA will be deallocated.
    pub fn pop<'a>(&self, qd: &'a mut QueueDescriptor) -> QueueToken<'a> {
        // TODO: When should post received messages be put on the qp? Only when the programmers
        // calls pop? Or should we preemptively set some.

        // TODO: When is the right time to allocate receive buffers? Only when the user calls pop?
        // I doubt it. Registering memory with the device is expensive and requires a kernel
        // context switch.
        let mut mem: Box<[u8]> = vec![0; 1000].into_boxed_slice();

        let mut mr = qd.cm.register_memory_buffer(
            qd.protection_domain.as_ref().expect("Pd not initialized"),
            &mut mem,
        );

        // TODO set up windows for flow control between both sides.
        // TODO Count work request ids.
        qd.queue_pair
            .as_mut()
            .expect("qp not initialized")
            .post_receive(&mut mr, 0);
        QueueToken {
            mem: IoQueueMemory { mr, mem },
            completion_queue: qd.completion_queue.as_ref().expect("cq not initialized"),
            work_request_id: 0,
        }
    }

    pub fn wait(&self, qt: QueueToken) -> IoQueueMemory {
        loop {
            match qt.completion_queue.poll() {
                None => {}
                Some(entries) => {
                    // TODO do not drop other entries lol.
                    for e in entries {
                        assert_eq!(qt.work_request_id, e.wr_id, "Incorrect work request id.");
                        return qt.mem;
                    }
                }
            }
        }
    }
}

pub struct QueueToken<'a> {
    mem: IoQueueMemory,
    completion_queue: &'a CompletionQueue,
    work_request_id: u64,
}
