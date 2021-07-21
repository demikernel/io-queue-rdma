use nix::sys::socket::SockAddr;
use rdma_cm;
use rdma_cm::{CommunicatioManager, CompletionQueue, MemoryRegion, RdmaCmEvent};
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;

/// Rust doesn't support async trait methods. So we use a struct instead?
pub struct QueueDescriptor {
    cm: rdma_cm::CommunicatioManager,

    // TODO a better API could avoid having these as options
    pd: Option<rdma_cm::ProtectionDomain>,
    qp: Option<rdma_cm::QueuePair>,
    cq: Option<rdma_cm::CompletionQueue>,
}

pub struct IoQueueMemory {
    mr: MemoryRegion,
    mem: Vec<u8>,
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
    /// Intializes RDMA by fetching the device?
    /// Allocates memory regions?
    pub fn socket(&self) -> QueueDescriptor {
        let cm = rdma_cm::CommunicatioManager::new();

        QueueDescriptor {
            cm,
            pd: None,
            qp: None,
            cq: None,
        }
    }

    pub fn bind(&mut self, qd: &mut QueueDescriptor, socket_address: &SockAddr) -> Result<(), ()> {
        qd.cm.bind(socket_address);
        Ok(())
    }

    pub fn connect(&mut self, qd: &mut QueueDescriptor) {
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

        qd.cm.resolve_route(0);
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
        event.ack();

        // Allocate pd, cq, and qp.
        let pd = qd.cm.allocate_pd();
        let mut cq = qd.cm.create_cq();
        let mut qp = qd.cm.create_qp(&pd, &cq);

        // TODO assert these fields haven't been set in struct.
        qd.pd = Some(pd);
        qd.cq = Some(cq);
        qd.qp = Some(qp);

        qd.cm.connect();
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::Established, event.get_event());
    }

    pub fn listen(&mut self, qd: &mut QueueDescriptor) {
        qd.cm.listen();
    }

    /// NOTE: Accept allocates a protection domain and queue descriptor internally for this id.
    /// And acks establishes connection.
    pub fn accept(&mut self, qd: &mut QueueDescriptor) -> QueueDescriptor {
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());
        let mut connected_id = event.get_connection_request_id();
        event.ack();

        let pd = connected_id.allocate_pd();
        let cq = connected_id.create_cq();
        let qp = connected_id.create_qp(&pd, &cq);

        connected_id.accept();
        let event = qd.cm.get_cm_event();
        assert_eq!(RdmaCmEvent::Established, event.get_event());
        event.ack();

        QueueDescriptor {
            cm: connected_id,
            pd: Some(pd),
            qp: Some(qp),
            cq: Some(cq),
        }
    }

    /// Allocate memory and register it with RDMA.
    /// TODO: This function should only be called once the protection domain has been allocated.
    pub fn malloc(&self, qd: &mut QueueDescriptor, elements: usize) -> IoQueueMemory {
        let mut mem: Vec<u8> = vec![0; elements];
        let mr = qd
            .cm
            .register_memory(qd.pd.as_mut().unwrap(), mem.deref_mut());

        IoQueueMemory { mr, mem }
    }

    /// We will need to use the lower level ibverbs interface to register UserArrays with
    /// RDMA on behalf of the user.
    pub fn push<'a>(&self, qd: &'a mut QueueDescriptor, mem: IoQueueMemory) -> QueueToken<'a> {
        // rebind mem to make it mutable, yuck.
        let mut mem = mem;
        qd.qp.as_mut().unwrap().post_send(&mut mem.mr, 0);

        QueueToken {
            mem,
            completion_queue: qd.cq.as_mut().unwrap(),
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
        let mut mem: Vec<u8> = vec![0; 1000];

        let mut mr = qd.cm.register_memory(
            qd.pd.as_ref().expect("Pd not initialized"),
            mem.as_mut_slice(),
        );

        // TODO set up windows for flow control between both sides.
        // TODO Count work request ids.
        qd.qp
            .as_mut()
            .expect("qp not initialized")
            .post_receive(&mut mr, 0);
        QueueToken {
            mem: IoQueueMemory { mr, mem },
            completion_queue: qd.cq.as_ref().expect("cq not initialized"),
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
