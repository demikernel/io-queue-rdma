use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

#[allow(unused_imports)]
use tracing::{debug, info, span, trace, Level};

use crate::function_name;

use rdma_cm::{
    CommunicatioManager, CompletionQueue, ProtectionDomain, QueuePair, RegisteredMemoryRef,
};

use crate::control_flow::ControlFlow;
use std::cmp::min;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

pub(crate) struct Executor<const N: usize, const SIZE: usize> {
    tasks: Vec<ConnectionTask>,
}

#[derive(Copy, Clone)]
pub struct QueueToken {
    work_id: u64,
    task_id: TaskHandle,
}

#[derive(Debug, Copy, Clone)]
pub struct TaskHandle(usize);

// Every connection has a push, pop, completion_queue, (and more) coroutines. A ControlFlow handle
// and
struct ConnectionTask {
    memory_pool: Rc<RefCell<VecDeque<RegisteredMemoryRef>>>,
    protection_domain: ProtectionDomain,

    push_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    recv_buffers_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    completions_coroutine: Pin<Box<dyn Future<Output = ()>>>,

    push_work_queue: Rc<RefCell<VecDeque<WorkRequest>>>,
    pop_work_queue: Rc<RefCell<VecDeque<WorkRequest>>>,

    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemoryRef>>>,
    completed_requests: Rc<RefCell<HashSet<u64>>>,

    next_pop_work_id: Receiver<u64>,

    control_flow: Rc<RefCell<ControlFlow>>,
    work_id_counter: Rc<RefCell<u64>>,
}

impl<const N: usize, const SIZE: usize> Executor<N, SIZE> {
    pub fn new() -> Executor<N, SIZE> {
        info!("{}", function_name!());
        Executor {
            tasks: Vec::with_capacity(100),
        }
    }

    pub fn add_new_connection(
        &mut self,
        control_flow: ControlFlow,
        queue_pair: QueuePair,
        mut protection_domain: ProtectionDomain,
        completion_queue: CompletionQueue,
    ) -> TaskHandle {
        info!("{}", function_name!());

        let push_work_queue = Rc::new(RefCell::new(VecDeque::with_capacity(1000)));
        let pop_work_queue = Rc::new(RefCell::new(VecDeque::with_capacity(1000)));

        let (ready_pop_work_id, next_pop_work_id) = std::sync::mpsc::channel::<u64>();

        let processed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));
        let completed_requests = Rc::new(RefCell::new(HashSet::with_capacity(1000)));

        let control_flow = Rc::new(RefCell::new(control_flow));
        let queue_pair = Rc::new(RefCell::new(queue_pair));
        let completion_queue = Rc::new(RefCell::new(completion_queue));

        // Create N chunks of SIZE guaranteed to be contiguous in memory! Then register this memory.
        // TODO: I'm still not sure what the right way to expose memory is. So I will handle doing
        // it properly later.
        // let mut memory = vec![0 as u8; N * SIZE].into_boxed_slice();
        // let registered_memory =
        //     CommunicatioManager::register_memory_buffer(&mut protection_domain, &mut memory);
        //
        // let memory: *mut u8 = Box::into_raw(memory) as *mut u8;
        // let mut memory_pool: Vec<RegisteredMemoryRef> = Vec::with_capacity(N);
        // for n in 0..N {
        //     unsafe {
        //         let mut new_slice: &[u8] =
        //             std::slice::from_raw_parts_mut(memory.add(n * SIZE), SIZE);
        //         let new_box: Box<[u8]> = Box::from_raw(new_slice.as_mut_ptr());
        //         let io_queue_memory =
        //             RegisteredMemoryRef::new(registered_memory.get_lkey(), new_box);
        //         memory_pool.push(io_queue_memory);
        //     }
        // }

        // Allocate four times as many vectors as our
        let mut memory_pool: VecDeque<RegisteredMemoryRef> = VecDeque::with_capacity(N);
        for _ in 0..4 * N {
            let mut memory = vec![0 as u8; N as usize * SIZE].into_boxed_slice();
            let registered_memory =
                CommunicatioManager::register_memory_buffer(&mut protection_domain, &mut memory);
            memory_pool.push_back(RegisteredMemoryRef::new(
                registered_memory.get_lkey(),
                memory,
            ));
        }
        let work_id_counter = Rc::new(RefCell::new(0));
        let memory_pool = Rc::new(RefCell::new(memory_pool));

        let mut ct = ConnectionTask {
            memory_pool: memory_pool.clone(),
            protection_domain,
            push_coroutine: Box::pin(push_coroutine(
                queue_pair.clone(),
                push_work_queue.clone(),
                control_flow.clone(),
                processed_requests.clone(),
            )),
            recv_buffers_coroutine: Box::pin(post_receive_coroutine(
                queue_pair,
                control_flow.clone(),
                memory_pool,
                processed_requests.clone(),
                work_id_counter.clone(),
                ready_pop_work_id,
            )),
            completions_coroutine: Box::pin(completions_coroutine(
                control_flow.clone(),
                completion_queue,
                completed_requests.clone(),
            )),
            push_work_queue,
            pop_work_queue,
            processed_requests,
            completed_requests,
            next_pop_work_id,
            control_flow,
            work_id_counter,
        };

        info!("Starting coroutines.");
        Self::schedule(&mut ct.recv_buffers_coroutine);
        // Self::schedule(&mut ct.push_coroutine);
        // Self::schedule(&mut ct.completions_coroutine);

        let current_task_id = self.tasks.len();
        self.tasks.push(ct);
        TaskHandle(current_task_id)
    }

    pub fn malloc(&mut self, task: TaskHandle) -> RegisteredMemoryRef {
        info!("{}", function_name!());

        self.tasks
            .get_mut(task.0)
            .expect(&format!("Missing task {:?}", task))
            .memory_pool
            .borrow_mut()
            .pop_front()
            .expect("Out of memory!")
    }

    // TODO Make sure this buffer actually belongs to this handle?
    pub fn free(&mut self, task: TaskHandle, memory: RegisteredMemoryRef) {
        self.tasks
            .get_mut(task.0)
            .expect(&format!("Missing task {:?}", task))
            .memory_pool
            .borrow_mut()
            .push_back(memory)
    }

    pub fn push(&mut self, task_handle: TaskHandle, mem: RegisteredMemoryRef) -> QueueToken {
        info!("{}", function_name!());

        let task: &mut ConnectionTask = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id: u64 = task.work_id_counter.borrow_mut().clone();

        *task.work_id_counter.borrow_mut() += 1;
        let work = WorkRequest {
            memory: mem,
            work_id,
        };
        task.push_work_queue.borrow_mut().push_back(work);
        // TODO: Is push coroutine called too often?
        Self::schedule(&mut task.push_coroutine);

        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    pub fn pop(&mut self, task_handle: TaskHandle) -> QueueToken {
        info!("{}", function_name!());

        let task: &mut ConnectionTask = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id = match task.next_pop_work_id.try_recv() {
            Ok(work_id) => work_id,
            Err(TryRecvError::Empty) => {
                debug!("Allocating more receive buffers.");
                // Allocate more recv buffers.
                Self::schedule(&mut task.recv_buffers_coroutine);
                task.next_pop_work_id
                    .try_recv()
                    .expect("Could not allocate more recv buffers")
            }
            Err(TryRecvError::Disconnected) => panic!("next_pop_work_id disconnected"),
        };

        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    fn schedule(task: &mut Pin<Box<dyn Future<Output = ()>>>) {
        // info!("{}", function_name!());

        let waker = crate::waker::emtpy_waker();
        if let Poll::Ready(_) = task.as_mut().poll(&mut Context::from_waker(&waker)) {
            panic!("Our coroutines should never finish!")
        }
    }

    pub fn service_completion_queue(&mut self, qt: QueueToken) {
        let task: &mut ConnectionTask = self.tasks.get_mut(qt.task_id.0).unwrap();
        Self::schedule(&mut task.completions_coroutine);
        Self::schedule(&mut task.recv_buffers_coroutine);
    }

    pub fn wait(&mut self, qt: QueueToken) -> Option<RegisteredMemoryRef> {
        let task: &mut ConnectionTask = self.tasks.get_mut(qt.task_id.0).unwrap();
        if task.completed_requests.borrow_mut().contains(&qt.work_id) {
            let entry = task
                .processed_requests
                .borrow_mut()
                .remove(&qt.work_id)
                .expect("Entry in completed queue but not in sent_push_requests...");
            Some(entry)
        } else {
            // Not yet ready.
            None
        }
    }
}

struct WorkRequest {
    memory: RegisteredMemoryRef,
    work_id: u64,
}

struct WorkRequestQueue {
    work_requests: Rc<RefCell<VecDeque<WorkRequest>>>,
}

impl Future for WorkRequestQueue {
    type Output = VecDeque<WorkRequest>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut push_work = self.work_requests.borrow_mut();
        if push_work.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(push_work.drain(..).collect())
        }
    }
}

struct SendWindows {
    control_flow: Rc<RefCell<ControlFlow>>,
}

/// Pending until more send windows are allocated by other side.
impl Future for SendWindows {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.control_flow.borrow_mut().remaining_send_windows() {
            0 => Poll::Pending,
            n => Poll::Ready(n),
        }
    }
}

// Note: Make sure you don't hold RefCells across yield points.
async fn push_coroutine(
    queue_pairs: Rc<RefCell<QueuePair>>,
    push_work: Rc<RefCell<VecDeque<WorkRequest>>>,
    control_flow: Rc<RefCell<ControlFlow>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemoryRef>>>,
) {
    let s = span!(Level::INFO, "push_coroutine");
    s.in_scope(|| trace!("started!"));

    let mut work_requests: VecDeque<WorkRequest> = VecDeque::with_capacity(1000);

    loop {
        let send_windows = SendWindows {
            control_flow: control_flow.clone(),
        }
        .await;
        s.in_scope(|| trace!("{} send windows currently available.", send_windows));

        work_requests.append(
            &mut WorkRequestQueue {
                work_requests: push_work.clone(),
            }
            .await,
        );

        let range = min(work_requests.len(), send_windows as usize);
        s.in_scope(|| trace!("Sending {} requests.", range));

        let requests_to_send: VecDeque<WorkRequest> = work_requests.drain(..range).collect();
        let mut mrs = requests_to_send.iter().map(|pw| (pw.work_id, &pw.memory));

        queue_pairs.borrow_mut().post_send(&mut mrs);

        let mut processed_push_requests = processed_requests.borrow_mut();
        for WorkRequest { memory, work_id } in requests_to_send {
            assert!(
                processed_push_requests.insert(work_id, memory).is_none(),
                "duplicate entry"
            );
        }
    }
}

struct RemainingReceiveWindows {
    control_flow: Rc<RefCell<ControlFlow>>,
}

/// Pending until more send windows are allocated by other side.
impl Future for RemainingReceiveWindows {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let cf = self.control_flow.deref().borrow();
        match cf.remaining_receive_windows() {
            0 => Poll::Ready(cf.batch_size),
            n => Poll::Pending,
        }
    }
}

async fn post_receive_coroutine(
    queue_pair: Rc<RefCell<QueuePair>>,
    control_flow: Rc<RefCell<ControlFlow>>,
    memory_pool: Rc<RefCell<VecDeque<RegisteredMemoryRef>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemoryRef>>>,
    work_id_counter: Rc<RefCell<u64>>,
    ready_pop_work_id: Sender<u64>,
) {
    let s = span!(Level::INFO, "post_receive_coroutine");
    s.in_scope(|| trace!("started!"));

    loop {
        let how_many = RemainingReceiveWindows {
            control_flow: control_flow.clone(),
        }
        .await;
        s.in_scope(|| trace!("Allocating {} new receive buffers.!", how_many));
        let receive_buffers: Vec<RegisteredMemoryRef> = (0..how_many)
            .map(|_| memory_pool.borrow_mut().pop_front().expect("Out of memory"))
            .collect();

        let work_id: u64 = work_id_counter.deref().borrow().clone();
        let to_post_recv = receive_buffers
            .iter()
            .enumerate()
            .map(|(i, memory)| (i as u64 + work_id, memory));

        queue_pair.borrow_mut().post_receive(to_post_recv);

        s.in_scope(|| {
            trace!(
                "Work ids {} through {} are ready.",
                work_id,
                work_id + how_many
            )
        });
        for n in work_id..work_id + how_many {
            ready_pop_work_id.send(n).unwrap();
        }

        let mut processed_requests = processed_requests.borrow_mut();
        for (i, memory) in receive_buffers.into_iter().enumerate() {
            processed_requests.insert(i as u64 + work_id, memory);
        }

        control_flow.borrow_mut().add_recv_windows(how_many);
        *work_id_counter.borrow_mut() += how_many;
    }
}

async fn completions_coroutine(
    control_flow: Rc<RefCell<ControlFlow>>,
    cq: Rc<RefCell<CompletionQueue>>,
    completed_requests: Rc<RefCell<HashSet<u64>>>,
) -> () {
    let s = span!(Level::INFO, "completions_coroutine");
    s.in_scope(|| trace!("started!"));

    loop {
        let completed = AsyncCompletionQueue { cq: cq.clone() }.await;
        s.in_scope(|| trace!("{} events completed!.", completed.len()));

        let mut recv_requests_completed = 0;
        let mut completed_requests = completed_requests.borrow_mut();
        for c in completed {
            info!("Work completion status: {}", c.status);
            if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_RECV {
                recv_requests_completed += 1;
            }
            completed_requests.insert(c.wr_id);
        }

        control_flow
            .borrow_mut()
            .subtract_recv_windows(recv_requests_completed);
    }
}

struct AsyncCompletionQueue {
    cq: Rc<RefCell<rdma_cm::CompletionQueue>>,
}

impl Future for AsyncCompletionQueue {
    type Output = Vec<rdma_cm::ffi::ibv_wc>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.cq.borrow_mut().poll() {
            None => Poll::Pending,
            Some(entries) => Poll::Ready(entries),
        }
    }
}
