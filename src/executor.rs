use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tracing::{debug, info, span, trace, Level};

use crate::function_name;

use rdma_cm::{
    CommunicatioManager, CompletionQueue, ProtectionDomain, QueuePair, RegisteredMemoryRef,
};

use crate::control_flow::ControlFlow;
use std::cmp::min;
use std::ptr::slice_from_raw_parts_mut;

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
    memory_pool: Vec<RegisteredMemoryRef>,
    protection_domain: ProtectionDomain,

    push_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    pop_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    completions_coroutine: Pin<Box<dyn Future<Output = ()>>>,

    // pop_task: Pin<dyn Future<Output = ()>>,
    // alloc_memory_task: Box<dyn Future<Output = ()>>,
    // receive_buffer_task: Box<dyn Future<Output = ()>>,
    push_work_queue: Rc<RefCell<VecDeque<WorkRequest>>>,
    pop_work_queue: Rc<RefCell<VecDeque<WorkRequest>>>,

    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemoryRef>>>,
    completed_requests: Rc<RefCell<HashSet<u64>>>,

    work_id_counter: u64,
}

impl<const N: usize, const SIZE: usize> Executor<N, SIZE> {
    pub fn new() -> Executor<N, SIZE> {
        info!("{}", function_name!());
        Executor {
            tasks: Vec::with_capacity(100),
        }
    }

    /// TODO: Change usize to proper type.
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

        let processed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));
        let completed_push_requests = Rc::new(RefCell::new(HashSet::with_capacity(1000)));

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

        let mut memory_pool: Vec<RegisteredMemoryRef> = Vec::with_capacity(N);
        for n in 0..N {
            let mut memory = vec![0 as u8; N * SIZE].into_boxed_slice();
            let registered_memory =
                CommunicatioManager::register_memory_buffer(&mut protection_domain, &mut memory);
            memory_pool.push(RegisteredMemoryRef::new(
                registered_memory.get_lkey(),
                memory,
            ));
        }

        let ct = ConnectionTask {
            memory_pool,
            protection_domain,
            push_coroutine: Box::pin(push_coroutine(
                queue_pair.clone(),
                push_work_queue.clone(),
                control_flow.clone(),
                processed_requests.clone(),
            )),
            pop_coroutine: Box::pin(pop_coroutine(
                queue_pair.clone(),
                pop_work_queue.clone(),
                control_flow.clone(),
                processed_requests.clone(),
            )),
            completions_coroutine: Box::pin(completions_coroutine(
                completion_queue,
                completed_push_requests.clone(),
            )),
            push_work_queue,
            pop_work_queue,
            processed_requests,
            completed_requests: completed_push_requests,
            work_id_counter: 0,
        };

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
            .pop()
            .expect("Out of memory!")
    }

    pub fn push(&mut self, task_handle: TaskHandle, mem: RegisteredMemoryRef) -> QueueToken {
        info!("{}", function_name!());

        let task: &mut ConnectionTask = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id = task.work_id_counter;
        let work = WorkRequest {
            memory: mem,
            work_id,
        };
        task.work_id_counter += 1;

        task.push_work_queue.borrow_mut().push_back(work);
        Executor::<N, SIZE>::schedule(&mut task.push_coroutine);
        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    pub fn pop(&mut self, task_handle: TaskHandle) -> QueueToken {
        info!("{}", function_name!());

        let memory = self.malloc(task_handle);
        let task: &mut ConnectionTask = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id = task.work_id_counter;
        let work = WorkRequest { memory, work_id };

        task.work_id_counter += 1;
        task.pop_work_queue.borrow_mut().push_back(work);
        Executor::<N, SIZE>::schedule(&mut task.pop_coroutine);
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
        Executor::<N, SIZE>::schedule(&mut task.completions_coroutine);
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
        match self.control_flow.borrow_mut().get_avaliable_entries() {
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

        let mut requests_to_send: VecDeque<WorkRequest> = work_requests.drain(..range).collect();
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

async fn pop_coroutine(
    queue_pairs: Rc<RefCell<QueuePair>>,
    pop_work: Rc<RefCell<VecDeque<WorkRequest>>>,
    control_flow: Rc<RefCell<ControlFlow>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemoryRef>>>,
) {
    let s = span!(Level::INFO, "pop_coroutine");
    s.in_scope(|| trace!("started!"));

    // TODO Allocate more receive windows if needed?
    loop {
        let new_work = WorkRequestQueue {
            work_requests: pop_work.clone(),
        }
        .await;
        s.in_scope(|| trace!("{} new work to pop.", new_work.len()));
        let mut receive_requests = new_work.iter().map(|pw| (pw.work_id, &pw.memory));

        queue_pairs.borrow_mut().post_receive(&mut receive_requests);

        let mut processed_pop_requests = processed_requests.borrow_mut();
        for WorkRequest { memory, work_id } in new_work {
            assert!(
                processed_pop_requests.insert(work_id, memory).is_none(),
                "duplicate entry"
            );
        }
    }
}

async fn completions_coroutine(
    cq: Rc<RefCell<CompletionQueue>>,
    completed_push_requests: Rc<RefCell<HashSet<u64>>>,
) -> () {
    let s = span!(Level::INFO, "completions_coroutine");
    s.in_scope(|| trace!("started!"));

    loop {
        let completed = AsyncCompletionQueue { cq: cq.clone() }.await;
        s.in_scope(|| trace!("{} events completed!.", completed.len()));

        let mut completed_requests = completed_push_requests.borrow_mut();
        for c in completed {
            completed_requests.insert(c.wr_id);
        }
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
