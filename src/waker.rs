use std::task::{Context, RawWaker, RawWakerVTable, Waker};

fn do_nothing(_ptr: *const ()) {}

fn clone(ptr: *const ()) -> RawWaker {
    RawWaker::new(ptr, &VTABLE)
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, do_nothing, do_nothing, do_nothing);

pub fn emtpy_waker() -> Waker {
    let raw = RawWaker::new(std::ptr::null(), &VTABLE);
    unsafe { Waker::from_raw(raw) }
}
