## IO queue RDMA

This crate implements the [Demikernel](https://www.microsoft.com/en-us/research/project/demikernel/) RDMA IO queue interface.
Check main [Demikernel Repository page](https://github.com/demikernel/demikernel) for more info about the project.

This crate provides high-level bindings around RDMA ibverbs without sacrificing performance.

Sending a byte is as simple as:
```rust
// Create IQueue with specified sizes for queues. 
let mut io_queue = IoQueue::<2048, 256, 32, 1024, 1024>::new();
// Create connection socket.
let mut connection = io_queue.socket();
// Connect to server via specified ip address and port.
io_queue.connect(&mut connection, &ip_address, &port);

println!("Sending byte to server.");
// Get an RDMA registered memory region.
let mut memory = io_queue.malloc(&mut connection);
// Write single byte to local memory.
memory.as_mut_slice(1)[0] = 42;
// Send byte as post_send message.
let qt = io_queue.push(&mut connection, memory);
// Wait for ack of post_send request.
let memory = io_queue.wait(qt).push_op();
// Allow IO Queue to reclaim memory for reuse.
io_queue.free(&mut connection, memory);
println!("Byte sent!");
```

Check the `examples/` directories for various RDMA IoQueue programs.