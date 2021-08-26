use io_queue_rdma::IoQueue;
use nix::sys::socket::{InetAddr, SockAddr};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;
use structopt::StructOpt;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
enum Mode {
    Client,
    Server,
}

impl FromStr for Mode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Client" | "client" => Ok(Mode::Client),
            "Server" | "server" => Ok(Mode::Server),
            _ => Err("Unknown mode. Available modes: 'client', 'server'."),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Echo RDMA server/client program.",
    about = "Computes roundtrip latency for RDMA IoQueue Echo Server."
)]
struct Opt {
    #[structopt(long)]
    mode: Mode,
    #[structopt(long)]
    ip_address: String,
    #[structopt(long)]
    port: String,
    #[structopt(long)]
    /// How many iterations to run the experiment for.
    loops: usize,
    #[structopt(long)]
    memory_size: usize,
}

fn main() {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    let opt = Opt::from_args();
    let address = format!("{}:{}", opt.ip_address, opt.port);
    let address: SocketAddr = address.parse().expect("Unable to parse socket address");
    match opt.mode {
        Mode::Server => {
            let mut io_queue = IoQueue::new();
            let mut listening_qd = io_queue.socket();

            io_queue
                .bind(
                    &mut listening_qd,
                    &SockAddr::new_inet(InetAddr::from_std(&address)),
                )
                .unwrap();
            io_queue.listen(&mut listening_qd);
            let mut connected_qd = io_queue.accept(&mut listening_qd);

            println!("Server connected!");

            for _ in 0..opt.loops {
                let qt = io_queue.pop(&mut connected_qd);
                let memory = io_queue.wait(qt);

                let qt = io_queue.push(&mut connected_qd, memory);
                let memory = io_queue.wait(qt);

                io_queue.free(&mut connected_qd, memory);
            }

            io_queue.disconnect(connected_qd);
        }
        Mode::Client => {
            let mut io_queue = IoQueue::new();
            let mut connection = io_queue.socket();
            io_queue.connect(&mut connection, &opt.ip_address, &opt.port);

            let mut running: u128 = 0;
            let mut push: u128 = 0;
            let mut pop: u128 = 0;
            let mut push_wait: u128 = 0;
            let mut pop_wait: u128 = 0;

            for loop_val in 0..opt.loops {
                let mut memory = io_queue.malloc(&mut connection);
                let slice = memory.as_mut_slice(opt.memory_size);

                for i in 0..opt.memory_size {
                    slice[i] = (loop_val % 255) as u8;
                }

                let roundtrip_time = Instant::now();

                let push_time = Instant::now();
                let qt = io_queue.push(&mut connection, memory);
                push += push_time.elapsed().as_micros();

                let push_wait_time = Instant::now();
                let memory = io_queue.wait(qt);
                push_wait += push_wait_time.elapsed().as_micros();
                io_queue.free(&mut connection, memory);

                let pop_time = Instant::now();
                let qt = io_queue.pop(&mut connection);
                pop += pop_time.elapsed().as_micros();

                let pop_wait_time = Instant::now();
                let mut memory = io_queue.wait(qt);

                pop_wait += pop_wait_time.elapsed().as_micros();
                running += roundtrip_time.elapsed().as_micros();

                let slice = memory.as_mut_slice(opt.memory_size);
                for i in 0..opt.memory_size {
                    assert_eq!(slice[i], (loop_val % 255) as u8);
                }

                io_queue.free(&mut connection, memory);
            }
            println!("Latencies averaged over {} runs.", opt.loops);
            println!(
                "Total roundtrip latency: {}us",
                (running as f64) / (opt.loops as f64)
            );
            println!("Push latency: {}us", (push as f64) / (opt.loops as f64));
            println!(
                "Push-wait latency: {}us",
                (push_wait as f64) / (opt.loops as f64)
            );
            println!("Pop latency: {}us", (pop as f64) / (opt.loops as f64));
            println!(
                "Pop-wait latency: {}us",
                (pop_wait as f64) / (opt.loops as f64)
            );
            io_queue.disconnect(connection);
        }
    }
}
