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
    name = "IoQueue Client/Server",
    about = "Example IOQueue RDMA Client/Server Program."
)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short, long)]
    loopback_address: String,
    #[structopt(short, long)]
    port: u16,
    #[structopt(short, long)]
    /// How many iterations to run the experiment for.
    loops: usize,
}

fn main() {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    let opt = Opt::from_args();
    match opt.mode {
        Mode::Server => {
            let mut io_queue = IoQueue::new();
            let mut listening_qd = io_queue.socket();

            let address = format!("{}:{}", opt.loopback_address, opt.port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");
            io_queue.bind(
                &mut listening_qd,
                &SockAddr::new_inet(InetAddr::from_std(&address)),
            );
            io_queue.listen(&mut listening_qd);
            let mut connected_qd = io_queue.accept(&mut listening_qd);

            println!("Server connected!");

            for _ in 0..opt.loops {
                let qt = io_queue.pop(&mut connected_qd);
                let memory = io_queue.wait(qt);

                let qt = io_queue.push(&mut connected_qd, memory);
                let memory = io_queue.wait(qt);

                // println!("Server message echoed!");
                io_queue.free(&mut connected_qd, memory);
            }
        }
        Mode::Client => {
            let mut io_queue = IoQueue::new();
            let mut connection = io_queue.socket();

            let address = format!("{}:{}", opt.loopback_address, opt.port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");
            io_queue.connect(&mut connection, InetAddr::from_std(&address));

            let mut running: u128 = 0;
            let mut push: u128 = 0;
            let mut pop: u128 = 0;
            let mut push_wait: u128 = 0;
            let mut pop_wait: u128 = 0;

            for _ in 0..opt.loops {
                let mut memory = io_queue.malloc(&mut connection);
                memory[0] = 42;

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
                let memory = io_queue.wait(qt);
                pop_wait += pop_wait_time.elapsed().as_micros();

                running += roundtrip_time.elapsed().as_micros();

                io_queue.free(&mut connection, memory);

                println!(
                    "Roundtrip latency over {} runs = {}us",
                    opt.loops,
                    (running as f64) / (opt.loops as f64)
                );
                println!(
                    "Push latency over {} runs = {}us",
                    opt.loops,
                    (push as f64) / (opt.loops as f64)
                );
                println!(
                    "Push-wait latency over {} runs = {}us",
                    opt.loops,
                    (push_wait as f64) / (opt.loops as f64)
                );
                println!(
                    "Pop latency over {} runs = {}us",
                    opt.loops,
                    (pop as f64) / (opt.loops as f64)
                );
                println!(
                    "Pop-wait latency over {} runs = {}us",
                    opt.loops,
                    (pop_wait as f64) / (opt.loops as f64)
                );
            }
        }
    }
}
