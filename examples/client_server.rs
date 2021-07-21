use io_queue_rdma::IoQueue;
use nix::sys::socket::{InetAddr, IpAddr, SockAddr};
use std::str::FromStr;
use structopt::StructOpt;

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
}

fn main() {
    let opt = Opt::from_args();
    match opt.mode {
        Mode::Server => {
            let mut io_queue = IoQueue::new();
            let mut listening_qd = io_queue.socket();

            let addr = SockAddr::Inet(InetAddr::new(IpAddr::new_v4(127, 0, 0, 1), 4000));
            io_queue.bind(&mut listening_qd, &addr);
            io_queue.listen(&mut listening_qd);
            let mut connected_qd = io_queue.accept(&mut listening_qd);
            println!("Connected to client!");

            println!("Waiting to receive byte...");
            let qt = io_queue.pop(&mut connected_qd);
            let buffer = io_queue.wait(qt);
            println!("Server got: {:?}", buffer[0]);
        }
        Mode::Client => {
            let mut io_queue = IoQueue::new();
            let mut connection = io_queue.socket();
            io_queue.connect(&mut connection);

            println!("Sending byte to server.");
            let mut mem = io_queue.malloc(&mut connection, 1);
            mem[0] = 42;
            let qt = io_queue.push(&mut connection, mem);
            // Acquire our allocated memory again.
            let mem = io_queue.wait(qt);
            println!("Byte sent!");
        }
    }
}
