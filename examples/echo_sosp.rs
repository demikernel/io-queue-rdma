use structopt::StructOpt;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::time::{Instant, Duration};

use io_queue_rdma;
use rdma_cm::RdmaMemory;
use io_queue_rdma::{IoQueue, QueueDescriptor, CompletedRequest, QueueToken};
use utilities::Statistics;
use nix::sys::socket::{SockAddr, InetAddr};
use std::convert::TryInto;


#[derive(StructOpt)]
#[structopt(about = "SOSP Echo Benchmark")]
enum Options {
    Server {
        #[structopt(long)]
        ip_address: String,
        #[structopt(long)]
        port: String,
    },
    Client {
        #[structopt(long)]
        ip_address: String,
        #[structopt(long)]
        port: String,
        #[structopt(long)]
        bufsize: usize,
        #[structopt(long)]
        nflows: usize,
    }
}

fn main() {
    let options = Options::from_args();

    match options {
        Options::Server { ip_address, port } => {
            let address = format!("{}:{}", ip_address, port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");

            let mut server = Server::new(address);
            server.run();
        }
        Options::Client {ip_address, port, bufsize, nflows} => {
            let mut c = Client::new(bufsize, &ip_address, &port, nflows);
            c.client()
        }
    }
}

struct Server {
    stats: Statistics,
    listening_qd: QueueDescriptor,
    libos: IoQueue,
}

impl Server {
    pub fn new(socket_address: SocketAddr) -> Self {
        let address = SockAddr::new_inet(InetAddr::from_std(&socket_address));

        let mut libos = IoQueue::new();
        // Setup connection.
        let mut listening_qd: QueueDescriptor = libos.socket();
        libos.bind(&mut listening_qd, &address).unwrap();
        libos.listen(&mut listening_qd);

        Self {
            stats: Statistics::new("server"),
            listening_qd,
            libos,
        }
    }

    pub fn run(&mut self) {
        let mut qtokens: Vec<QueueToken> = Vec::new();
        let mut connected_qd = self.libos.accept(&mut self.listening_qd);

        let mut bufsize: usize = 0;
        let mut start: Instant = Instant::now();
        let mut last_log: Instant = Instant::now();

        // Wait for client to write to us.
        let qt = self.libos.pop(&mut connected_qd);
        qtokens.push(qt);

        loop {
            // Dump statistics.
            if last_log.elapsed() > Duration::from_secs(5) {
                self.stats.print();
                last_log = Instant::now();
            }

            let (i, result) = self.libos.wait_any(&qtokens);
            qtokens.swap_remove(i);

            match result {
                CompletedRequest::Pop(memory) => {
                    bufsize = memory.accessed();
                    let qt = self.libos.push(&mut connected_qd, memory);
                    qtokens.push(qt);
                }
                CompletedRequest::Push(memory) => {
                    self.libos.free(&mut connected_qd, memory);

                    self.stats.record(2 * bufsize, start.elapsed());
                    start = Instant::now();
                    let qt = self.libos.pop(&mut connected_qd);
                    qtokens.push(qt);
                }
            }
        }
    }
}

struct Client {
    stats: Statistics,
    qd: QueueDescriptor,
    nflows: usize,
    bufsize: usize,
    libos: IoQueue,
    nextpkt: u64,
}

impl Client {
    pub fn new(bufsize: usize, address: &str, port: &str, nflows: usize) -> Self {
        println!("buffer size {:?}, number of flows {:?}", bufsize, nflows);

        // Setup connection.
        let mut libos = IoQueue::new();
        let mut connection: QueueDescriptor = libos.socket();


        libos.connect(&mut connection, &address, &port);

        Self {
            stats: Statistics::new("client"),
            qd: connection,
            nflows,
            bufsize,
            libos,
            nextpkt: 0,
        }
    }

    fn client(&mut self) {
        let mut qtokens: Vec<QueueToken> = Vec::new();
        let mut packet_times: HashMap<u64, Instant> = HashMap::new();
        let mut last_log = Instant::now();

        // Send initial packets.
        for _ in 0..self.nflows {
            let (buf, stamp) = self.makepkt(self.bufsize);
            packet_times.insert(stamp, Instant::now());

            let qt = self.libos.push(&mut self.qd, buf);
            qtokens.push(qt);
        }

        loop {
            // Dump statistics.
            if last_log.elapsed() > Duration::from_secs(5) {
                self.stats.print();
                last_log = Instant::now();
            }

            let (i, result) = self.libos.wait_any(&qtokens);
            qtokens.swap_remove(i);

            match result {
                CompletedRequest::Push(memory) => {
                    let qt = self.libos.pop(&mut self.qd);
                    qtokens.push(qt);
                    self.libos.free(&mut self.qd, memory);
                }
                CompletedRequest::Pop(memory) => {
                    // Record statistics.
                    let bufsize: usize = memory.accessed();
                    let stamp: u64 = Self::getstamp(&memory);
                    self.libos.free(&mut self.qd, memory);

                    let timestamp = packet_times.remove(&stamp).unwrap();
                    self.stats.record(2 * bufsize, timestamp.elapsed());

                    // Send another packet.
                    let (buf, stamp) = self.makepkt(self.bufsize);
                    packet_times.insert(stamp, Instant::now());
                    let qt = self.libos.push(&mut self.qd, buf);
                    qtokens.push(qt);
                }
            }
        }
    }

    fn makepkt(&mut self, pktsize: usize) -> (RdmaMemory<u8, 1000>, u64) {
        let mut rdma_memory = self.libos.malloc(&mut self.qd);

        assert!(
            pktsize <= 1000,
            "pktsize {}, pktbuf size {}",
            pktsize,
            1000,
        );

        let stamp = self.nextpkt;
        self.nextpkt += 1;

        // Factory packet.
        let stamp_slice = stamp.to_ne_bytes();

        let memory_slice = rdma_memory.as_mut_slice(stamp_slice.len());
        for i in 0..stamp_slice.len() {
            memory_slice[i] = stamp_slice[i];
        }


        (rdma_memory, stamp)
    }

    fn getstamp(memory: &RdmaMemory<u8, 1000>) -> u64 {
        u64::from_ne_bytes(memory.as_slice()[0..8].try_into().unwrap())
    }
}