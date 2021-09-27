// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

use histogram::Histogram;
use std::time::Duration;

pub struct Statistics {
    label: String,
    times: Histogram,
    bytes: Histogram,
}

impl Statistics {
    pub fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
            times: Histogram::configure().precision(3).build().unwrap(),
            bytes: Histogram::configure().precision(4).build().unwrap(),
        }
    }

    fn latency(&self) -> Duration {
        let median = self.times.percentile(0.50).unwrap();
        Duration::from_nanos(median)
    }

    fn throughput(&self) -> f64 {
        let duration = self.latency();
        let nbytes = self.bytes.percentile(0.50).unwrap();
        // dbg!(nbytes);
        // dbg!(duration.as_secs_f64());

        let bps = (nbytes as f64) / duration.as_secs_f64();
        bps / 1024. / 1024. / 1024. * 8.
    }

    pub fn print_latency(&self) {
        if self.times.entries() > 0 {
            println!("{}: {:?}", self.label, self.latency(),);
        }
    }

    pub fn print_throughput(&self) {
        if self.times.entries() > 0 {
            println!("{}: {} Gbps", self.label, self.throughput(),);
        }
    }

    pub fn print(&self) {
        if self.times.entries() > 0 {
            println!(
                "{}: {:?}, {} Gbps",
                self.label,
                self.latency(),
                self.throughput(),
            );
        }
    }

    pub fn record(&mut self, nbytes: usize, sample: Duration) {
        self.bytes.increment(nbytes as u64).unwrap();
        self.times.increment(sample.as_nanos() as u64).unwrap();
    }
}
