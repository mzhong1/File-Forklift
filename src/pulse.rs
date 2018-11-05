use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Pulse {
    pub interval: u64,
    c_time: Instant,
    timeout: Duration,
}

impl Pulse {
    pub fn new(i: u64) -> Self {
        Pulse {
            interval: i,
            c_time: Instant::now(),
            timeout: Duration::from_millis(i),
        }
    }

    pub fn beat(&mut self) -> bool {
        let elapsed = self.c_time.elapsed();
        trace!(
            "Time elapsed in millis {:?}",
            (elapsed.as_secs() * 1_000) + (elapsed.subsec_nanos() / 1_000_000) as u64
        );
        if elapsed > self.timeout {
            debug!("The previous instant was {:?}", self.c_time);
            self.c_time = Instant::now();
            debug!("current instant is now {:?}", self.c_time);
            return true;
        }
        false
    }
}
