use log::*;
use std::time::{Duration, Instant};

#[derive(Debug, Copy, Clone)]
///  the pulse of the heartbeat protocol, determining when to beat
pub struct Pulse {
    /// amount of time b/t each heartbeat in millis
    pub interval: u64,
    /// The time of the last heartbeat
    c_time: Instant,
    /// The amount of time b/t each heartbeat as a Duration
    timeout: Duration,
}

impl Pulse {
    /// create a new Pulse
    pub fn new(interval: u64) -> Self {
        Pulse { interval, c_time: Instant::now(), timeout: Duration::from_millis(interval) }
    }

    /// tell if it is time to heartbeat (if the time elapsed is more than timeout duration)
    pub fn beat(&mut self) -> bool {
        let elapsed = self.c_time.elapsed();
        trace!(
            "Time elapsed in millis {:?}",
            (elapsed.as_secs() * 1_000) + u64::from(elapsed.subsec_millis())
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
