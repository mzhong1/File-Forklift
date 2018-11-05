use error::ForkliftResult;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Pulse {
    heartbeat_at: u64,
    pub interval: u64,
}

impl Pulse {
    pub fn new(i: u64) -> ForkliftResult<Self> {
        let c_time = current_time_in_millis(SystemTime::now())?;
        Ok(Pulse {
            heartbeat_at: c_time + i,
            interval: i,
        })
    }

    pub fn beat(&mut self) -> ForkliftResult<bool> {
        let c_time = current_time_in_millis(SystemTime::now())?;
        trace!("current time in millis {}", c_time);
        trace!("heartbeat_at {}", self.heartbeat_at);
        if c_time > self.heartbeat_at {
            debug!("Time to heartbeat! heartbeat_at is {}", self.heartbeat_at);
            self.heartbeat_at = c_time + self.interval;
            debug!(
                "current time in millis {}, heartbeat_at is now {}",
                c_time, self.heartbeat_at
            );
            return Ok(true);
        }
        Ok(false)
    }
}

#[test]
fn test_current_time_in_millis() {
    let start = current_time_in_millis(SystemTime::now()).unwrap();
    ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    let end = current_time_in_millis(SystemTime::now()).unwrap();
    println!("Time difference {}", end - start);
    assert!(end - start < 1002 && end - start >= 1000);
}

/*
    current_time_in_millis: SystemTime -> u64
    REQUIRES: start is the current System Time
    ENSURES: returns the time since the UNIX_EPOCH in milliseconds
*/
fn current_time_in_millis(start: SystemTime) -> ForkliftResult<u64> {
    let since_epoch = start.duration_since(UNIX_EPOCH)?;
    trace!("Time since epoch {:?}", since_epoch);
    Ok(since_epoch.as_secs() * 1000 + u64::from(since_epoch.subsec_nanos()) / 1_000_000)
}
