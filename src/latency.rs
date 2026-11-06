//! `LATENCY`: spikes by the kind of command they came from, as Redis' monitor keeps
//! them. It's off until `latency-monitor-threshold` is set, and while it's on, every
//! command takes the slow path with a real clock (see `Context::special`), as Redis'
//! does. Only the two events commands make exist here: Tantalum has no forks to time.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

/// Samples kept per event, as in Redis.
const KEPT: usize = 160;
const GRAPH_COLUMNS: usize = 80;
const GRAPH_ROWS: i64 = 4;

#[derive(Default)]
struct Series {
    /// Seconds and milliseconds, oldest first: at most one a second, the worst of it.
    samples: VecDeque<(u64, u64)>,
    /// The worst ever, which outlives its sample.
    max: u64,
}

#[derive(Default)]
pub(crate) struct Latency {
    /// `latency-monitor-threshold`, in milliseconds: 0 for off.
    threshold: AtomicU64,
    events: Mutex<Vec<(&'static str, Series)>>,
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Latency {
    pub(crate) fn threshold(&self) -> u64 {
        self.threshold.load(Relaxed)
    }

    pub(crate) fn set_threshold(&self, millis: u64) {
        self.threshold.store(millis, Relaxed);
    }

    /// Notes what a command took, if it's a spike.
    pub(crate) fn note(&self, event: &'static str, millis: u64) {
        let threshold = self.threshold();
        if threshold == 0 || millis < threshold {
            return;
        }
        let now = unix_seconds();
        let mut events = self.events.lock();
        let at = match events.iter().position(|(name, _)| *name == event) {
            Some(at) => at,
            None => {
                events.push((event, Series::default()));
                events.len() - 1
            }
        };
        let series = &mut events[at].1;
        series.max = series.max.max(millis);
        match series.samples.back_mut() {
            Some((time, worst)) if *time == now => *worst = (*worst).max(millis),
            _ => {
                if series.samples.len() == KEPT {
                    series.samples.pop_front();
                }
                series.samples.push_back((now, millis));
            }
        }
    }

    /// `LATENCY LATEST`: each event's name, latest sample's time and value, and worst ever.
    pub(crate) fn latest(&self) -> Vec<(&'static str, u64, u64, u64)> {
        self.events
            .lock()
            .iter()
            .filter_map(|(name, series)| {
                let (time, millis) = *series.samples.back()?;
                Some((*name, time, millis, series.max))
            })
            .collect()
    }

    /// `LATENCY HISTORY`: an event's samples, oldest first.
    pub(crate) fn history(&self, event: &[u8]) -> Vec<(u64, u64)> {
        self.events
            .lock()
            .iter()
            .find(|(name, _)| name.as_bytes() == event)
            .map_or_else(Vec::new, |(_, series)| {
                series.samples.iter().copied().collect()
            })
    }

    /// `LATENCY RESET`: forgets those named, or with none named, all. How many went.
    pub(crate) fn reset(&self, named: &[&[u8]]) -> usize {
        let mut events = self.events.lock();
        let before = events.len();
        if named.is_empty() {
            events.clear();
        } else {
            events.retain(|(name, _)| !named.contains(&name.as_bytes()));
        }
        before - events.len()
    }

    /// `LATENCY GRAPH`: Redis' drawing of an event's samples, if it has any.
    pub(crate) fn graph(&self, event: &[u8]) -> Option<String> {
        let events = self.events.lock();
        let (name, series) = events.iter().find(|(name, _)| name.as_bytes() == event)?;
        let now = unix_seconds();
        let samples: Vec<(u64, String)> = series
            .samples
            .iter()
            .map(|&(time, millis)| (millis, age(now.saturating_sub(time))))
            .collect();
        let min = samples.iter().map(|s| s.0).min()?;
        let max = samples.iter().map(|s| s.0).max()?;
        let mut text = format!(
            "{name} - high {max} ms, low {min} ms (all time high {} ms)\n",
            series.max
        );
        text.push_str(&"-".repeat(GRAPH_COLUMNS));
        text.push('\n');
        for (i, part) in samples.chunks(GRAPH_COLUMNS).enumerate() {
            if i != 0 {
                text.push('\n');
            }
            draw(&mut text, part, min, max);
        }
        Some(text)
    }

    /// `LATENCY DOCTOR`: Redis' report, on the events there can be here.
    pub(crate) fn doctor(&self, slowlog_threshold: i64) -> String {
        let events = self.events.lock();
        let threshold = self.threshold();
        if events.is_empty() && threshold == 0 {
            return "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this Redis instance. You may use \"CONFIG SET latency-monitor-threshold <milliseconds>.\" in order to enable it. If we weren't in a deep space mission I'd suggest to take a look at https://redis.io/topics/latency-monitor.\n".to_string();
        }
        if events.is_empty() {
            return "Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sit down calmly, take a stress pill, and think things over.\n".to_string();
        }
        let mut report = "Dave, I have observed latency spikes in this Redis instance. You don't mind talking about it, do you Dave?\n\n".to_string();
        let now = unix_seconds();
        let (mut commands, mut fast_commands) = (false, false);
        for (i, (name, series)) in events.iter().enumerate() {
            let count = series.samples.len() as u64;
            let sum: u64 = series.samples.iter().map(|s| s.1).sum();
            let average = sum.checked_div(count).unwrap_or(0);
            let deviation: u64 = series
                .samples
                .iter()
                .map(|s| s.1.abs_diff(average))
                .sum::<u64>()
                .checked_div(count)
                .unwrap_or(0);
            let oldest = series.samples.iter().map(|s| s.0).min().unwrap_or(now);
            let period = now.saturating_sub(oldest).max(1);
            report.push_str(&format!(
                "{}. {name}: {count} latency spikes (average {average}ms, mean deviation {deviation}ms, period {:.2} sec). Worst all time event {}ms.\n",
                i + 1,
                period as f64 / count as f64,
                series.max,
            ));
            commands |= *name == "command";
            fast_commands |= *name == "fast-command";
        }
        report.push_str("\nI have a few advices for you:\n\n");
        if commands {
            if slowlog_threshold < 0 {
                report.push_str(&format!("- There are latency issues with potentially slow commands you are using. Try to enable the Slow Log Redis feature using the command 'CONFIG SET slowlog-log-slower-than {}'. If the Slow log is disabled Redis is not able to log slow commands execution for you.\n", threshold * 1000));
            } else if slowlog_threshold as u64 / 1000 > threshold {
                report.push_str(&format!("- Your current Slow Log configuration only logs events that are slower than your configured latency monitor threshold. Please use 'CONFIG SET slowlog-log-slower-than {}'.\n", threshold * 1000));
            }
            report.push_str("- Check your Slow Log to understand what are the commands you are running which are too slow to execute. Please check https://redis.io/commands/slowlog for more information.\n");
        }
        if fast_commands {
            report.push_str("- The system is slow to execute Redis code paths not containing system calls. This usually means the system does not provide Redis CPU time to run for long periods. You should try to:\n  1) Lower the system load.\n  2) Use a computer / VM just for Redis if you are running other software in the same system.\n  3) Check if you have a \"noisy neighbour\" problem.\n  4) Check with 'redis-cli --intrinsic-latency 100' what is the intrinsic latency in your system.\n  5) Check if the problem is allocator-related by recompiling Redis with MALLOC=libc, if you are using Jemalloc. However this may create fragmentation problems.\n");
        }
        if commands {
            report.push_str("- Deleting, expiring or evicting (because of maxmemory policy) large objects is a blocking operation. If you have very large objects that are often deleted, expired, or evicted, try to fragment those objects into multiple smaller objects.\n");
        }
        report
    }
}

/// How long ago, as the graph labels it.
fn age(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

/// One line of the graph's columns: bars, a blank row, then each column's label downwards.
fn draw(text: &mut String, samples: &[(u64, String)], min: u64, max: u64) {
    const MARKS: &[u8] = b"_o#";
    let marks = MARKS.len() as i64;
    let steps = marks * GRAPH_ROWS;
    let range = if max == min { 1.0 } else { (max - min) as f64 };
    let mut row = 0;
    loop {
        let mut more = false;
        let mut line = vec![b' '; samples.len()];
        for (at, (value, label)) in samples.iter().enumerate() {
            if row < GRAPH_ROWS {
                let scaled = ((value - min) as f64 * steps as f64) as i64;
                let step = ((scaled as f64 / range) as i64).clamp(0, steps - 1);
                let mark = step - (GRAPH_ROWS - row - 1) * marks;
                more = true;
                if (0..marks).contains(&mark) {
                    line[at] = MARKS[mark as usize];
                } else if mark >= marks {
                    line[at] = b'|';
                }
            } else if row == GRAPH_ROWS {
                more = true;
                break;
            } else if let Some(&letter) = label.as_bytes().get((row - GRAPH_ROWS - 1) as usize) {
                more = true;
                line[at] = letter;
            }
        }
        if !more {
            return;
        }
        row += 1;
        text.push_str(&String::from_utf8_lossy(&line));
        text.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_graph_is_drawn_as_redis_draws_it() {
        // These samples and this drawing are from a Redis 7.2.4.
        let samples: Vec<(u64, String)> = [200, 100, 300, 70, 120]
            .iter()
            .zip(["4s", "3s", "2s", "1s", "0s"])
            .map(|(&ms, label)| (ms, label.to_string()))
            .collect();
        let mut text = String::new();
        draw(&mut text, &samples, 70, 300);
        assert_eq!(text, "  #  \n_ |  \n| |  \n|o|_#\n     \n43210\nsssss\n");

        let mut text = String::new();
        draw(
            &mut text,
            &[(200, "1s".to_string()), (100, "0s".to_string())],
            100,
            200,
        );
        assert_eq!(text, "# \n| \n| \n|_\n  \n10\nss\n");
    }

    #[test]
    fn spikes_are_kept_as_redis_keeps_them() {
        let latency = Latency::default();
        latency.note("command", 500);
        assert!(latency.latest().is_empty(), "off by default");
        latency.set_threshold(100);
        latency.note("command", 99);
        assert!(latency.latest().is_empty(), "below the threshold");
        latency.note("command", 150);
        latency.note("command", 120);
        let (name, _, _, max) = latency.latest()[0];
        assert_eq!((name, max), ("command", 150));
        assert_eq!(latency.reset(&[b"nope"]), 0);
        assert_eq!(latency.reset(&[b"command", b"nope"]), 1);
        assert_eq!(latency.reset(&[]), 0);
        assert!(latency.graph(b"command").is_none());
    }
}
