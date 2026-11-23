//! Sentinel: watches masters and their replicas, and when a master is down
//! by enough Sentinels' account, elects one of them to promote a replica in
//! its place, as Redis' Sentinel does, over the same protocol, so that
//! Tantalum's and Redis' Sentinels can watch the same servers (Redis or
//! Tantalum) together, and clients find the master the same way
//! (`SENTINEL get-master-addr-by-name`, the `+switch-master` event).
//!
//! `tantalum --sentinel FILE` reads Redis' `sentinel.conf` directives from
//! FILE and rewrites it as what it learns changes (the replicas and
//! Sentinels it found, the epochs), as Redis does. It's a server of its own:
//! the data commands aren't there, only Sentinel's (`SENTINEL`, `INFO`,
//! `ROLE`, `PING`, pub/sub for the events, `AUTH`, `HELLO`, `CLIENT`).
//!
//! The main loop (`monitor.rs`) owns all the state: links to instances
//! (`link.rs`) and clients (`server.rs`) talk to it by message.

pub(crate) mod link;
pub(crate) mod monitor;
pub(crate) mod server;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch (Redis' `mstime`).
pub(crate) fn mstime() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// A random 40-hex-digit ID.
pub(crate) fn new_id() -> String {
    use std::hash::{BuildHasher, RandomState};
    let random = RandomState::new();
    let mut id = String::new();
    for i in 0..3u8 {
        let _ = write!(id, "{:016x}", random.hash_one((i, mstime())));
    }
    id.truncate(40);
    id
}

pub(crate) fn random_below(n: u64) -> u64 {
    use std::hash::{BuildHasher, RandomState};
    if n == 0 {
        return 0;
    }
    RandomState::new().hash_one(mstime()) % n
}

/// Timings Redis' Sentinel lets `SENTINEL DEBUG` change (milliseconds).
#[derive(Clone, Debug)]
pub(crate) struct Timings {
    pub(crate) info_period: i64,
    pub(crate) ping_period: i64,
    pub(crate) ask_period: i64,
    pub(crate) publish_period: i64,
    pub(crate) default_down_after: i64,
    pub(crate) tilt_trigger: i64,
    pub(crate) tilt_period: i64,
    pub(crate) slave_reconf_timeout: i64,
    pub(crate) min_link_reconnect_period: i64,
    pub(crate) default_failover_timeout: i64,
    pub(crate) election_timeout: i64,
    pub(crate) script_max_runtime: i64,
    pub(crate) script_retry_delay: i64,
}

impl Default for Timings {
    fn default() -> Self {
        Timings {
            info_period: 10_000,
            ping_period: 1000,
            ask_period: 1000,
            publish_period: 2000,
            default_down_after: 30_000,
            tilt_trigger: 2000,
            tilt_period: 30_000,
            slave_reconf_timeout: 10_000,
            min_link_reconnect_period: 15_000,
            default_failover_timeout: 180_000,
            election_timeout: 10_000,
            script_max_runtime: 60_000,
            script_retry_delay: 30_000,
        }
    }
}

impl Timings {
    pub(crate) fn fields(&mut self) -> [(&'static str, &mut i64); 13] {
        [
            ("info-period", &mut self.info_period),
            ("ping-period", &mut self.ping_period),
            ("ask-period", &mut self.ask_period),
            ("publish-period", &mut self.publish_period),
            ("default-down-after", &mut self.default_down_after),
            ("tilt-trigger", &mut self.tilt_trigger),
            ("tilt-period", &mut self.tilt_period),
            ("slave-reconf-timeout", &mut self.slave_reconf_timeout),
            (
                "min-link-reconnect-period",
                &mut self.min_link_reconnect_period,
            ),
            (
                "default-failover-timeout",
                &mut self.default_failover_timeout,
            ),
            ("election-timeout", &mut self.election_timeout),
            ("script-max-runtime", &mut self.script_max_runtime),
            ("script-retry-delay", &mut self.script_retry_delay),
        ]
    }
}

/// A master as the configuration file has it.
#[derive(Clone, Debug)]
pub(crate) struct MasterConfig {
    pub(crate) name: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) quorum: u32,
    pub(crate) down_after: i64,
    pub(crate) failover_timeout: i64,
    pub(crate) parallel_syncs: u32,
    pub(crate) auth_pass: Option<String>,
    pub(crate) auth_user: Option<String>,
    pub(crate) notification_script: Option<String>,
    pub(crate) client_reconfig_script: Option<String>,
    pub(crate) master_reboot_down_after: i64,
    pub(crate) config_epoch: u64,
    pub(crate) leader_epoch: u64,
    pub(crate) rename: BTreeMap<String, String>,
    /// `known-replica`s: `(host, port)`.
    pub(crate) replicas: Vec<(String, u16)>,
    /// `known-sentinel`s: `(host, port, runid)`.
    pub(crate) sentinels: Vec<(String, u16, String)>,
}

/// What the configuration file says.
#[derive(Clone, Debug, Default)]
pub(crate) struct Config {
    pub(crate) path: PathBuf,
    pub(crate) port: u16,
    pub(crate) bind: Vec<String>,
    pub(crate) myid: Option<String>,
    pub(crate) current_epoch: u64,
    pub(crate) announce_ip: Option<String>,
    pub(crate) announce_port: u16,
    pub(crate) resolve_hostnames: bool,
    pub(crate) announce_hostnames: bool,
    pub(crate) deny_scripts_reconfig: bool,
    /// For clients of this Sentinel (`requirepass`).
    pub(crate) requirepass: Option<String>,
    /// For other Sentinels (`sentinel sentinel-user` / `sentinel-pass`).
    pub(crate) sentinel_user: Option<String>,
    pub(crate) sentinel_pass: Option<String>,
    pub(crate) masters: Vec<MasterConfig>,
    /// Lines kept as they were, for the rewrite: what isn't Sentinel's state.
    pub(crate) kept: Vec<String>,
}

/// The words of a line, as Redis' `sdssplitargs` splits them: by spaces,
/// with double quotes (and their escapes) and single quotes.
pub(crate) fn split_args(line: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        let Some(&first) = chars.peek() else {
            return Some(words);
        };
        let mut word = String::new();
        if first == '"' {
            chars.next();
            loop {
                match chars.next()? {
                    '"' => break,
                    '\\' => match chars.next()? {
                        'n' => word.push('\n'),
                        'r' => word.push('\r'),
                        't' => word.push('\t'),
                        c => word.push(c),
                    },
                    c => word.push(c),
                }
            }
        } else if first == '\'' {
            chars.next();
            loop {
                match chars.next()? {
                    '\'' => break,
                    c => word.push(c),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() {
                    break;
                }
                word.push(c);
                chars.next();
            }
        }
        words.push(word);
    }
}

/// A word as the rewrite writes it: quoted if it needs to be.
fn quote(word: &str) -> String {
    if !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_graphic() && c != '"' && c != '\'')
    {
        return word.to_string();
    }
    let mut out = String::from("\"");
    for c in word.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

impl Config {
    /// Reads `path`, as Redis reads a Sentinel's configuration.
    pub(crate) fn load(path: &Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("can't read the Sentinel config {}: {e}", path.display()))?;
        let mut config = Config {
            path: path.to_path_buf(),
            port: 26379,
            deny_scripts_reconfig: true,
            ..Config::default()
        };
        for (n, line) in text.lines().enumerate() {
            let bad = |why: &str| format!("{}:{}: {why}: {line}", path.display(), n + 1);
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                if !trimmed.starts_with("# Generated by CONFIG REWRITE") {
                    config.kept.push(line.to_string());
                }
                continue;
            }
            let words = split_args(trimmed).ok_or_else(|| bad("unbalanced quotes"))?;
            let word = |i: usize| words.get(i).map(String::as_str).unwrap_or("");
            match word(0).to_ascii_lowercase().as_str() {
                "port" => {
                    config.port = word(1).parse().map_err(|_| bad("bad port"))?;
                }
                "bind" => config.bind = words[1..].to_vec(),
                "requirepass" => {
                    config.requirepass = Some(word(1).to_string()).filter(|p| !p.is_empty());
                }
                "sentinel" => config.sentinel_directive(&words).map_err(|e| bad(&e))?,
                _ => config.kept.push(line.to_string()),
            }
        }
        Ok(config)
    }

    fn master(&mut self, name: &str) -> Result<&mut MasterConfig, String> {
        self.masters
            .iter_mut()
            .find(|m| m.name == name)
            .ok_or_else(|| "No such master with specified name.".to_string())
    }

    fn sentinel_directive(&mut self, words: &[String]) -> Result<(), String> {
        let word = |i: usize| words.get(i).map(String::as_str).unwrap_or("");
        let number = |i: usize| word(i).parse::<i64>().map_err(|_| "bad number".to_string());
        let n = words.len();
        match (word(1).to_ascii_lowercase().as_str(), n) {
            ("monitor", 6) => {
                let quorum = number(5)?;
                if quorum <= 0 {
                    return Err("Quorum must be 1 or greater.".into());
                }
                if self.masters.iter().any(|m| m.name == word(2)) {
                    return Err("Duplicate master name.".into());
                }
                self.masters.push(MasterConfig {
                    name: word(2).to_string(),
                    host: word(3).to_string(),
                    port: word(4).parse().map_err(|_| "bad port".to_string())?,
                    quorum: quorum as u32,
                    down_after: 30_000,
                    failover_timeout: 180_000,
                    parallel_syncs: 1,
                    auth_pass: None,
                    auth_user: None,
                    notification_script: None,
                    client_reconfig_script: None,
                    master_reboot_down_after: 0,
                    config_epoch: 0,
                    leader_epoch: 0,
                    rename: BTreeMap::new(),
                    replicas: Vec::new(),
                    sentinels: Vec::new(),
                });
            }
            ("down-after-milliseconds", 4) => {
                let ms = number(3)?;
                if ms <= 0 {
                    return Err("negative or zero time parameter.".into());
                }
                self.master(word(2))?.down_after = ms;
            }
            ("failover-timeout", 4) => {
                let ms = number(3)?;
                if ms <= 0 {
                    return Err("negative or zero time parameter.".into());
                }
                self.master(word(2))?.failover_timeout = ms;
            }
            ("parallel-syncs", 4) => {
                self.master(word(2))?.parallel_syncs = number(3)?.max(1) as u32
            }
            ("notification-script", 4) => {
                self.master(word(2))?.notification_script = Some(word(3).to_string());
            }
            ("client-reconfig-script", 4) => {
                self.master(word(2))?.client_reconfig_script = Some(word(3).to_string());
            }
            ("auth-pass", 4) => self.master(word(2))?.auth_pass = Some(word(3).to_string()),
            ("auth-user", 4) => self.master(word(2))?.auth_user = Some(word(3).to_string()),
            ("master-reboot-down-after-period", 4) => {
                self.master(word(2))?.master_reboot_down_after = number(3)?.max(0);
            }
            ("current-epoch", 3) => self.current_epoch = number(2)?.max(0) as u64,
            ("myid", 3) => {
                if word(2).len() != 40 {
                    return Err("Malformed Sentinel id in myid option.".into());
                }
                self.myid = Some(word(2).to_string());
            }
            ("config-epoch", 4) => self.master(word(2))?.config_epoch = number(3)?.max(0) as u64,
            ("leader-epoch", 4) => self.master(word(2))?.leader_epoch = number(3)?.max(0) as u64,
            ("known-slave" | "known-replica", 5) => {
                let port = word(4).parse().map_err(|_| "bad port".to_string())?;
                let host = word(3).to_string();
                self.master(word(2))?.replicas.push((host, port));
            }
            ("known-sentinel", 6) => {
                let port = word(4).parse().map_err(|_| "bad port".to_string())?;
                let (host, runid) = (word(3).to_string(), word(5).to_string());
                self.master(word(2))?.sentinels.push((host, port, runid));
            }
            ("rename-command", 5) => {
                let (from, to) = (word(3).to_string(), word(4).to_string());
                self.master(word(2))?.rename.insert(from, to);
            }
            ("announce-ip", 3) => {
                self.announce_ip = Some(word(2).to_string()).filter(|s| !s.is_empty());
            }
            ("announce-port", 3) => self.announce_port = number(2)?.clamp(0, 65535) as u16,
            ("deny-scripts-reconfig", 3) => self.deny_scripts_reconfig = yes(word(2))?,
            ("resolve-hostnames", 3) => self.resolve_hostnames = yes(word(2))?,
            ("announce-hostnames", 3) => self.announce_hostnames = yes(word(2))?,
            ("sentinel-user", 3) => self.sentinel_user = Some(word(2).to_string()),
            ("sentinel-pass", 3) => self.sentinel_pass = Some(word(2).to_string()),
            ("known-replica" | "known-slave" | "known-sentinel", _) => {
                return Err("Wrong number of arguments for known-* option".into());
            }
            _ => return Err("Unrecognized sentinel configuration statement.".into()),
        }
        Ok(())
    }

    /// The file as Redis' Sentinel rewrites it: what was there that isn't
    /// Sentinel's state, then its state.
    pub(crate) fn render(&self) -> String {
        let mut out = String::new();
        for line in &self.kept {
            out.push_str(line);
            out.push('\n');
        }
        while out.ends_with("\n\n") {
            out.pop();
        }
        out.push_str("# Generated by CONFIG REWRITE\n");
        let _ = writeln!(out, "port {}", self.port);
        if !self.bind.is_empty() {
            let bind: Vec<String> = self.bind.iter().map(|b| quote(b)).collect();
            let _ = writeln!(out, "bind {}", bind.join(" "));
        }
        if let Some(pass) = &self.requirepass {
            let _ = writeln!(out, "requirepass {}", quote(pass));
        }
        if let Some(id) = &self.myid {
            let _ = writeln!(out, "sentinel myid {id}");
        }
        let _ = writeln!(
            out,
            "sentinel deny-scripts-reconfig {}",
            if self.deny_scripts_reconfig {
                "yes"
            } else {
                "no"
            }
        );
        if self.resolve_hostnames {
            out.push_str("sentinel resolve-hostnames yes\n");
        }
        if self.announce_hostnames {
            out.push_str("sentinel announce-hostnames yes\n");
        }
        if let Some(ip) = &self.announce_ip {
            let _ = writeln!(out, "sentinel announce-ip {}", quote(ip));
        }
        if self.announce_port != 0 {
            let _ = writeln!(out, "sentinel announce-port {}", self.announce_port);
        }
        if let Some(user) = &self.sentinel_user {
            let _ = writeln!(out, "sentinel sentinel-user {}", quote(user));
        }
        if let Some(pass) = &self.sentinel_pass {
            let _ = writeln!(out, "sentinel sentinel-pass {}", quote(pass));
        }
        for m in &self.masters {
            let name = quote(&m.name);
            let _ = writeln!(
                out,
                "sentinel monitor {name} {} {} {}",
                quote(&m.host),
                m.port,
                m.quorum
            );
            if m.down_after != 30_000 {
                let _ = writeln!(
                    out,
                    "sentinel down-after-milliseconds {name} {}",
                    m.down_after
                );
            }
            if m.failover_timeout != 180_000 {
                let _ = writeln!(
                    out,
                    "sentinel failover-timeout {name} {}",
                    m.failover_timeout
                );
            }
            if m.parallel_syncs != 1 {
                let _ = writeln!(out, "sentinel parallel-syncs {name} {}", m.parallel_syncs);
            }
            if let Some(script) = &m.notification_script {
                let _ = writeln!(out, "sentinel notification-script {name} {}", quote(script));
            }
            if let Some(script) = &m.client_reconfig_script {
                let _ = writeln!(
                    out,
                    "sentinel client-reconfig-script {name} {}",
                    quote(script)
                );
            }
            if let Some(pass) = &m.auth_pass {
                let _ = writeln!(out, "sentinel auth-pass {name} {}", quote(pass));
            }
            if let Some(user) = &m.auth_user {
                let _ = writeln!(out, "sentinel auth-user {name} {}", quote(user));
            }
            if m.master_reboot_down_after != 0 {
                let _ = writeln!(
                    out,
                    "sentinel master-reboot-down-after-period {name} {}",
                    m.master_reboot_down_after
                );
            }
            for (from, to) in &m.rename {
                let _ = writeln!(
                    out,
                    "sentinel rename-command {name} {} {}",
                    quote(from),
                    quote(to)
                );
            }
            let _ = writeln!(out, "sentinel config-epoch {name} {}", m.config_epoch);
            let _ = writeln!(out, "sentinel leader-epoch {name} {}", m.leader_epoch);
            for (host, port) in &m.replicas {
                let _ = writeln!(out, "sentinel known-replica {name} {} {port}", quote(host));
            }
            for (host, port, runid) in &m.sentinels {
                let _ = writeln!(
                    out,
                    "sentinel known-sentinel {name} {} {port} {runid}",
                    quote(host)
                );
            }
        }
        let _ = writeln!(out, "sentinel current-epoch {}", self.current_epoch);
        out
    }

    /// Writes [`Config::render`] over the file, through a temporary one.
    pub(crate) fn save(&self) -> std::io::Result<()> {
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        std::fs::write(&tmp, self.render())?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &self.path)
    }
}

fn yes(word: &str) -> Result<bool, String> {
    match word.to_ascii_lowercase().as_str() {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err("argument must be 'yes' or 'no'".into()),
    }
}

/// `tantalum --sentinel FILE [--port N]`: runs a Sentinel until it's shut
/// down.
pub fn run(path: &Path, port: Option<u16>) -> std::io::Result<()> {
    let mut config = Config::load(path).map_err(std::io::Error::other)?;
    if let Some(port) = port {
        config.port = port;
    }
    // A Sentinel must be able to rewrite its file, as Redis insists.
    if std::fs::OpenOptions::new().append(true).open(path).is_err() {
        return Err(std::io::Error::other(format!(
            "Sentinel config file {} is not writable. Exiting...",
            path.display()
        )));
    }
    if config.myid.is_none() {
        config.myid = Some(new_id());
    }
    config.save()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(monitor::serve(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_config_reads_and_rewrites_the_same() {
        let dir = std::env::temp_dir().join(format!("tantalum-sentinel-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("sentinel.conf");
        std::fs::write(
            &path,
            "# mine\nport 26390\nlogfile \"\"\n\
             sentinel monitor m 127.0.0.1 6379 2\n\
             sentinel down-after-milliseconds m 1000\n\
             sentinel auth-pass m \"p w\"\n\
             sentinel known-replica m 127.0.0.1 6380\n\
             sentinel known-sentinel m 127.0.0.1 26391 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n\
             sentinel current-epoch 3\n",
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.port, 26390);
        let m = &config.masters[0];
        assert_eq!(
            (m.name.as_str(), m.port, m.quorum, m.down_after),
            ("m", 6379, 2, 1000)
        );
        assert_eq!(m.auth_pass.as_deref(), Some("p w"));
        assert_eq!(m.replicas, vec![("127.0.0.1".to_string(), 6380)]);
        assert_eq!(config.current_epoch, 3);
        config.save().unwrap();
        let again = Config::load(&path).unwrap();
        assert_eq!(again.render(), config.render());
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("# mine\nlogfile \"\"\n"), "{text}");
        assert!(
            Config::load(&{
                std::fs::write(&path, "sentinel monitor m 127.0.0.1 6379 0\n").unwrap();
                path.clone()
            })
            .is_err()
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn words_split_as_redis_splits_them() {
        assert_eq!(
            split_args(r#"a "b c" 'd e' "f\"g""#).unwrap(),
            vec!["a", "b c", "d e", "f\"g"]
        );
        assert!(split_args("\"open").is_none());
    }
}
