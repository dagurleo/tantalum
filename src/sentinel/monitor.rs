//! The main loop: what's known of each master, its replicas and the other
//! Sentinels, kept up to date from their links (`PING` every second, `INFO`
//! every ten, or every second while a master is down or failing over, a
//! hello published every two), and acted on ten times a second, as Redis'
//! `sentinelTimer` does: a master that doesn't answer for
//! `down-after-milliseconds` is down by this Sentinel's account (`+sdown`);
//! when `quorum` Sentinels say so it's down (`+odown`), and the Sentinels
//! elect one of them (most votes of all, by epoch) to fail it over: pick
//! the best replica, `SLAVEOF NO ONE` it, point the others at it, and
//! announce the new master (`+switch-master`), which every Sentinel then
//! takes up from the hellos.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};

use super::link::{self, Event, Happened, Link, Login, Tag, Value, Which};
use super::{Config, MasterConfig, Timings, mstime, random_below};

/// What an instance is to this Sentinel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Master,
    Replica,
    Sentinel,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Master => "master",
            Kind::Replica => "slave",
            Kind::Sentinel => "sentinel",
        }
    }
}

/// Where a failover is (Redis' `SENTINEL_FAILOVER_STATE_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FailoverState {
    None,
    WaitStart,
    SelectSlave,
    SendSlaveofNoone,
    WaitPromotion,
    ReconfSlaves,
    UpdateConfig,
}

impl FailoverState {
    pub(crate) fn name(self) -> &'static str {
        match self {
            FailoverState::None => "none",
            FailoverState::WaitStart => "wait_start",
            FailoverState::SelectSlave => "select_slave",
            FailoverState::SendSlaveofNoone => "send_slaveof_noone",
            FailoverState::WaitPromotion => "wait_promotion",
            FailoverState::ReconfSlaves => "reconf_slaves",
            FailoverState::UpdateConfig => "update_config",
        }
    }
}

/// A master, a replica or a Sentinel, as this one sees it.
pub(crate) struct Instance {
    pub(crate) id: u64,
    pub(crate) kind: Kind,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) runid: Option<String>,
    // Flags (Redis' `SRI_*`).
    pub(crate) s_down: bool,
    pub(crate) o_down: bool,
    /// A Sentinel that says the master is down.
    pub(crate) master_down: bool,
    pub(crate) promoted: bool,
    pub(crate) reconf_sent: bool,
    pub(crate) reconf_inprog: bool,
    pub(crate) reconf_done: bool,
    pub(crate) script_kill_sent: bool,
    pub(crate) master_reboot: bool,
    pub(crate) s_down_since: i64,
    pub(crate) o_down_since: i64,
    // The links.
    cmd: Option<Link>,
    pubsub: Option<Link>,
    pub(crate) cmd_up: bool,
    pubsub_up: bool,
    generation: u64,
    last_reconnect: i64,
    pub(crate) pending: u32,
    cc_conn_time: i64,
    pc_conn_time: i64,
    pc_last_activity: i64,
    pub(crate) act_ping_time: i64,
    pub(crate) last_ping_time: i64,
    pub(crate) last_pong_time: i64,
    pub(crate) last_avail_time: i64,
    last_pub_time: i64,
    // From `INFO`.
    pub(crate) info: String,
    pub(crate) info_refresh: i64,
    pub(crate) role_reported: Kind,
    pub(crate) role_reported_time: i64,
    slave_conf_change_time: i64,
    pub(crate) master_link_down_time: i64,
    pub(crate) slave_master_host: Option<String>,
    pub(crate) slave_master_port: u16,
    pub(crate) slave_master_link_up: bool,
    pub(crate) slave_priority: i64,
    pub(crate) slave_repl_offset: u64,
    pub(crate) replica_announced: bool,
    master_reboot_since: i64,
    slave_reconf_sent_time: i64,
    // A Sentinel's.
    pub(crate) last_hello_time: i64,
    pub(crate) leader: Option<String>,
    pub(crate) leader_epoch: u64,
    last_master_down_reply_time: i64,
}

impl Instance {
    fn new(id: u64, kind: Kind, host: String, port: u16) -> Instance {
        let now = mstime();
        Instance {
            id,
            kind,
            host,
            port,
            runid: None,
            s_down: false,
            o_down: false,
            master_down: false,
            promoted: false,
            reconf_sent: false,
            reconf_inprog: false,
            reconf_done: false,
            script_kill_sent: false,
            master_reboot: false,
            s_down_since: 0,
            o_down_since: 0,
            cmd: None,
            pubsub: None,
            cmd_up: false,
            pubsub_up: false,
            generation: 0,
            last_reconnect: 0,
            pending: 0,
            cc_conn_time: 0,
            pc_conn_time: 0,
            pc_last_activity: 0,
            act_ping_time: now,
            last_ping_time: 0,
            last_pong_time: now,
            last_avail_time: now,
            last_pub_time: 0,
            info: String::new(),
            info_refresh: 0,
            role_reported: kind,
            role_reported_time: now,
            slave_conf_change_time: now,
            master_link_down_time: 0,
            slave_master_host: None,
            slave_master_port: 0,
            slave_master_link_up: false,
            slave_priority: 100,
            slave_repl_offset: 0,
            replica_announced: true,
            master_reboot_since: 0,
            slave_reconf_sent_time: 0,
            last_hello_time: now,
            leader: None,
            leader_epoch: 0,
            last_master_down_reply_time: now,
        }
    }

    /// Its name in replies and events: a Sentinel's ID, a replica's
    /// address (a master's is the master's name).
    pub(crate) fn name(&self) -> String {
        match (&self.runid, self.kind) {
            (Some(id), Kind::Sentinel) => id.clone(),
            _ => format!("{}:{}", self.host, self.port),
        }
    }

    /// Redis' `link->disconnected`: a link it needs isn't up.
    pub(crate) fn disconnected(&self) -> bool {
        !self.cmd_up || (self.kind != Kind::Sentinel && !self.pubsub_up)
    }

    pub(crate) fn close_links(&mut self) {
        self.cmd = None;
        self.pubsub = None;
        self.cmd_up = false;
        self.pubsub_up = false;
        self.pending = 0;
        self.generation += 1;
    }

    fn send(&mut self, args: Vec<Vec<u8>>, tag: Tag) -> bool {
        let sent = self.cmd_up && self.cmd.as_ref().is_some_and(|l| l.send(args, tag));
        if sent {
            self.pending += 1;
        }
        sent
    }

    /// `sentinelRedisInstanceNoDownFor`.
    fn no_down_for(&self, ms: i64) -> bool {
        let recent = self.s_down_since.max(self.o_down_since);
        recent == 0 || mstime() - recent > ms
    }

    pub(crate) fn flags(&self, failover: bool, force: bool) -> String {
        let mut flags = Vec::new();
        if self.s_down {
            flags.push("s_down");
        }
        if self.o_down {
            flags.push("o_down");
        }
        flags.push(self.kind.name());
        if self.disconnected() {
            flags.push("disconnected");
        }
        if self.master_down {
            flags.push("master_down");
        }
        if failover {
            flags.push("failover_in_progress");
        }
        if self.promoted {
            flags.push("promoted");
        }
        if self.reconf_sent {
            flags.push("reconf_sent");
        }
        if self.reconf_inprog {
            flags.push("reconf_inprog");
        }
        if self.reconf_done {
            flags.push("reconf_done");
        }
        if force {
            flags.push("force_failover");
        }
        if self.script_kill_sent {
            flags.push("script_kill_sent");
        }
        if self.master_reboot {
            flags.push("master_reboot");
        }
        flags.join(",")
    }
}

/// A master and what goes with it.
pub(crate) struct Master {
    pub(crate) name: String,
    pub(crate) me: Instance,
    pub(crate) quorum: u32,
    pub(crate) down_after: i64,
    pub(crate) failover_timeout: i64,
    pub(crate) parallel_syncs: u32,
    pub(crate) auth_pass: Option<String>,
    pub(crate) auth_user: Option<String>,
    pub(crate) notification_script: Option<String>,
    pub(crate) client_reconfig_script: Option<String>,
    pub(crate) master_reboot_down_after: i64,
    pub(crate) rename: BTreeMap<String, String>,
    pub(crate) config_epoch: u64,
    pub(crate) leader: Option<String>,
    pub(crate) leader_epoch: u64,
    pub(crate) failover_epoch: u64,
    pub(crate) failover_state: FailoverState,
    pub(crate) failover_state_change_time: i64,
    pub(crate) failover_start_time: i64,
    failover_delay_logged: i64,
    pub(crate) failover_in_progress: bool,
    pub(crate) force_failover: bool,
    /// Index into `replicas`.
    pub(crate) promoted: Option<usize>,
    pub(crate) replicas: Vec<Instance>,
    pub(crate) sentinels: Vec<Instance>,
}

impl Master {
    /// The address clients are told: the promoted replica's once it has
    /// taken over (`sentinelGetCurrentMasterAddress`).
    pub(crate) fn current_addr(&self) -> (String, u16) {
        if self.failover_in_progress
            && self.failover_state >= FailoverState::ReconfSlaves
            && let Some(p) = self.promoted
        {
            let r = &self.replicas[p];
            return (r.host.clone(), r.port);
        }
        (self.me.host.clone(), self.me.port)
    }

    /// `sentinelMasterLooksSane`.
    fn looks_sane(&self, t: &Timings) -> bool {
        self.me.role_reported == Kind::Master
            && !self.me.s_down
            && !self.me.o_down
            && mstime() - self.me.info_refresh < t.info_period * 2
    }

    /// A command's name as this master's instances know it
    /// (`rename-command`).
    fn command(&self, name: &str) -> Vec<u8> {
        self.rename
            .iter()
            .find(|(from, _)| from.eq_ignore_ascii_case(name))
            .map_or_else(
                || name.as_bytes().to_vec(),
                |(_, to)| to.as_bytes().to_vec(),
            )
    }
}

/// A client's command, for the main loop to answer.
pub(crate) struct Request {
    pub(crate) args: Vec<Vec<u8>>,
    pub(crate) resp3: bool,
    pub(crate) reply: oneshot::Sender<Vec<u8>>,
}

/// Everything the Sentinel knows.
pub(crate) struct State {
    pub(crate) config: Config,
    pub(crate) myid: String,
    pub(crate) current_epoch: u64,
    pub(crate) masters: Vec<Master>,
    pub(crate) timings: Timings,
    pub(crate) tilt: bool,
    pub(crate) tilt_start: i64,
    previous_time: i64,
    pub(crate) simfailure_crash_after_election: bool,
    pub(crate) simfailure_crash_after_promotion: bool,
    next_id: u64,
    pub(crate) events: mpsc::UnboundedSender<Event>,
    /// Pub/sub messages for clients: `(channel, message)`.
    pub(crate) publish: broadcast::Sender<(String, String)>,
    pub(crate) started: i64,
    pub(crate) running_scripts: usize,
    pub(crate) stats: server::Stats,
}

use super::server;

impl State {
    fn new(
        config: Config,
        events: mpsc::UnboundedSender<Event>,
        publish: broadcast::Sender<(String, String)>,
    ) -> State {
        let mut state = State {
            myid: config.myid.clone().expect("an ID by now"),
            current_epoch: config.current_epoch,
            masters: Vec::new(),
            timings: Timings::default(),
            tilt: false,
            tilt_start: 0,
            previous_time: mstime(),
            simfailure_crash_after_election: false,
            simfailure_crash_after_promotion: false,
            next_id: 1,
            events,
            publish,
            started: mstime(),
            running_scripts: 0,
            stats: server::Stats::default(),
            config,
        };
        for m in state.config.masters.clone() {
            state.add_master(&m);
        }
        state
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    pub(crate) fn add_master(&mut self, m: &MasterConfig) {
        let id = self.id();
        let mut master = Master {
            name: m.name.clone(),
            me: Instance::new(id, Kind::Master, m.host.clone(), m.port),
            quorum: m.quorum,
            down_after: m.down_after,
            failover_timeout: m.failover_timeout,
            parallel_syncs: m.parallel_syncs,
            auth_pass: m.auth_pass.clone(),
            auth_user: m.auth_user.clone(),
            notification_script: m.notification_script.clone(),
            client_reconfig_script: m.client_reconfig_script.clone(),
            master_reboot_down_after: m.master_reboot_down_after,
            rename: m.rename.clone(),
            config_epoch: m.config_epoch,
            leader: None,
            leader_epoch: m.leader_epoch,
            failover_epoch: 0,
            failover_state: FailoverState::None,
            failover_state_change_time: 0,
            failover_start_time: 0,
            failover_delay_logged: 0,
            failover_in_progress: false,
            force_failover: false,
            promoted: None,
            replicas: Vec::new(),
            sentinels: Vec::new(),
        };
        for (host, port) in &m.replicas {
            let id = self.id();
            master
                .replicas
                .push(Instance::new(id, Kind::Replica, host.clone(), *port));
        }
        for (host, port, runid) in &m.sentinels {
            let id = self.id();
            let mut s = Instance::new(id, Kind::Sentinel, host.clone(), *port);
            s.runid = Some(runid.clone());
            master.sentinels.push(s);
        }
        self.masters.push(master);
    }

    /// The configuration as it stands, written to the file
    /// (`sentinelFlushConfig`).
    pub(crate) fn flush_config(&mut self) -> std::io::Result<()> {
        self.config.current_epoch = self.current_epoch;
        self.config.myid = Some(self.myid.clone());
        self.config.masters = self
            .masters
            .iter()
            .map(|m| MasterConfig {
                name: m.name.clone(),
                host: m.me.host.clone(),
                port: m.me.port,
                quorum: m.quorum,
                down_after: m.down_after,
                failover_timeout: m.failover_timeout,
                parallel_syncs: m.parallel_syncs,
                auth_pass: m.auth_pass.clone(),
                auth_user: m.auth_user.clone(),
                notification_script: m.notification_script.clone(),
                client_reconfig_script: m.client_reconfig_script.clone(),
                master_reboot_down_after: m.master_reboot_down_after,
                config_epoch: m.config_epoch,
                leader_epoch: m.leader_epoch,
                rename: m.rename.clone(),
                // The replica that took over is the master now.
                replicas: m
                    .replicas
                    .iter()
                    .filter(|r| (r.host.as_str(), r.port) != (m.me.host.as_str(), m.me.port))
                    .map(|r| (r.host.clone(), r.port))
                    .collect(),
                sentinels: m
                    .sentinels
                    .iter()
                    .filter_map(|s| Some((s.host.clone(), s.port, s.runid.clone()?)))
                    .collect(),
            })
            .collect();
        let saved = self.config.save();
        if let Err(e) = &saved {
            log(&format!(
                "WARNING: Sentinel was not able to save the new configuration on disk!!!: {e}"
            ));
        }
        saved
    }

    /// `sentinelEvent`: logged, published to clients on a channel named for
    /// it, and given to the master's notification script if it's a warning.
    pub(crate) fn event(&mut self, warning: bool, kind: &str, m: usize, who: Who, rest: &str) {
        let master = &self.masters[m];
        let mut msg = match who {
            Who::None => String::new(),
            Who::Master => format!(
                "master {} {} {}",
                master.name, master.me.host, master.me.port
            ),
            Who::Replica(i) | Who::Sentinel(i) => {
                let inst = match who {
                    Who::Replica(_) => &master.replicas[i],
                    _ => &master.sentinels[i],
                };
                format!(
                    "{} {} {} {} @ {} {} {}",
                    inst.kind.name(),
                    inst.name(),
                    inst.host,
                    inst.port,
                    master.name,
                    master.me.host,
                    master.me.port
                )
            }
        };
        if !rest.is_empty() {
            if !msg.is_empty() {
                msg.push(' ');
            }
            msg.push_str(rest);
        }
        self.emit(warning, kind, &msg, Some(m));
    }

    pub(crate) fn emit(&mut self, warning: bool, kind: &str, msg: &str, m: Option<usize>) {
        log(&format!("{} {kind} {msg}", if warning { "#" } else { "*" }));
        let _ = self.publish.send((kind.to_string(), msg.to_string()));
        if warning
            && let Some(m) = m
            && let Some(script) = self.masters[m].notification_script.clone()
        {
            self.run_script(script, vec![kind.to_string(), msg.to_string()]);
        }
    }

    fn run_script(&mut self, script: String, args: Vec<String>) {
        let max = Duration::from_millis(self.timings.script_max_runtime as u64);
        std::thread::spawn(move || {
            let Ok(mut child) = std::process::Command::new(&script).args(&args).spawn() else {
                log(&format!("-script-error {script} could not be run"));
                return;
            };
            let started = std::time::Instant::now();
            loop {
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => return,
                    Ok(None) if started.elapsed() > max => {
                        let _ = child.kill();
                        log(&format!("-script-timeout {script}"));
                        return;
                    }
                    Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                }
            }
        });
    }

    fn login(&self, m: usize, kind: Kind, which: Which) -> Login {
        let master = &self.masters[m];
        let (user, pass) = match kind {
            Kind::Sentinel => (
                self.config.sentinel_user.clone(),
                self.config.sentinel_pass.clone(),
            ),
            _ => (master.auth_user.clone(), master.auth_pass.clone()),
        };
        Login {
            user,
            pass,
            name: format!(
                "sentinel-{}-{}",
                &self.myid[..8],
                if which == Which::Cmd { "cmd" } else { "pubsub" }
            ),
        }
    }

    fn instance(&mut self, m: usize, who: Who) -> &mut Instance {
        let master = &mut self.masters[m];
        match who {
            Who::Master | Who::None => &mut master.me,
            Who::Replica(i) => &mut master.replicas[i],
            Who::Sentinel(i) => &mut master.sentinels[i],
        }
    }

    fn find(&self, id: u64) -> Option<(usize, Who)> {
        for (m, master) in self.masters.iter().enumerate() {
            if master.me.id == id {
                return Some((m, Who::Master));
            }
            if let Some(i) = master.replicas.iter().position(|r| r.id == id) {
                return Some((m, Who::Replica(i)));
            }
            if let Some(i) = master.sentinels.iter().position(|s| s.id == id) {
                return Some((m, Who::Sentinel(i)));
            }
        }
        None
    }
}

/// Which instance of a master.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Who {
    None,
    Master,
    Replica(usize),
    Sentinel(usize),
}

pub(crate) fn log(line: &str) {
    let now = mstime();
    let secs = now / 1000;
    let (h, mi, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    eprintln!(
        "{}:X {h:02}:{mi:02}:{s:02}.{:03} {line}",
        std::process::id(),
        now % 1000
    );
}

/// Runs the Sentinel: its clients' port and its main loop.
pub(crate) async fn serve(config: Config) -> std::io::Result<()> {
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let (publish, _) = broadcast::channel(4096);
    let (requests_tx, mut requests) = mpsc::unbounded_channel::<Request>();
    let listeners = server::listen(&config).await?;
    let mut state = State::new(config, events_tx, publish.clone());
    log(&format!(
        "Sentinel ID is {}; listening on port {}",
        state.myid, state.config.port
    ));
    for m in 0..state.masters.len() {
        state.event(
            true,
            "+monitor",
            m,
            Who::Master,
            &format!("quorum {}", state.masters[m].quorum),
        );
    }
    let shared = server::Shared::new(requests_tx, publish, state.config.requirepass.clone());
    for listener in listeners {
        tokio::spawn(server::accept(listener, shared.clone()));
    }
    let mut next_tick = tokio::time::Instant::now();
    loop {
        tokio::select! {
            Some(event) = events.recv() => handle_event(&mut state, event),
            Some(request) = requests.recv() => {
                let reply = server::command(&mut state, &request.args, request.resp3);
                let _ = request.reply.send(reply);
                if state.stats.shutdown {
                    let _ = state.flush_config();
                    log("# User requested shutdown...");
                    return Ok(());
                }
            }
            _ = tokio::time::sleep_until(next_tick) => {
                timer(&mut state);
                // Ten times a second or so, never in step with another
                // Sentinel's (`server.hz` changes each time in Redis).
                let hz = 10 + random_below(10);
                next_tick = tokio::time::Instant::now() + Duration::from_millis(1000 / hz);
            }
        }
    }
}

fn handle_event(state: &mut State, event: Event) {
    let Some((m, who)) = state.find(event.instance) else {
        return;
    };
    let now = mstime();
    let inst = state.instance(m, who);
    if inst.generation != event.generation {
        return;
    }
    match event.happened {
        Happened::Up => match event.which {
            Which::Cmd => {
                inst.cmd_up = true;
                inst.cc_conn_time = now;
                inst.pending = 0;
                send_ping(inst);
            }
            Which::Pubsub => {
                inst.pubsub_up = true;
                inst.pc_conn_time = now;
                inst.pc_last_activity = now;
            }
        },
        Happened::Down => {
            match event.which {
                Which::Cmd => {
                    inst.cmd = None;
                    inst.cmd_up = false;
                    inst.pending = 0;
                }
                Which::Pubsub => {
                    inst.pubsub = None;
                    inst.pubsub_up = false;
                }
            }
            // Anything still to come from the other link is from a
            // connection being replaced.
        }
        Happened::Activity => inst.pc_last_activity = now,
        Happened::Hello(message) => {
            let message = String::from_utf8_lossy(&message).into_owned();
            if !message.contains(&state.myid) {
                process_hello(state, &message);
            }
        }
        Happened::Reply(tag, value) => {
            inst.pending = inst.pending.saturating_sub(1);
            match tag {
                Tag::Ping => {
                    let text = match &value {
                        Value::Simple(s) | Value::Error(s) => s.clone(),
                        _ => String::new(),
                    };
                    let ok = text.starts_with("PONG")
                        || text.starts_with("LOADING")
                        || text.starts_with("MASTERDOWN");
                    if ok {
                        inst.last_avail_time = now;
                        inst.act_ping_time = 0;
                        if inst.master_reboot && text.starts_with("PONG") {
                            inst.master_reboot = false;
                        }
                    } else if text.starts_with("BUSY") && inst.s_down && !inst.script_kill_sent {
                        let kill = vec![state.masters[m].command("SCRIPT"), b"KILL".to_vec()];
                        let inst = state.instance(m, who);
                        inst.send(kill, Tag::Discard);
                        inst.script_kill_sent = true;
                    }
                    state.instance(m, who).last_pong_time = now;
                }
                Tag::Info => {
                    if let Some(info) = value.text() {
                        refresh_info(state, m, who, &info);
                    }
                }
                Tag::Publish => {
                    if !matches!(value, Value::Error(_)) {
                        inst.last_pub_time = now;
                    }
                }
                Tag::IsMasterDown => {
                    if let Value::Array(Some(items)) = &value
                        && let [
                            Value::Int(down),
                            Value::Bulk(Some(leader)),
                            Value::Int(epoch),
                        ] = items.as_slice()
                    {
                        inst.last_master_down_reply_time = now;
                        inst.master_down = *down == 1;
                        let leader = String::from_utf8_lossy(leader).into_owned();
                        if leader != "*" {
                            if inst.leader_epoch as i64 != *epoch {
                                log(&format!(
                                    "* {}:{} voted for {leader} {epoch}",
                                    inst.host, inst.port
                                ));
                            }
                            inst.leader = Some(leader);
                            inst.leader_epoch = (*epoch).max(0) as u64;
                        }
                    }
                }
                Tag::Discard => {}
            }
        }
    }
}

fn send_ping(inst: &mut Instance) -> bool {
    if inst.send(link::args(&[b"PING"]), Tag::Ping) {
        inst.last_ping_time = mstime();
        if inst.act_ping_time == 0 {
            inst.act_ping_time = inst.last_ping_time;
        }
        true
    } else {
        false
    }
}

/// `sentinelTimer`.
fn timer(state: &mut State) {
    // TILT: the clock jumped, or this process was held up: watch, don't act.
    let now = mstime();
    let delta = now - state.previous_time;
    if delta < 0 || delta > state.timings.tilt_trigger {
        state.tilt = true;
        state.tilt_start = now;
        state.emit(true, "+tilt", "#tilt mode entered", None);
    }
    state.previous_time = now;
    let mut switch = None;
    for m in 0..state.masters.len() {
        handle_instance(state, m, Who::Master);
        for i in 0..state.masters[m].replicas.len() {
            handle_instance(state, m, Who::Replica(i));
        }
        for i in 0..state.masters[m].sentinels.len() {
            handle_instance(state, m, Who::Sentinel(i));
        }
        if state.masters[m].failover_state == FailoverState::UpdateConfig {
            switch = Some(m);
        }
    }
    if let Some(m) = switch {
        switch_to_promoted(state, m);
    }
}

/// `sentinelHandleRedisInstance`.
fn handle_instance(state: &mut State, m: usize, who: Who) {
    reconnect(state, m, who);
    send_periodic(state, m, who);
    if state.tilt {
        if mstime() - state.tilt_start < state.timings.tilt_period {
            return;
        }
        state.tilt = false;
        state.emit(true, "-tilt", "#tilt mode exited", None);
    }
    check_subjectively_down(state, m, who);
    if who == Who::Master {
        check_objectively_down(state, m);
        if start_failover_if_needed(state, m) {
            ask_other_sentinels(state, m, true);
        }
        failover_state_machine(state, m);
        ask_other_sentinels(state, m, false);
    }
}

/// `sentinelReconnectInstance`: links that aren't up are started again,
/// once a ping period.
fn reconnect(state: &mut State, m: usize, who: Who) {
    let now = mstime();
    let ping_period = state.timings.ping_period;
    let events = state.events.clone();
    let kind = state.instance(m, who).kind;
    let logins = (
        state.login(m, kind, Which::Cmd),
        state.login(m, kind, Which::Pubsub),
    );
    let inst = state.instance(m, who);
    if !inst.disconnected() || inst.port == 0 || now - inst.last_reconnect < ping_period {
        return;
    }
    inst.last_reconnect = now;
    if inst.cmd.is_none() {
        inst.generation += 1;
        inst.cmd = Some(Link::connect(
            inst.id,
            Which::Cmd,
            inst.generation,
            inst.host.clone(),
            inst.port,
            logins.0,
            events.clone(),
        ));
    }
    if inst.kind != Kind::Sentinel && inst.pubsub.is_none() {
        inst.pubsub = Some(Link::connect(
            inst.id,
            Which::Pubsub,
            inst.generation,
            inst.host.clone(),
            inst.port,
            logins.1,
            events,
        ));
    }
}

/// `sentinelSendPeriodicCommands`.
fn send_periodic(state: &mut State, m: usize, who: Who) {
    let now = mstime();
    let t = state.timings.clone();
    let master_busy = {
        let master = &state.masters[m];
        master.me.o_down || master.failover_in_progress
    };
    let down_after = state.masters[m].down_after;
    let info_cmd = state.masters[m].command("INFO");
    let ping_cmd = state.masters[m].command("PING");
    let publish_cmd = state.masters[m].command("PUBLISH");
    let hello = hello_message(state, m, who);
    let inst = state.instance(m, who);
    if inst.disconnected() || inst.pending >= 100 {
        return;
    }
    let info_period =
        if inst.kind == Kind::Replica && (master_busy || inst.master_link_down_time != 0) {
            1000
        } else {
            t.info_period
        };
    let ping_period = down_after.min(t.ping_period);
    if inst.kind != Kind::Sentinel
        && (inst.info_refresh == 0 || now - inst.info_refresh > info_period)
    {
        inst.send(vec![info_cmd], Tag::Info);
    }
    if now - inst.last_pong_time > ping_period
        && now - inst.last_ping_time > ping_period / 2
        && inst.send(vec![ping_cmd], Tag::Ping)
    {
        inst.last_ping_time = now;
        if inst.act_ping_time == 0 {
            inst.act_ping_time = now;
        }
    }
    if now - inst.last_pub_time > t.publish_period
        && let Some(hello) = hello
    {
        inst.send(
            vec![
                publish_cmd,
                b"__sentinel__:hello".to_vec(),
                hello.into_bytes(),
            ],
            Tag::Publish,
        );
    }
}

/// The hello this Sentinel publishes: its address, ID and epoch, and the
/// master's name, address and configuration epoch.
fn hello_message(state: &State, m: usize, _who: Who) -> Option<String> {
    let master = &state.masters[m];
    let (host, port) = master.current_addr();
    let ip = state
        .config
        .announce_ip
        .clone()
        .or_else(|| local_ip(&state.config))?;
    let my_port = if state.config.announce_port != 0 {
        state.config.announce_port
    } else {
        state.config.port
    };
    Some(format!(
        "{ip},{my_port},{},{},{},{host},{port},{}",
        state.myid, state.current_epoch, master.name, master.config_epoch
    ))
}

/// The address others reach this Sentinel at, if it's bound to one, or the
/// loopback otherwise (Redis takes it from the link's socket; this is
/// what that gives on one machine, and `announce-ip` says otherwise).
fn local_ip(config: &Config) -> Option<String> {
    config
        .bind
        .iter()
        .find(|b| {
            b.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| !ip.is_unspecified())
        })
        .cloned()
        .or_else(|| Some("127.0.0.1".to_string()))
}

/// `sentinelProcessHelloMessage`.
pub(crate) fn process_hello(state: &mut State, hello: &str) {
    let token: Vec<&str> = hello.split(',').collect();
    if token.len() != 8 {
        return;
    }
    let Some(m) = state.masters.iter().position(|x| x.name == token[4]) else {
        return;
    };
    let port: u16 = token[1].parse().unwrap_or(0);
    let master_port: u16 = token[6].parse().unwrap_or(0);
    let current_epoch: u64 = token[3].parse().unwrap_or(0);
    let master_config_epoch: u64 = token[7].parse().unwrap_or(0);
    let (ip, runid) = (token[0].to_string(), token[2].to_string());
    let mut si = state.masters[m]
        .sentinels
        .iter()
        .position(|s| s.host == ip && s.port == port && s.runid.as_deref() == Some(&runid));
    if si.is_none() {
        // A Sentinel known by its ID at another address: moved.
        let before = state.masters[m].sentinels.len();
        state.masters[m]
            .sentinels
            .retain(|s| s.runid.as_deref() != Some(&runid));
        let removed = state.masters[m].sentinels.len() != before;
        if removed {
            state.event(
                false,
                "+sentinel-address-switch",
                m,
                Who::Master,
                &format!("ip {ip} port {port} for {runid}"),
            );
        } else if let Some(other) = state.masters[m]
            .sentinels
            .iter()
            .position(|s| s.host == ip && s.port == port)
        {
            // Another ID at this address: the old one is gone everywhere.
            state.event(false, "+sentinel-invalid-addr", m, Who::Sentinel(other), "");
            let obsolete = state.masters[m].sentinels[other].runid.clone();
            for master in &mut state.masters {
                master.sentinels.retain(|s| s.runid != obsolete);
            }
        }
        let id = state.id();
        let mut s = Instance::new(id, Kind::Sentinel, ip.clone(), port);
        s.runid = Some(runid.clone());
        state.masters[m].sentinels.push(s);
        let i = state.masters[m].sentinels.len() - 1;
        if !removed {
            state.event(false, "+sentinel", m, Who::Sentinel(i), "");
        }
        let _ = state.flush_config();
        si = Some(i);
    }
    if current_epoch > state.current_epoch {
        state.current_epoch = current_epoch;
        let _ = state.flush_config();
        state.event(true, "+new-epoch", m, Who::None, &current_epoch.to_string());
    }
    let Some(si) = si else { return };
    if state.masters[m].config_epoch < master_config_epoch {
        state.masters[m].config_epoch = master_config_epoch;
        let (old_host, old_port) = (state.masters[m].me.host.clone(), state.masters[m].me.port);
        if master_port != old_port || token[5] != old_host {
            state.event(true, "+config-update-from", m, Who::Sentinel(si), "");
            let name = state.masters[m].name.clone();
            state.event(
                true,
                "+switch-master",
                m,
                Who::None,
                &format!("{name} {old_host} {old_port} {} {master_port}", token[5]),
            );
            reset_and_change_address(state, m, token[5].to_string(), master_port);
            if let Some(script) = state.masters[m].client_reconfig_script.clone() {
                let args = vec![
                    name,
                    "observer".into(),
                    "start".into(),
                    old_host,
                    old_port.to_string(),
                    token[5].to_string(),
                    master_port.to_string(),
                ];
                state.run_script(script, args);
            }
            let _ = state.flush_config();
        }
    }
    if let Some(s) = state.masters.get_mut(m).and_then(|master| {
        master
            .sentinels
            .iter_mut()
            .find(|s| s.runid.as_deref() == Some(&runid))
    }) {
        s.last_hello_time = mstime();
    }
}

/// `sentinelRefreshInstanceInfo`: what an instance's `INFO` says, and what
/// follows from it.
fn refresh_info(state: &mut State, m: usize, who: Who, info: &str) {
    let now = mstime();
    let mut role = None;
    let mut found_replicas = Vec::new();
    let mut rebooted = false;
    {
        let reboot_after = state.masters[m].master_reboot_down_after;
        let inst = state.instance(m, who);
        inst.info = info.to_string();
        inst.master_link_down_time = 0;
        for line in info.split("\r\n") {
            if let Some(id) = line.strip_prefix("run_id:")
                && id.len() >= 40
            {
                let id = id[..40].to_string();
                match &inst.runid {
                    None => inst.runid = Some(id),
                    Some(old) if *old != id => {
                        rebooted = true;
                        if inst.kind == Kind::Master && reboot_after != 0 {
                            inst.master_reboot = true;
                            inst.master_reboot_since = now;
                        }
                        inst.runid = Some(id);
                    }
                    _ => {}
                }
            }
            if inst.kind == Kind::Master
                && line.starts_with("slave")
                && line.as_bytes().get(5).is_some_and(u8::is_ascii_digit)
                && let Some((_, fields)) = line.split_once(':')
            {
                let field = |name: &str| {
                    fields
                        .split(',')
                        .find_map(|f| f.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
                };
                if let (Some(ip), Some(port)) = (
                    field("ip"),
                    field("port").and_then(|p| p.parse::<u16>().ok()),
                ) {
                    found_replicas.push((ip.to_string(), port));
                }
            }
            if let Some(secs) = line.strip_prefix("master_link_down_since_seconds:") {
                inst.master_link_down_time = secs.parse::<i64>().unwrap_or(0) * 1000;
            }
            if line.starts_with("role:master") {
                role = Some(Kind::Master);
            } else if line.starts_with("role:slave") {
                role = Some(Kind::Replica);
            }
            if role == Some(Kind::Replica) {
                if let Some(host) = line.strip_prefix("master_host:")
                    && inst
                        .slave_master_host
                        .as_deref()
                        .is_none_or(|h| !h.eq_ignore_ascii_case(host))
                {
                    inst.slave_master_host = Some(host.to_string());
                    inst.slave_conf_change_time = now;
                }
                if let Some(port) = line.strip_prefix("master_port:") {
                    let port = port.parse().unwrap_or(0);
                    if inst.slave_master_port != port {
                        inst.slave_master_port = port;
                        inst.slave_conf_change_time = now;
                    }
                }
                if let Some(status) = line.strip_prefix("master_link_status:") {
                    inst.slave_master_link_up = status.eq_ignore_ascii_case("up");
                }
                if let Some(p) = line.strip_prefix("slave_priority:") {
                    inst.slave_priority = p.parse().unwrap_or(100);
                }
                if let Some(o) = line.strip_prefix("slave_repl_offset:") {
                    inst.slave_repl_offset = o.parse().unwrap_or(0);
                }
                if let Some(a) = line.strip_prefix("replica_announced:") {
                    inst.replica_announced = a.parse::<i64>().unwrap_or(1) != 0;
                }
            }
        }
        inst.info_refresh = now;
    }
    if rebooted {
        state.event(false, "+reboot", m, who, "");
    }
    for (ip, port) in found_replicas {
        let master = &state.masters[m];
        if !master
            .replicas
            .iter()
            .any(|r| r.host == ip && r.port == port)
        {
            let id = state.id();
            state.masters[m]
                .replicas
                .push(Instance::new(id, Kind::Replica, ip, port));
            let i = state.masters[m].replicas.len() - 1;
            state.event(false, "+slave", m, Who::Replica(i), "");
            let _ = state.flush_config();
        }
    }
    // The acting half.
    let Some(role) = role else {
        return;
    };
    let kind = state.instance(m, who).kind;
    if role != state.instance(m, who).role_reported {
        let inst = state.instance(m, who);
        inst.role_reported_time = now;
        inst.role_reported = role;
        if role == Kind::Replica {
            inst.slave_conf_change_time = now;
        }
        let coherent = if kind == role {
            "+role-change"
        } else {
            "-role-change"
        };
        let name = if role == Kind::Master {
            "master"
        } else {
            "slave"
        };
        state.event(
            false,
            coherent,
            m,
            who,
            &format!("new reported role is {name}"),
        );
    }
    if state.tilt {
        return;
    }
    let t = state.timings.clone();
    // A replica that turned master.
    if let Who::Replica(i) = who
        && role == Kind::Master
    {
        let master = &state.masters[m];
        if master.replicas[i].promoted
            && master.failover_in_progress
            && master.failover_state == FailoverState::WaitPromotion
        {
            let master = &mut state.masters[m];
            master.config_epoch = master.failover_epoch;
            master.failover_state = FailoverState::ReconfSlaves;
            master.failover_state_change_time = now;
            let _ = state.flush_config();
            state.event(true, "+promoted-slave", m, who, "");
            if state.simfailure_crash_after_promotion {
                log("# Sentinel CRASH because of SENTINEL simulate-failure");
                std::process::exit(99);
            }
            state.event(true, "+failover-state-reconf-slaves", m, Who::Master, "");
            if let Some(script) = state.masters[m].client_reconfig_script.clone() {
                let master = &state.masters[m];
                let r = &master.replicas[i];
                let args = vec![
                    master.name.clone(),
                    "leader".into(),
                    "start".into(),
                    master.me.host.clone(),
                    master.me.port.to_string(),
                    r.host.clone(),
                    r.port.to_string(),
                ];
                state.run_script(script, args);
            }
            // Tell the others at once.
            let master = &mut state.masters[m];
            for inst in std::iter::once(&mut master.me)
                .chain(master.replicas.iter_mut())
                .chain(master.sentinels.iter_mut())
            {
                inst.last_pub_time = 0;
            }
        } else {
            let wait = t.publish_period * 4;
            let master = &state.masters[m];
            let r = &master.replicas[i];
            if !r.promoted
                && master.looks_sane(&t)
                && r.no_down_for(wait)
                && now - r.role_reported_time > wait
                && send_slaveof(
                    state,
                    m,
                    who,
                    Some((state.masters[m].me.host.clone(), state.masters[m].me.port)),
                )
            {
                state.event(false, "+convert-to-slave", m, who, "");
            }
        }
    }
    // A replica of another master than its own.
    if let Who::Replica(i) = who
        && role == Kind::Replica
    {
        let master = &state.masters[m];
        let r = &master.replicas[i];
        let elsewhere = r.slave_master_port != master.me.port
            || r.slave_master_host
                .as_deref()
                .is_none_or(|h| !h.eq_ignore_ascii_case(&master.me.host));
        let wait = master.failover_timeout;
        if elsewhere
            && master.looks_sane(&t)
            && r.no_down_for(wait)
            && now - r.slave_conf_change_time > wait
            && send_slaveof(
                state,
                m,
                who,
                Some((state.masters[m].me.host.clone(), state.masters[m].me.port)),
            )
        {
            state.event(false, "+fix-slave-config", m, who, "");
        }
    }
    // A replica being pointed at the promoted one.
    if let Who::Replica(i) = who
        && role == Kind::Replica
    {
        let master = &state.masters[m];
        let r = &master.replicas[i];
        if (r.reconf_sent || r.reconf_inprog)
            && let Some(p) = master.promoted
        {
            let promoted = &master.replicas[p];
            let to_promoted = r
                .slave_master_host
                .as_deref()
                .is_some_and(|h| h.eq_ignore_ascii_case(&promoted.host))
                && r.slave_master_port == promoted.port;
            let link_up = r.slave_master_link_up;
            if r.reconf_sent && to_promoted {
                let r = &mut state.masters[m].replicas[i];
                r.reconf_sent = false;
                r.reconf_inprog = true;
                state.event(false, "+slave-reconf-inprog", m, who, "");
            }
            let r = &mut state.masters[m].replicas[i];
            if r.reconf_inprog && link_up {
                r.reconf_inprog = false;
                r.reconf_done = true;
                state.event(false, "+slave-reconf-done", m, who, "");
            }
        }
    }
}

/// `sentinelSendSlaveOf`: `SLAVEOF host port` (or `NO ONE`), in a
/// transaction with `CONFIG REWRITE` and disconnecting its clients, whose
/// replies aren't looked at: the next `INFO` shows how it went.
fn send_slaveof(state: &mut State, m: usize, who: Who, to: Option<(String, u16)>) -> bool {
    let master = &state.masters[m];
    let (host, port) = match to {
        Some((h, p)) => (h.into_bytes(), p.to_string().into_bytes()),
        None => (b"NO".to_vec(), b"ONE".to_vec()),
    };
    let commands = vec![
        vec![master.command("MULTI")],
        vec![master.command("SLAVEOF"), host, port],
        vec![master.command("CONFIG"), b"REWRITE".to_vec()],
        vec![
            master.command("CLIENT"),
            b"KILL".to_vec(),
            b"TYPE".to_vec(),
            b"normal".to_vec(),
        ],
        vec![
            master.command("CLIENT"),
            b"KILL".to_vec(),
            b"TYPE".to_vec(),
            b"pubsub".to_vec(),
        ],
        vec![master.command("EXEC")],
    ];
    let inst = state.instance(m, who);
    if inst.disconnected() {
        return false;
    }
    commands.into_iter().all(|c| inst.send(c, Tag::Discard))
}

/// `sentinelCheckSubjectivelyDown`.
fn check_subjectively_down(state: &mut State, m: usize, who: Who) {
    let now = mstime();
    let t = state.timings.clone();
    let down_after = state.masters[m].down_after;
    let reboot_after = state.masters[m].master_reboot_down_after;
    let inst = state.instance(m, who);
    let elapsed = if inst.act_ping_time != 0 {
        now - inst.act_ping_time
    } else if inst.disconnected() {
        now - inst.last_avail_time
    } else {
        0
    };
    // A link that seems up but doesn't answer is reconnected.
    if inst.cmd_up
        && now - inst.cc_conn_time > t.min_link_reconnect_period
        && inst.act_ping_time != 0
        && now - inst.act_ping_time > down_after / 2
        && now - inst.last_pong_time > down_after / 2
    {
        inst.cmd = None;
        inst.cmd_up = false;
        inst.pending = 0;
        inst.generation += 1;
        // (The pub/sub link goes too: it's the same generation.)
        inst.pubsub = None;
        inst.pubsub_up = false;
    }
    if inst.pubsub_up
        && now - inst.pc_conn_time > t.min_link_reconnect_period
        && now - inst.pc_last_activity > t.publish_period * 3
    {
        inst.close_links();
    }
    let down = elapsed > down_after
        || (inst.kind == Kind::Master
            && inst.role_reported == Kind::Replica
            && now - inst.role_reported_time > down_after + t.info_period * 2)
        || (inst.master_reboot && now - inst.master_reboot_since > reboot_after);
    if down {
        if !inst.s_down {
            inst.s_down = true;
            inst.s_down_since = now;
            state.event(true, "+sdown", m, who, "");
        }
    } else if inst.s_down {
        inst.s_down = false;
        inst.script_kill_sent = false;
        state.event(true, "-sdown", m, who, "");
    }
}

/// `sentinelCheckObjectivelyDown`.
fn check_objectively_down(state: &mut State, m: usize) {
    let master = &state.masters[m];
    let mut quorum = 0;
    let mut odown = false;
    if master.me.s_down {
        quorum = 1 + master.sentinels.iter().filter(|s| s.master_down).count() as u32;
        odown = quorum >= master.quorum;
    }
    let q = master.quorum;
    if odown {
        if !master.me.o_down {
            state.event(
                true,
                "+odown",
                m,
                Who::Master,
                &format!("#quorum {quorum}/{q}"),
            );
            let master = &mut state.masters[m];
            master.me.o_down = true;
            master.me.o_down_since = mstime();
        }
    } else if master.me.o_down {
        state.event(true, "-odown", m, Who::Master, "");
        state.masters[m].me.o_down = false;
    }
}

/// `sentinelAskMasterStateToOtherSentinels`.
fn ask_other_sentinels(state: &mut State, m: usize, forced: bool) {
    let now = mstime();
    let ask_period = state.timings.ask_period;
    let my_vote = if state.masters[m].failover_state > FailoverState::None {
        state.myid.clone()
    } else {
        "*".to_string()
    };
    let epoch = state.current_epoch.to_string();
    let master = &mut state.masters[m];
    let command = master.command("SENTINEL");
    let (host, port, s_down) = (master.me.host.clone(), master.me.port, master.me.s_down);
    for s in &mut master.sentinels {
        if now - s.last_master_down_reply_time > ask_period * 5 {
            s.master_down = false;
            s.leader = None;
        }
        if !s_down || s.disconnected() {
            continue;
        }
        if !forced && now - s.last_master_down_reply_time < ask_period {
            continue;
        }
        s.send(
            vec![
                command.clone(),
                b"is-master-down-by-addr".to_vec(),
                host.clone().into_bytes(),
                port.to_string().into_bytes(),
                epoch.clone().into_bytes(),
                my_vote.clone().into_bytes(),
            ],
            Tag::IsMasterDown,
        );
    }
}

/// `sentinelVoteLeader`: votes for `runid` in `epoch`, unless it has
/// already voted then; returns the vote and its epoch.
pub(crate) fn vote_leader(
    state: &mut State,
    m: usize,
    epoch: u64,
    runid: &str,
) -> (Option<String>, u64) {
    if epoch > state.current_epoch {
        state.current_epoch = epoch;
        let _ = state.flush_config();
        state.event(true, "+new-epoch", m, Who::None, &epoch.to_string());
    }
    let master = &mut state.masters[m];
    if master.leader_epoch < epoch && state.current_epoch <= epoch {
        master.leader = Some(runid.to_string());
        master.leader_epoch = state.current_epoch;
        let leader_epoch = master.leader_epoch;
        let _ = state.flush_config();
        state.event(
            true,
            "+vote-for-leader",
            m,
            Who::Master,
            &format!("{runid} {leader_epoch}"),
        );
        if !runid.eq_ignore_ascii_case(&state.myid) {
            state.masters[m].failover_start_time = mstime() + random_below(1000) as i64;
        }
    }
    let master = &state.masters[m];
    (master.leader.clone(), master.leader_epoch)
}

/// `sentinelGetLeader`: the Sentinel with a majority of votes (and at least
/// `quorum`) in `epoch`, if there's one, after this one votes.
fn get_leader(state: &mut State, m: usize, epoch: u64) -> Option<String> {
    let master = &state.masters[m];
    let voters = master.sentinels.len() + 1;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for s in &master.sentinels {
        if let Some(leader) = &s.leader
            && s.leader_epoch == state.current_epoch
        {
            *counts.entry(leader.clone()).or_default() += 1;
        }
    }
    let mut winner = None;
    let mut max = 0;
    for (runid, &votes) in &counts {
        if votes > max {
            max = votes;
            winner = Some(runid.clone());
        }
    }
    let candidate = winner.clone().unwrap_or_else(|| state.myid.clone());
    let (my_vote, leader_epoch) = vote_leader(state, m, epoch, &candidate);
    if let Some(vote) = my_vote
        && leader_epoch == epoch
    {
        let votes = counts.get(&vote).copied().unwrap_or(0) + 1;
        if votes > max {
            max = votes;
            winner = Some(vote);
        }
    }
    let quorum = state.masters[m].quorum as usize;
    if winner.is_some() && (max < voters / 2 + 1 || max < quorum) {
        winner = None;
    }
    winner
}

/// `sentinelStartFailover`.
pub(crate) fn start_failover(state: &mut State, m: usize) {
    state.current_epoch += 1;
    let epoch = state.current_epoch;
    let master = &mut state.masters[m];
    master.failover_state = FailoverState::WaitStart;
    master.failover_in_progress = true;
    master.failover_epoch = epoch;
    state.event(true, "+new-epoch", m, Who::None, &epoch.to_string());
    state.event(true, "+try-failover", m, Who::Master, "");
    let master = &mut state.masters[m];
    master.failover_start_time = mstime() + random_below(1000) as i64;
    master.failover_state_change_time = mstime();
}

/// `sentinelStartFailoverIfNeeded`.
fn start_failover_if_needed(state: &mut State, m: usize) -> bool {
    let master = &mut state.masters[m];
    if !master.me.o_down || master.failover_in_progress {
        return false;
    }
    if mstime() - master.failover_start_time < master.failover_timeout * 2 {
        if master.failover_delay_logged != master.failover_start_time {
            master.failover_delay_logged = master.failover_start_time;
            log(&format!(
                "* Next failover delay: I will not start a failover before {} (Unix ms)",
                master.failover_start_time + master.failover_timeout * 2
            ));
        }
        return false;
    }
    start_failover(state, m);
    true
}

/// `sentinelSelectSlave`: the best replica to promote, if one will do.
pub(crate) fn select_replica(state: &State, m: usize) -> Option<usize> {
    let now = mstime();
    let t = &state.timings;
    let master = &state.masters[m];
    let mut max_down = master.down_after * 10;
    if master.me.s_down {
        max_down += now - master.me.s_down_since;
    }
    let validity = if master.me.s_down {
        t.ping_period * 5
    } else {
        t.info_period * 3
    };
    let mut candidates: Vec<usize> = (0..master.replicas.len())
        .filter(|&i| {
            let r = &master.replicas[i];
            !r.s_down
                && !r.o_down
                && !r.disconnected()
                && now - r.last_avail_time <= t.ping_period * 5
                && r.slave_priority != 0
                && now - r.info_refresh <= validity
                && r.master_link_down_time <= max_down
        })
        .collect();
    candidates.sort_by(|&a, &b| {
        let (a, b) = (&master.replicas[a], &master.replicas[b]);
        a.slave_priority
            .cmp(&b.slave_priority)
            .then(b.slave_repl_offset.cmp(&a.slave_repl_offset))
            .then_with(|| match (&a.runid, &b.runid) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(x), Some(y)) => x.to_ascii_lowercase().cmp(&y.to_ascii_lowercase()),
            })
    });
    candidates.first().copied()
}

/// `sentinelFailoverStateMachine`.
fn failover_state_machine(state: &mut State, m: usize) {
    if !state.masters[m].failover_in_progress {
        return;
    }
    let now = mstime();
    match state.masters[m].failover_state {
        FailoverState::WaitStart => {
            let epoch = state.masters[m].failover_epoch;
            let leader = get_leader(state, m, epoch);
            let is_leader = leader
                .as_deref()
                .is_some_and(|l| l.eq_ignore_ascii_case(&state.myid));
            if !is_leader && !state.masters[m].force_failover {
                let master = &state.masters[m];
                let election_timeout = state.timings.election_timeout.min(master.failover_timeout);
                if now - master.failover_start_time > election_timeout {
                    state.event(true, "-failover-abort-not-elected", m, Who::Master, "");
                    abort_failover(state, m);
                }
                return;
            }
            state.event(true, "+elected-leader", m, Who::Master, "");
            if state.simfailure_crash_after_election {
                log("# Sentinel CRASH because of SENTINEL simulate-failure");
                std::process::exit(99);
            }
            let master = &mut state.masters[m];
            master.failover_state = FailoverState::SelectSlave;
            master.failover_state_change_time = now;
            state.event(true, "+failover-state-select-slave", m, Who::Master, "");
        }
        FailoverState::SelectSlave => match select_replica(state, m) {
            None => {
                state.event(true, "-failover-abort-no-good-slave", m, Who::Master, "");
                abort_failover(state, m);
            }
            Some(i) => {
                state.event(true, "+selected-slave", m, Who::Replica(i), "");
                let master = &mut state.masters[m];
                master.replicas[i].promoted = true;
                master.promoted = Some(i);
                master.failover_state = FailoverState::SendSlaveofNoone;
                master.failover_state_change_time = now;
                state.event(
                    false,
                    "+failover-state-send-slaveof-noone",
                    m,
                    Who::Replica(i),
                    "",
                );
            }
        },
        FailoverState::SendSlaveofNoone => {
            let Some(p) = state.masters[m].promoted else {
                return;
            };
            if state.masters[m].replicas[p].disconnected() {
                let master = &state.masters[m];
                if now - master.failover_state_change_time > master.failover_timeout {
                    state.event(true, "-failover-abort-slave-timeout", m, Who::Master, "");
                    abort_failover(state, m);
                }
                return;
            }
            if send_slaveof(state, m, Who::Replica(p), None) {
                state.event(
                    false,
                    "+failover-state-wait-promotion",
                    m,
                    Who::Replica(p),
                    "",
                );
                let master = &mut state.masters[m];
                master.failover_state = FailoverState::WaitPromotion;
                master.failover_state_change_time = now;
            }
        }
        FailoverState::WaitPromotion => {
            let master = &state.masters[m];
            if now - master.failover_state_change_time > master.failover_timeout {
                state.event(true, "-failover-abort-slave-timeout", m, Who::Master, "");
                abort_failover(state, m);
            }
        }
        FailoverState::ReconfSlaves => reconf_next_replica(state, m),
        FailoverState::None | FailoverState::UpdateConfig => {}
    }
}

/// `sentinelFailoverReconfNextSlave`.
fn reconf_next_replica(state: &mut State, m: usize) {
    let now = mstime();
    let reconf_timeout = state.timings.slave_reconf_timeout;
    let Some(p) = state.masters[m].promoted else {
        return;
    };
    let to = {
        let promoted = &state.masters[m].replicas[p];
        (promoted.host.clone(), promoted.port)
    };
    let mut in_progress = state.masters[m]
        .replicas
        .iter()
        .filter(|r| r.reconf_sent || r.reconf_inprog)
        .count() as u32;
    for i in 0..state.masters[m].replicas.len() {
        if in_progress >= state.masters[m].parallel_syncs {
            break;
        }
        let r = &mut state.masters[m].replicas[i];
        if r.promoted || r.reconf_done {
            continue;
        }
        if r.reconf_sent && now - r.slave_reconf_sent_time > reconf_timeout {
            r.reconf_sent = false;
            r.reconf_done = true;
            state.event(false, "-slave-reconf-sent-timeout", m, Who::Replica(i), "");
        }
        let r = &state.masters[m].replicas[i];
        if r.reconf_sent || r.reconf_inprog || r.disconnected() {
            continue;
        }
        if send_slaveof(state, m, Who::Replica(i), Some(to.clone())) {
            let r = &mut state.masters[m].replicas[i];
            r.reconf_sent = true;
            r.slave_reconf_sent_time = now;
            state.event(false, "+slave-reconf-sent", m, Who::Replica(i), "");
            in_progress += 1;
        }
    }
    detect_failover_end(state, m);
}

/// `sentinelFailoverDetectEnd`.
fn detect_failover_end(state: &mut State, m: usize) {
    let now = mstime();
    let master = &state.masters[m];
    let Some(p) = master.promoted else { return };
    if master.replicas[p].s_down {
        return;
    }
    let mut not_reconfigured = master
        .replicas
        .iter()
        .filter(|r| !(r.promoted || r.reconf_done) && !r.s_down)
        .count();
    let mut timeout = false;
    if now - master.failover_state_change_time > master.failover_timeout {
        not_reconfigured = 0;
        timeout = true;
        state.event(true, "+failover-end-for-timeout", m, Who::Master, "");
    }
    if not_reconfigured == 0 {
        state.event(true, "+failover-end", m, Who::Master, "");
        let master = &mut state.masters[m];
        master.failover_state = FailoverState::UpdateConfig;
        master.failover_state_change_time = now;
    }
    if timeout {
        let to = {
            let promoted = &state.masters[m].replicas[p];
            (promoted.host.clone(), promoted.port)
        };
        for i in 0..state.masters[m].replicas.len() {
            let r = &state.masters[m].replicas[i];
            if r.promoted || r.reconf_done || r.reconf_sent || r.disconnected() {
                continue;
            }
            if send_slaveof(state, m, Who::Replica(i), Some(to.clone())) {
                state.event(false, "+slave-reconf-sent-be", m, Who::Replica(i), "");
                state.masters[m].replicas[i].reconf_sent = true;
            }
        }
    }
}

/// `sentinelAbortFailover`.
fn abort_failover(state: &mut State, m: usize) {
    let master = &mut state.masters[m];
    master.failover_in_progress = false;
    master.force_failover = false;
    master.failover_state = FailoverState::None;
    master.failover_state_change_time = mstime();
    if let Some(p) = master.promoted.take() {
        master.replicas[p].promoted = false;
    }
}

/// `sentinelFailoverSwitchToPromotedSlave`.
fn switch_to_promoted(state: &mut State, m: usize) {
    let master = &state.masters[m];
    let (to_host, to_port) = match master.promoted {
        Some(p) => (master.replicas[p].host.clone(), master.replicas[p].port),
        None => (master.me.host.clone(), master.me.port),
    };
    let rest = format!(
        "{} {} {} {to_host} {to_port}",
        master.name, master.me.host, master.me.port
    );
    state.event(true, "+switch-master", m, Who::None, &rest);
    reset_and_change_address(state, m, to_host, to_port);
}

/// `sentinelResetMaster`: forget its replicas (and its Sentinels, unless
/// `keep_sentinels`), links and failover.
pub(crate) fn reset_master(state: &mut State, m: usize, keep_sentinels: bool, event: bool) {
    let now = mstime();
    let master = &mut state.masters[m];
    master.replicas.clear();
    if !keep_sentinels {
        master.sentinels.clear();
    }
    master.me.close_links();
    master.me.s_down = false;
    master.me.o_down = false;
    master.me.master_down = false;
    master.me.master_reboot = false;
    master.me.script_kill_sent = false;
    master.leader = None;
    master.failover_state = FailoverState::None;
    master.failover_state_change_time = 0;
    master.failover_start_time = 0;
    master.failover_in_progress = false;
    master.force_failover = false;
    master.promoted = None;
    master.me.runid = None;
    master.me.slave_master_host = None;
    master.me.act_ping_time = now;
    master.me.last_ping_time = 0;
    master.me.last_avail_time = now;
    master.me.last_pong_time = now;
    master.me.role_reported_time = now;
    master.me.role_reported = Kind::Master;
    master.me.last_reconnect = 0;
    if event {
        state.event(true, "+reset-master", m, Who::Master, "");
    }
}

/// `sentinelResetMasterAndChangeAddress`: the master is at `host:port` now;
/// its replicas stay replicas (but the one there), and the old master
/// becomes one.
fn reset_and_change_address(state: &mut State, m: usize, host: String, port: u16) {
    let master = &state.masters[m];
    let mut replicas: Vec<(String, u16)> = master
        .replicas
        .iter()
        .filter(|r| !(r.host == host && r.port == port))
        .map(|r| (r.host.clone(), r.port))
        .collect();
    if !(master.me.host == host && master.me.port == port) {
        replicas.push((master.me.host.clone(), master.me.port));
    }
    reset_master(state, m, true, false);
    let master = &mut state.masters[m];
    master.me.host = host;
    master.me.port = port;
    master.me.o_down_since = 0;
    master.me.s_down_since = 0;
    for (h, p) in replicas {
        let id = state.id();
        state.masters[m]
            .replicas
            .push(Instance::new(id, Kind::Replica, h, p));
        let i = state.masters[m].replicas.len() - 1;
        state.event(false, "+slave", m, Who::Replica(i), "");
    }
    let _ = state.flush_config();
}
