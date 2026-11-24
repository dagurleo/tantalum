//! What a node knows of the cluster, and what it does with what it hears:
//! `cluster.c`'s state machine, step for step (gossip, failure reports and
//! `FAIL`, slot ownership by config epoch, replicas' elections, manual
//! failover), apart from the sockets (`bus.rs`) and the commands
//! (`command.rs`). What needs the server (following a master, deleting a
//! slot's keys, pausing writes) is queued as [`Action`]s, done once the
//! state's lock is let go.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use tokio::sync::mpsc;

use super::wire::{
    self, Body, Ext, Gossip, Header, Msg, NAMELEN, NULL_NAME, Name, SLOT_BYTES, SLOTS, Slots,
};

pub(crate) const MASTER: u16 = 1;
pub(crate) const SLAVE: u16 = 2;
pub(crate) const PFAIL: u16 = 4;
pub(crate) const FAIL: u16 = 8;
pub(crate) const MYSELF: u16 = 16;
pub(crate) const HANDSHAKE: u16 = 32;
pub(crate) const NOADDR: u16 = 64;
pub(crate) const MEET: u16 = 128;
pub(crate) const MIGRATE_TO: u16 = 256;
pub(crate) const NOFAILOVER: u16 = 512;

pub(crate) const OK: u8 = 0;
pub(crate) const DOWN: u8 = 1;

const FAIL_REPORT_VALIDITY_MULT: i64 = 2;
const FAIL_UNDO_TIME_MULT: i64 = 2;
pub(crate) const MF_TIMEOUT: i64 = 5000;
pub(crate) const MF_PAUSE_MULT: i64 = 2;
const BLACKLIST_TTL: i64 = 60;
const MAX_REJOIN_DELAY: i64 = 5000;
const MIN_REJOIN_DELAY: i64 = 500;
const WRITABLE_DELAY: i64 = 2000;

pub(crate) const TODO_HANDLE_FAILOVER: u32 = 1;
pub(crate) const TODO_UPDATE_STATE: u32 = 2;
pub(crate) const TODO_SAVE_CONFIG: u32 = 4;
pub(crate) const TODO_FSYNC_CONFIG: u32 = 8;
pub(crate) const TODO_HANDLE_MANUALFAILOVER: u32 = 16;

pub(crate) fn mstime() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn random_u64() -> u64 {
    use std::hash::{BuildHasher, RandomState};
    RandomState::new().hash_one(mstime())
}

pub(crate) fn random_name() -> Name {
    let mut name = [0u8; NAMELEN];
    let hex = b"0123456789abcdef";
    let mut bits = 0u64;
    for (i, c) in name.iter_mut().enumerate() {
        if i % 16 == 0 {
            bits = random_u64() ^ (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        }
        *c = hex[(bits & 15) as usize];
        bits >>= 4;
    }
    name
}

pub(crate) fn name_str(name: &Name) -> &str {
    std::str::from_utf8(name).unwrap_or("")
}

/// `verifyClusterNodeId`.
pub(crate) fn valid_name(name: &[u8]) -> Option<Name> {
    if name.len() != NAMELEN
        || !name
            .iter()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
    {
        return None;
    }
    name.try_into().ok()
}

/// A link's handle: where to send it messages.
pub(crate) struct Link {
    pub(crate) node: Option<Name>,
    pub(crate) inbound: bool,
    pub(crate) ctime: i64,
    pub(crate) tx: mpsc::UnboundedSender<Vec<u8>>,
    /// What's queued for it, until its socket's there.
    pub(crate) rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    /// Its task, once it has one.
    pub(crate) abort: Option<tokio::task::AbortHandle>,
    /// The addresses of its socket, for what a node learns from them.
    pub(crate) peer_ip: String,
    pub(crate) local_ip: String,
    pub(crate) send_bytes: u64,
}

pub(crate) struct Node {
    pub(crate) name: Name,
    pub(crate) shard_id: Name,
    pub(crate) ctime: i64,
    pub(crate) flags: u16,
    pub(crate) config_epoch: u64,
    pub(crate) slots: Box<Slots>,
    pub(crate) numslots: usize,
    pub(crate) replicas: Vec<Name>,
    pub(crate) slaveof: Option<Name>,
    last_in_ping_gossip: u64,
    pub(crate) ping_sent: i64,
    pub(crate) pong_received: i64,
    pub(crate) data_received: i64,
    pub(crate) fail_time: i64,
    pub(crate) voted_time: i64,
    pub(crate) repl_offset_time: i64,
    pub(crate) repl_offset: u64,
    pub(crate) ip: String,
    pub(crate) hostname: String,
    pub(crate) human_nodename: String,
    pub(crate) tcp_port: u16,
    pub(crate) tls_port: u16,
    pub(crate) cport: u16,
    pub(crate) link: Option<u64>,
    pub(crate) inbound_link: Option<u64>,
    fail_reports: Vec<(Name, i64)>,
}

impl Node {
    pub(crate) fn new(name: Option<Name>, flags: u16) -> Node {
        Node {
            name: name.unwrap_or_else(random_name),
            shard_id: random_name(),
            ctime: mstime(),
            flags,
            config_epoch: 0,
            slots: Box::new([0; SLOT_BYTES]),
            numslots: 0,
            replicas: Vec::new(),
            slaveof: None,
            last_in_ping_gossip: 0,
            ping_sent: 0,
            pong_received: 0,
            data_received: 0,
            fail_time: 0,
            voted_time: 0,
            repl_offset_time: 0,
            repl_offset: 0,
            ip: String::new(),
            hostname: String::new(),
            human_nodename: String::new(),
            tcp_port: 0,
            tls_port: 0,
            cport: 0,
            link: None,
            inbound_link: None,
            fail_reports: Vec::new(),
        }
    }
    pub(crate) fn is_master(&self) -> bool {
        self.flags & MASTER != 0
    }
    pub(crate) fn is_slave(&self) -> bool {
        self.flags & SLAVE != 0
    }
    pub(crate) fn in_handshake(&self) -> bool {
        self.flags & HANDSHAKE != 0
    }
    pub(crate) fn timed_out(&self) -> bool {
        self.flags & PFAIL != 0
    }
    pub(crate) fn failed(&self) -> bool {
        self.flags & FAIL != 0
    }
    fn has_addr(&self) -> bool {
        self.flags & NOADDR == 0
    }
}

/// What the server does for the cluster, once the state's lock is let go.
pub(crate) enum Action {
    /// Replicate this master (`replicationSetMaster`).
    Follow(String, u16),
    /// Stop replicating, keeping the data (`replicationUnsetMaster`).
    Promote,
    /// Stop replicating and empty the data (`CLUSTER RESET` of a replica).
    PromoteAndEmpty,
    /// Hold writes back until then (Unix ms), or let them go (0).
    Pause(i64),
    /// Delete the keys in a slot this node no longer owns.
    DeleteSlot(u16),
    /// A message for this node's own subscribers.
    Publish {
        channel: Vec<u8>,
        message: Vec<u8>,
        shard: bool,
    },
}

/// Settings (`cluster-*` directives).
#[derive(Clone)]
pub(crate) struct Settings {
    pub(crate) node_timeout: i64,
    pub(crate) port: u16,
    pub(crate) cport: u16,
    pub(crate) announce_ip: Option<String>,
    pub(crate) announce_hostname: String,
    pub(crate) require_full_coverage: bool,
    pub(crate) allow_reads_when_down: bool,
    pub(crate) allow_pubsubshard_when_down: bool,
    pub(crate) replica_validity_factor: i64,
    pub(crate) replica_no_failover: bool,
    pub(crate) allow_replica_migration: bool,
    pub(crate) repl_ping_period: i64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            node_timeout: 15_000,
            port: 6379,
            cport: 16379,
            announce_ip: None,
            announce_hostname: String::new(),
            require_full_coverage: true,
            allow_reads_when_down: false,
            allow_pubsubshard_when_down: true,
            replica_validity_factor: 10,
            replica_no_failover: false,
            allow_replica_migration: true,
            repl_ping_period: 10,
        }
    }
}

/// What the server says of its replication, as of now (for the messages
/// and the elections).
#[derive(Clone, Copy, Default)]
pub(crate) struct Repl {
    pub(crate) offset: u64,
    /// For a replica: how long since it heard from its master (ms).
    pub(crate) data_age: i64,
    /// Whether it follows a master.
    pub(crate) following: bool,
}

pub(crate) struct State {
    pub(crate) myself: Name,
    pub(crate) current_epoch: u64,
    pub(crate) last_vote_epoch: u64,
    pub(crate) state: u8,
    pub(crate) size: usize,
    pub(crate) nodes: BTreeMap<Name, Node>,
    blacklist: HashMap<Name, i64>,
    pub(crate) slots: Vec<Option<Name>>,
    pub(crate) migrating: Vec<Option<Name>>,
    pub(crate) importing: Vec<Option<Name>>,
    owner_not_claiming: Box<Slots>,
    failover_auth_time: i64,
    failover_auth_count: usize,
    failover_auth_sent: bool,
    failover_auth_rank: usize,
    failover_auth_epoch: u64,
    pub(crate) mf_end: i64,
    mf_slave: Option<Name>,
    mf_master_offset: i64,
    mf_can_start: bool,
    pub(crate) todo: u32,
    pub(crate) stats_sent: [u64; wire::TYPES],
    pub(crate) stats_received: [u64; wire::TYPES],
    stats_pfail_nodes: usize,
    pub(crate) links: HashMap<u64, Link>,
    pub(crate) next_link: u64,
    pings_sent: u64,
    iteration: u64,
    first_call_time: i64,
    among_minority_time: i64,
    pub(crate) settings: Settings,
    pub(crate) repl: Repl,
    pub(crate) actions: Vec<Action>,
    pub(crate) config_path: std::path::PathBuf,
}

impl State {
    pub(crate) fn new(settings: Settings, config_path: std::path::PathBuf) -> State {
        let mut me = Node::new(None, MYSELF | MASTER);
        me.tcp_port = settings.port;
        me.cport = settings.cport;
        let myself = me.name;
        let mut nodes = BTreeMap::new();
        nodes.insert(myself, me);
        State {
            myself,
            current_epoch: 0,
            last_vote_epoch: 0,
            state: DOWN,
            size: 0,
            nodes,
            blacklist: HashMap::new(),
            slots: vec![None; SLOTS],
            migrating: vec![None; SLOTS],
            importing: vec![None; SLOTS],
            owner_not_claiming: Box::new([0; SLOT_BYTES]),
            failover_auth_time: 0,
            failover_auth_count: 0,
            failover_auth_sent: false,
            failover_auth_rank: 0,
            failover_auth_epoch: 0,
            mf_end: 0,
            mf_slave: None,
            mf_master_offset: -1,
            mf_can_start: false,
            todo: 0,
            stats_sent: [0; wire::TYPES],
            stats_received: [0; wire::TYPES],
            stats_pfail_nodes: 0,
            links: HashMap::new(),
            next_link: 0,
            pings_sent: 0,
            iteration: 0,
            first_call_time: 0,
            among_minority_time: 0,
            settings,
            repl: Repl::default(),
            actions: Vec::new(),
            config_path,
        }
    }

    pub(crate) fn me(&self) -> &Node {
        &self.nodes[&self.myself]
    }

    fn me_mut(&mut self) -> &mut Node {
        self.nodes.get_mut(&self.myself).expect("myself")
    }

    pub(crate) fn lookup(&self, name: &[u8]) -> Option<&Node> {
        valid_name(name).and_then(|n| self.nodes.get(&n))
    }

    /// My master if I'm a replica, else me.
    fn my_master(&self) -> Name {
        let me = self.me();
        if me.is_slave()
            && let Some(m) = me.slaveof
            && self.nodes.contains_key(&m)
        {
            return m;
        }
        self.myself
    }

    pub(crate) fn todo(&mut self, flags: u32) {
        self.todo |= flags;
    }

    // ---- nodes ---------------------------------------------------------

    fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.name, node);
    }

    /// `clusterDelNode`.
    pub(crate) fn del_node(&mut self, name: Name) {
        for j in 0..SLOTS {
            if self.importing[j] == Some(name) {
                self.importing[j] = None;
            }
            if self.migrating[j] == Some(name) {
                self.migrating[j] = None;
            }
            if self.slots[j] == Some(name) {
                self.del_slot(j);
            }
        }
        for node in self.nodes.values_mut() {
            node.fail_reports.retain(|(n, _)| *n != name);
        }
        let Some(node) = self.nodes.remove(&name) else {
            return;
        };
        for r in &node.replicas {
            if let Some(r) = self.nodes.get_mut(r) {
                r.slaveof = None;
            }
        }
        if node.is_slave()
            && let Some(m) = node.slaveof
        {
            self.remove_replica(m, name);
        }
        for link in [node.link, node.inbound_link].into_iter().flatten() {
            self.free_link(link);
        }
    }

    fn rename_node(&mut self, old: Name, new: Name) {
        if let Some(mut node) = self.nodes.remove(&old) {
            node.name = new;
            for link in [node.link, node.inbound_link].into_iter().flatten() {
                if let Some(l) = self.links.get_mut(&link) {
                    l.node = Some(new);
                }
            }
            self.nodes.insert(new, node);
        }
    }

    fn remove_replica(&mut self, master: Name, replica: Name) {
        if let Some(m) = self.nodes.get_mut(&master) {
            m.replicas.retain(|r| *r != replica);
            if m.replicas.is_empty() {
                m.flags &= !MIGRATE_TO;
            }
        }
    }

    fn add_replica(&mut self, master: Name, replica: Name) {
        if let Some(m) = self.nodes.get_mut(&master)
            && !m.replicas.contains(&replica)
        {
            m.replicas.push(replica);
            m.flags |= MIGRATE_TO;
        }
    }

    // ---- links ---------------------------------------------------------

    pub(crate) fn free_link(&mut self, id: u64) {
        if let Some(link) = self.links.remove(&id) {
            if let Some(task) = &link.abort {
                task.abort();
            }
            if let Some(name) = link.node
                && let Some(node) = self.nodes.get_mut(&name)
            {
                if node.link == Some(id) {
                    node.link = None;
                }
                if node.inbound_link == Some(id) {
                    node.inbound_link = None;
                }
            }
        }
    }

    /// `clusterSendMessage`.
    fn send(&mut self, link: Option<u64>, msg: &Msg) {
        let Some(id) = link else { return };
        let Some(l) = self.links.get_mut(&id) else {
            return;
        };
        let bytes = msg.encode();
        l.send_bytes += bytes.len() as u64;
        if l.tx.send(bytes).is_ok()
            && let Some(n) = self.stats_sent.get_mut(msg.header.kind as usize)
        {
            *n += 1;
        }
    }

    /// `clusterBroadcastMessage`: to every node with a link but me and
    /// those in handshake.
    fn broadcast(&mut self, msg: &Msg) {
        let targets: Vec<u64> = self
            .nodes
            .values()
            .filter(|n| n.flags & (MYSELF | HANDSHAKE) == 0)
            .filter_map(|n| n.link)
            .collect();
        for link in targets {
            self.send(Some(link), msg);
        }
    }

    // ---- failure reports ------------------------------------------------

    fn add_failure_report(&mut self, failing: Name, sender: Name) -> bool {
        let now = mstime();
        let Some(node) = self.nodes.get_mut(&failing) else {
            return false;
        };
        if let Some(r) = node.fail_reports.iter_mut().find(|(n, _)| *n == sender) {
            r.1 = now;
            return false;
        }
        node.fail_reports.push((sender, now));
        true
    }

    fn cleanup_failure_reports(&mut self, name: Name) {
        let max = self.settings.node_timeout * FAIL_REPORT_VALIDITY_MULT;
        let now = mstime();
        if let Some(node) = self.nodes.get_mut(&name) {
            node.fail_reports.retain(|(_, t)| now - t <= max);
        }
    }

    fn del_failure_report(&mut self, name: Name, sender: Name) -> bool {
        let Some(node) = self.nodes.get_mut(&name) else {
            return false;
        };
        let before = node.fail_reports.len();
        node.fail_reports.retain(|(n, _)| *n != sender);
        let removed = node.fail_reports.len() != before;
        if removed {
            self.cleanup_failure_reports(name);
        }
        removed
    }

    pub(crate) fn failure_reports(&mut self, name: Name) -> usize {
        self.cleanup_failure_reports(name);
        self.nodes.get(&name).map_or(0, |n| n.fail_reports.len())
    }

    /// `markNodeAsFailingIfNeeded`.
    fn mark_failing_if_needed(&mut self, name: Name) {
        let needed = self.size / 2 + 1;
        let Some(node) = self.nodes.get(&name) else {
            return;
        };
        if !node.timed_out() || node.failed() {
            return;
        }
        let mut failures = self.failure_reports(name);
        if self.me().is_master() {
            failures += 1;
        }
        if failures < needed {
            return;
        }
        log(&format!(
            "Marking node {} as failing (quorum reached).",
            name_str(&name)
        ));
        let node = self.nodes.get_mut(&name).expect("known");
        node.flags &= !PFAIL;
        node.flags |= FAIL;
        node.fail_time = mstime();
        let msg = self.message(wire::FAIL, Body::Fail(name));
        self.broadcast(&msg);
        self.todo(TODO_UPDATE_STATE | TODO_SAVE_CONFIG);
    }

    /// `clearNodeFailureIfNeeded`.
    fn clear_failure_if_needed(&mut self, name: Name) {
        let now = mstime();
        let timeout = self.settings.node_timeout;
        let Some(node) = self.nodes.get_mut(&name) else {
            return;
        };
        if node.is_slave() || node.numslots == 0 {
            node.flags &= !FAIL;
            self.todo(TODO_UPDATE_STATE | TODO_SAVE_CONFIG);
            return;
        }
        if node.is_master()
            && node.numslots > 0
            && now - node.fail_time > timeout * FAIL_UNDO_TIME_MULT
        {
            node.flags &= !FAIL;
            self.todo(TODO_UPDATE_STATE | TODO_SAVE_CONFIG);
        }
    }

    // ---- slots ---------------------------------------------------------

    pub(crate) fn add_slot(&mut self, name: Name, slot: usize) -> bool {
        if self.slots[slot].is_some() {
            return false;
        }
        let Some(node) = self.nodes.get_mut(&name) else {
            return false;
        };
        if !wire::slot_bit(&node.slots, slot) {
            wire::set_slot_bit(&mut node.slots, slot, true);
            node.numslots += 1;
            // A master that gets its first slot can have replicas migrate to it.
            if node.numslots == 1 && self.settings.allow_replica_migration {
                node.flags |= MIGRATE_TO;
            }
        }
        self.slots[slot] = Some(name);
        wire::set_slot_bit(&mut self.owner_not_claiming, slot, false);
        true
    }

    pub(crate) fn del_slot(&mut self, slot: usize) -> bool {
        let Some(owner) = self.slots[slot].take() else {
            return false;
        };
        if let Some(node) = self.nodes.get_mut(&owner)
            && wire::slot_bit(&node.slots, slot)
        {
            wire::set_slot_bit(&mut node.slots, slot, false);
            node.numslots -= 1;
        }
        wire::set_slot_bit(&mut self.owner_not_claiming, slot, false);
        true
    }

    pub(crate) fn del_node_slots(&mut self, name: Name) -> usize {
        let mut deleted = 0;
        for j in 0..SLOTS {
            if self
                .nodes
                .get(&name)
                .is_some_and(|n| wire::slot_bit(&n.slots, j))
            {
                self.del_slot(j);
                deleted += 1;
            }
        }
        deleted
    }

    pub(crate) fn close_all_slots(&mut self) {
        self.migrating.iter_mut().for_each(|s| *s = None);
        self.importing.iter_mut().for_each(|s| *s = None);
    }

    fn slot_unclaimed(&self, slot: usize) -> bool {
        self.slots[slot].is_none() || wire::slot_bit(&self.owner_not_claiming, slot)
    }

    fn config_epoch_of(&self, name: Option<Name>) -> u64 {
        name.and_then(|n| self.nodes.get(&n))
            .map_or(0, |n| n.config_epoch)
    }

    /// `clusterSetNodeAsMaster`.
    fn set_node_as_master(&mut self, name: Name) {
        let myself = self.myself;
        let Some(node) = self.nodes.get(&name) else {
            return;
        };
        if node.is_master() {
            return;
        }
        if let Some(m) = node.slaveof {
            self.remove_replica(m, name);
            if name != myself {
                self.nodes.get_mut(&name).expect("known").flags |= MIGRATE_TO;
            }
        }
        let node = self.nodes.get_mut(&name).expect("known");
        node.flags &= !SLAVE;
        node.flags |= MASTER;
        node.slaveof = None;
        self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
    }

    /// `clusterSetMaster`: this node replicates `master`.
    pub(crate) fn set_master(&mut self, master: Name) {
        let myself = self.myself;
        let me = self.me();
        if me.is_master() {
            let me = self.me_mut();
            me.flags &= !(MASTER | MIGRATE_TO);
            me.flags |= SLAVE;
            self.close_all_slots();
        } else if let Some(old) = me.slaveof {
            self.remove_replica(old, myself);
        }
        self.me_mut().slaveof = Some(master);
        let shard = self.nodes.get(&master).map(|m| m.shard_id);
        if let Some(shard) = shard {
            self.update_shard_id(myself, shard);
        }
        self.add_replica(master, myself);
        if let Some(m) = self.nodes.get(&master) {
            let (ip, port) = (m.ip.clone(), m.tcp_port);
            self.actions.push(Action::Follow(ip, port));
        }
        self.reset_manual_failover();
    }

    fn update_shard_id(&mut self, name: Name, shard: Name) {
        let myself = self.myself;
        if let Some(n) = self.nodes.get_mut(&name)
            && n.shard_id != shard
        {
            n.shard_id = shard;
            self.todo(TODO_SAVE_CONFIG);
        }
        if name != myself && self.me().slaveof == Some(name) && self.me().shard_id != shard {
            self.me_mut().shard_id = shard;
            self.todo(TODO_SAVE_CONFIG | TODO_FSYNC_CONFIG);
        }
    }

    /// `clusterUpdateSlotsConfigWith`.
    fn update_slots_config_with(&mut self, sender: Name, sender_epoch: u64, slots: &Slots) {
        let myself = self.myself;
        if sender == myself {
            return;
        }
        let curmaster = self.my_master();
        let mut newmaster = None;
        let mut dirty = Vec::new();
        let (mut sender_slots, mut migrated_ours) = (0, 0);
        for j in 0..SLOTS {
            if wire::slot_bit(slots, j) {
                sender_slots += 1;
                if self.slots[j] == Some(sender) {
                    wire::set_slot_bit(&mut self.owner_not_claiming, j, false);
                    continue;
                }
                if self.importing[j].is_some() {
                    continue;
                }
                if self.slot_unclaimed(j) || self.config_epoch_of(self.slots[j]) < sender_epoch {
                    if self.slots[j] == Some(myself) && sender != myself {
                        dirty.push(j as u16);
                    }
                    if self.slots[j] == Some(curmaster) {
                        newmaster = Some(sender);
                        migrated_ours += 1;
                    }
                    self.del_slot(j);
                    self.add_slot(sender, j);
                    self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE | TODO_FSYNC_CONFIG);
                }
            } else if self.slots[j] == Some(sender) {
                wire::set_slot_bit(&mut self.owner_not_claiming, j, true);
            }
        }
        let cur_numslots = self.nodes.get(&curmaster).map_or(0, |n| n.numslots);
        let my_master = self.me().slaveof;
        let grand = my_master
            .and_then(|m| self.nodes.get(&m))
            .and_then(|m| m.slaveof);
        if let Some(newmaster) = newmaster
            && cur_numslots == 0
            && (self.settings.allow_replica_migration || sender_slots == migrated_ours)
        {
            log(&format!(
                "Configuration change detected. Reconfiguring myself as a replica of {}",
                name_str(&sender)
            ));
            self.set_master(newmaster);
            self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE | TODO_FSYNC_CONFIG);
        } else if let Some(grand) = grand
            && grand != myself
        {
            log(&format!(
                "I'm a sub-replica! Reconfiguring myself as a replica of grandmaster {}",
                name_str(&grand)
            ));
            self.set_master(grand);
            self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE | TODO_FSYNC_CONFIG);
        } else {
            // (Slots this node lost with keys in them: their keys go.)
            for slot in dirty {
                self.actions.push(Action::DeleteSlot(slot));
            }
        }
    }

    // ---- epochs --------------------------------------------------------

    fn max_epoch(&self) -> u64 {
        self.nodes
            .values()
            .map(|n| n.config_epoch)
            .max()
            .unwrap_or(0)
            .max(self.current_epoch)
    }

    /// `clusterBumpConfigEpochWithoutConsensus`.
    pub(crate) fn bump_epoch(&mut self) -> bool {
        let max = self.max_epoch();
        let mine = self.me().config_epoch;
        if mine == 0 || mine != max {
            self.current_epoch += 1;
            let epoch = self.current_epoch;
            self.me_mut().config_epoch = epoch;
            self.todo(TODO_SAVE_CONFIG | TODO_FSYNC_CONFIG);
            log(&format!("New configEpoch set to {epoch}"));
            true
        } else {
            false
        }
    }

    /// `clusterHandleConfigEpochCollision`.
    fn handle_epoch_collision(&mut self, sender: Name) {
        let me = self.me();
        let Some(s) = self.nodes.get(&sender) else {
            return;
        };
        if s.config_epoch != me.config_epoch || !s.is_master() || !me.is_master() {
            return;
        }
        if sender <= self.myself {
            return;
        }
        self.current_epoch += 1;
        let epoch = self.current_epoch;
        self.me_mut().config_epoch = epoch;
        let _ = self.save_config();
        log(&format!(
            "WARNING: configEpoch collision with node {}. configEpoch set to {epoch}",
            name_str(&sender)
        ));
    }

    // ---- blacklist -----------------------------------------------------

    fn blacklist_cleanup(&mut self) {
        let now = mstime() / 1000;
        self.blacklist.retain(|_, expire| *expire >= now);
    }

    pub(crate) fn blacklist_add(&mut self, name: Name) {
        self.blacklist_cleanup();
        self.blacklist.insert(name, mstime() / 1000 + BLACKLIST_TTL);
    }

    pub(crate) fn blacklisted(&mut self, name: &[u8]) -> bool {
        self.blacklist_cleanup();
        valid_name(name).is_some_and(|n| self.blacklist.contains_key(&n))
            || name
                .try_into()
                .is_ok_and(|n: Name| self.blacklist.contains_key(&n))
    }

    // ---- handshakes ----------------------------------------------------

    fn handshake_in_progress(&self, ip: &str, port: u16, cport: u16) -> bool {
        self.nodes.values().any(|n| {
            n.in_handshake()
                && n.ip.eq_ignore_ascii_case(ip)
                && n.tcp_port == port
                && n.cport == cport
        })
    }

    /// `clusterStartHandshake`: `Err(true)` for an address that won't do,
    /// `Err(false)` for one already being met.
    pub(crate) fn start_handshake(&mut self, ip: &str, port: i64, cport: i64) -> Result<(), bool> {
        let Ok(addr) = ip.parse::<std::net::IpAddr>() else {
            return Err(true);
        };
        if !(1..=65535).contains(&port) || !(1..=65535).contains(&cport) {
            return Err(true);
        }
        let ip = addr.to_string();
        if self.handshake_in_progress(&ip, port as u16, cport as u16) {
            return Err(false);
        }
        let mut n = Node::new(None, HANDSHAKE | MEET);
        n.ip = ip;
        n.tcp_port = port as u16;
        n.cport = cport as u16;
        self.add_node(n);
        Ok(())
    }

    // ---- messages ------------------------------------------------------

    /// `clusterBuildMessageHdr`, with `body`.
    pub(crate) fn message(&self, kind: u16, body: Body) -> Msg {
        let me = self.me();
        let master = self.nodes.get(&self.my_master()).unwrap_or(me);
        let mut mflags = [0u8; 3];
        if me.is_master() && self.mf_end != 0 {
            mflags[0] |= wire::FLAG0_PAUSED;
        }
        Msg {
            header: Header {
                kind,
                count: 0,
                port: self.settings.port,
                current_epoch: self.current_epoch,
                config_epoch: master.config_epoch,
                offset: self.repl.offset,
                sender: self.myself,
                myslots: master.slots.clone(),
                slaveof: me.slaveof.unwrap_or(NULL_NAME),
                myip: self.settings.announce_ip.clone().unwrap_or_default(),
                extensions: 0,
                pport: 0,
                cport: self.settings.cport,
                flags: me.flags,
                state: self.state,
                mflags,
            },
            body,
        }
    }

    fn gossip_entry(n: &Node) -> Gossip {
        Gossip {
            name: n.name,
            ping_sent: (n.ping_sent / 1000) as u32,
            pong_received: (n.pong_received / 1000) as u32,
            ip: n.ip.clone(),
            port: n.tcp_port,
            cport: n.cport,
            flags: n.flags,
            pport: n.tls_port,
        }
    }

    /// `writePingExt`.
    fn ping_exts(&mut self) -> Vec<Ext> {
        let mut exts = Vec::new();
        let me = self.me();
        if !me.hostname.is_empty() {
            exts.push(Ext::Hostname(me.hostname.clone()));
        }
        if !me.human_nodename.is_empty() {
            exts.push(Ext::HumanNodename(me.human_nodename.clone()));
        }
        let now = mstime() / 1000;
        for (name, expire) in &self.blacklist {
            if *expire >= now {
                exts.push(Ext::ForgottenNode(*name, (*expire - now) as u64));
            }
        }
        exts.push(Ext::ShardId(self.me().shard_id));
        exts
    }

    /// `clusterSendPing`.
    pub(crate) fn send_ping(&mut self, link: u64, kind: u16) {
        self.pings_sent += 1;
        let pings = self.pings_sent;
        let freshnodes = self.nodes.len().saturating_sub(2);
        let wanted = (self.nodes.len() / 10)
            .clamp(3, freshnodes.max(3))
            .min(freshnodes);
        let link_node = self
            .links
            .get(&link)
            .and_then(|l| if l.inbound { None } else { l.node });
        if kind == wire::PING
            && let Some(n) = link_node.and_then(|n| self.nodes.get_mut(&n))
        {
            n.ping_sent = mstime();
        }
        let mut gossip = Vec::new();
        // Random nodes, as `dictGetRandomKey` picks them.
        let names: Vec<Name> = self.nodes.keys().copied().collect();
        let mut fresh = freshnodes;
        let mut iterations = wanted * 3;
        while fresh > 0 && gossip.len() < wanted && iterations > 0 {
            iterations -= 1;
            let name = names[(random_u64() as usize) % names.len()];
            let n = self.nodes.get_mut(&name).expect("known");
            if name == self.myself || n.flags & PFAIL != 0 {
                continue;
            }
            if n.flags & (HANDSHAKE | NOADDR) != 0 || (n.link.is_none() && n.numslots == 0) {
                fresh -= 1;
                continue;
            }
            if n.last_in_ping_gossip == pings {
                continue;
            }
            n.last_in_ping_gossip = pings;
            gossip.push(State::gossip_entry(n));
            fresh -= 1;
        }
        // And every node this one thinks may be failing.
        let mut pfail_wanted = self.stats_pfail_nodes;
        for n in self.nodes.values() {
            if pfail_wanted == 0 {
                break;
            }
            if n.flags & (HANDSHAKE | NOADDR) != 0 || n.flags & PFAIL == 0 {
                continue;
            }
            gossip.push(State::gossip_entry(n));
            pfail_wanted -= 1;
        }
        let exts = self.ping_exts();
        let msg = self.message(kind, Body::Ping { gossip, exts });
        self.send(Some(link), &msg);
    }

    /// `clusterBroadcastPong`: to all, or only to this shard's replicas.
    pub(crate) fn broadcast_pong(&mut self, local_slaves: bool) {
        let my_master = self.me().slaveof;
        let targets: Vec<u64> = self
            .nodes
            .values()
            .filter(|n| n.flags & (MYSELF | HANDSHAKE) == 0)
            .filter(|n| {
                !local_slaves
                    || (n.is_slave()
                        && n.slaveof
                            .is_some_and(|m| m == self.myself || Some(m) == my_master))
            })
            .filter_map(|n| n.link)
            .collect();
        for link in targets {
            self.send_ping(link, wire::PONG);
        }
    }

    /// `clusterSendUpdate`.
    fn send_update(&mut self, link: Option<u64>, about: Name) {
        let Some(node) = self.nodes.get(&about) else {
            return;
        };
        let mut slots = node.slots.clone();
        for (s, nc) in slots.iter_mut().zip(self.owner_not_claiming.iter()) {
            *s &= !nc;
        }
        let msg = self.message(
            wire::UPDATE,
            Body::Update {
                config_epoch: node.config_epoch,
                name: about,
                slots,
            },
        );
        self.send(link, &msg);
    }

    /// `clusterPropagatePublish`.
    pub(crate) fn propagate_publish(&mut self, channel: &[u8], message: &[u8], shard: bool) {
        let body = Body::Publish {
            channel: channel.to_vec(),
            message: message.to_vec(),
        };
        if !shard {
            let msg = self.message(wire::PUBLISH, body);
            self.broadcast(&msg);
            return;
        }
        let msg = self.message(wire::PUBLISHSHARD, body);
        let shard_id = self.me().shard_id;
        let targets: Vec<u64> = self
            .nodes
            .values()
            .filter(|n| n.shard_id == shard_id && n.flags & (MYSELF | HANDSHAKE) == 0)
            .filter_map(|n| n.link)
            .collect();
        for link in targets {
            self.send(Some(link), &msg);
        }
    }

    // ---- what comes in ------------------------------------------------

    /// `getNodeFromLinkAndMsg`.
    fn sender_of(&mut self, link: u64, hdr: &Header) -> Option<Name> {
        let link_node = self.links.get(&link).and_then(|l| l.node);
        if let Some(n) = link_node
            && self.nodes.get(&n).is_some_and(|n| !n.in_handshake())
        {
            return Some(n);
        }
        let sender = valid_name(&hdr.sender).filter(|n| self.nodes.contains_key(n))?;
        if link_node.is_none() {
            // An inbound link from a node known by now.
            if let Some(l) = self.links.get_mut(&link) {
                l.node = Some(sender);
            }
            let old = self.nodes.get(&sender).and_then(|n| n.inbound_link);
            if let Some(old) = old
                && old != link
            {
                self.free_link(old);
            }
            if let Some(n) = self.nodes.get_mut(&sender) {
                n.inbound_link = Some(link);
            }
        }
        Some(sender)
    }

    /// `nodeIp2String`: the announced IP, or the link's peer's.
    fn ip_of(&self, link: u64, announced: &str) -> String {
        if !announced.is_empty() {
            return announced.to_string();
        }
        self.links
            .get(&link)
            .map(|l| l.peer_ip.clone())
            .unwrap_or_default()
    }

    /// `nodeUpdateAddressIfNeeded`.
    fn update_address_if_needed(&mut self, name: Name, link: u64, hdr: &Header) -> bool {
        let ip = self.ip_of(link, &hdr.myip);
        let Some(node) = self.nodes.get(&name) else {
            return false;
        };
        if node.link == Some(link) {
            return false;
        }
        if node.tcp_port == hdr.port
            && node.cport == hdr.cport
            && node.tls_port == hdr.pport
            && node.ip == ip
        {
            return false;
        }
        let old_link = node.link;
        let node = self.nodes.get_mut(&name).expect("known");
        node.ip = ip.clone();
        node.tcp_port = hdr.port;
        node.tls_port = hdr.pport;
        node.cport = hdr.cport;
        node.flags &= !NOADDR;
        if let Some(l) = old_link {
            self.free_link(l);
        }
        log(&format!(
            "Address updated for node {}, now {ip}:{}",
            name_str(&name),
            hdr.port
        ));
        if self.me().is_slave() && self.me().slaveof == Some(name) {
            self.actions.push(Action::Follow(ip, hdr.port));
        }
        true
    }

    /// `clusterProcessGossipSection`.
    fn process_gossip(&mut self, hdr: &Header, gossip: &[Gossip], link: u64) {
        let link_node = self.links.get(&link).and_then(|l| l.node);
        let sender =
            link_node.or_else(|| valid_name(&hdr.sender).filter(|n| self.nodes.contains_key(n)));
        let now = mstime();
        for g in gossip {
            let flags = g.flags;
            let known = valid_name(&g.name).filter(|n| self.nodes.contains_key(n));
            if let Some(name) = known {
                let sender_is_master = sender
                    .and_then(|s| self.nodes.get(&s))
                    .is_some_and(Node::is_master);
                if let Some(s) = sender
                    && sender_is_master
                    && name != self.myself
                {
                    if flags & (FAIL | PFAIL) != 0 {
                        self.add_failure_report(name, s);
                        self.mark_failing_if_needed(name);
                    } else {
                        self.del_failure_report(name, s);
                    }
                }
                let reports = self.failure_reports(name);
                let node = self.nodes.get_mut(&name).expect("known");
                if flags & (FAIL | PFAIL) == 0 && node.ping_sent == 0 && reports == 0 {
                    let pong = i64::from(g.pong_received) * 1000;
                    if pong <= now + 500 && pong > node.pong_received {
                        node.pong_received = pong;
                    }
                }
                if node.flags & (FAIL | PFAIL) != 0
                    && flags & NOADDR == 0
                    && flags & (FAIL | PFAIL) == 0
                    && (!node.ip.eq_ignore_ascii_case(&g.ip)
                        || node.tls_port != g.pport
                        || node.tcp_port != g.port
                        || node.cport != g.cport)
                {
                    let old = node.link.take();
                    node.ip = g.ip.clone();
                    node.tcp_port = g.port;
                    node.tls_port = g.pport;
                    node.cport = g.cport;
                    node.flags &= !NOADDR;
                    if let Some(l) = old {
                        self.free_link(l);
                    }
                }
            } else if sender.is_some()
                && flags & NOADDR == 0
                && !self.blacklisted(&g.name)
                && let Some(name) = valid_name(&g.name)
            {
                let mut node = Node::new(Some(name), flags);
                node.ip = g.ip.clone();
                node.tcp_port = g.port;
                node.tls_port = g.pport;
                node.cport = g.cport;
                self.add_node(node);
            }
        }
    }

    /// `clusterProcessPingExtensions`.
    fn process_ping_exts(&mut self, hdr: &Header, exts: &[Ext], link: u64) {
        let link_node = self.links.get(&link).and_then(|l| l.node);
        let Some(sender) =
            link_node.or_else(|| valid_name(&hdr.sender).filter(|n| self.nodes.contains_key(n)))
        else {
            return;
        };
        let (mut hostname, mut human, mut shard) = (None, None, None);
        for ext in exts {
            match ext {
                Ext::Hostname(h) => hostname = Some(h.clone()),
                Ext::HumanNodename(h) => human = Some(h.clone()),
                Ext::ShardId(id) => shard = Some(*id),
                Ext::ForgottenNode(name, ttl) => {
                    let me = self.me();
                    if self.nodes.contains_key(name)
                        && *name != self.myself
                        && !(me.is_slave() && me.slaveof == Some(*name))
                    {
                        self.blacklist.insert(*name, mstime() / 1000 + *ttl as i64);
                        self.del_node(*name);
                        self.todo(TODO_UPDATE_STATE | TODO_SAVE_CONFIG);
                    }
                }
                Ext::Other => {}
            }
        }
        if let Some(node) = self.nodes.get_mut(&sender) {
            node.hostname = hostname.unwrap_or_default();
            node.human_nodename = human.unwrap_or_default();
        }
        let shard = shard.or_else(|| {
            let n = self.nodes.get(&sender)?;
            let master = n.slaveof.and_then(|m| self.nodes.get(&m)).unwrap_or(n);
            Some(master.shard_id)
        });
        if let Some(shard) = shard {
            self.update_shard_id(sender, shard);
        }
    }

    /// `clusterProcessPacket`: what a node sent on link `link`.
    pub(crate) fn process(&mut self, link: u64, msg: Msg) {
        let now = mstime();
        let hdr = &msg.header;
        let kind = hdr.kind;
        if let Some(n) = self.stats_received.get_mut(kind as usize) {
            *n += 1;
        }
        let flags = hdr.flags;
        let sender = self.sender_of(link, hdr);
        let (mut sender_current_epoch, mut sender_config_epoch) = (0, 0);
        if let Some(s) = sender {
            self.nodes.get_mut(&s).expect("known").data_received = now;
        }
        if let Some(s) = sender
            && !self.nodes[&s].in_handshake()
        {
            sender_current_epoch = hdr.current_epoch;
            sender_config_epoch = hdr.config_epoch;
            if sender_current_epoch > self.current_epoch {
                self.current_epoch = sender_current_epoch;
            }
            let node = self.nodes.get_mut(&s).expect("known");
            if sender_config_epoch > node.config_epoch {
                node.config_epoch = sender_config_epoch;
                self.todo(TODO_SAVE_CONFIG | TODO_FSYNC_CONFIG);
            }
            let node = self.nodes.get_mut(&s).expect("known");
            node.repl_offset = hdr.offset;
            node.repl_offset_time = now;
            if self.mf_end != 0
                && self.me().is_slave()
                && self.me().slaveof == Some(s)
                && hdr.mflags[0] & wire::FLAG0_PAUSED != 0
                && self.mf_master_offset == -1
            {
                self.mf_master_offset = hdr.offset as i64;
                self.todo(TODO_HANDLE_MANUALFAILOVER);
                log(&format!(
                    "Received replication offset for paused master manual failover: {}",
                    self.mf_master_offset
                ));
            }
        }
        let is_ping = matches!(kind, wire::PING | wire::PONG | wire::MEET);
        if kind == wire::PING || kind == wire::MEET {
            // The address others reach this node at.
            if (kind == wire::MEET || self.me().ip.is_empty())
                && self.settings.announce_ip.is_none()
            {
                let ip = self
                    .links
                    .get(&link)
                    .map(|l| l.local_ip.clone())
                    .unwrap_or_default();
                if !ip.is_empty() && ip != self.me().ip {
                    log(&format!("IP address for this node updated to {ip}"));
                    self.me_mut().ip = ip;
                    self.todo(TODO_SAVE_CONFIG);
                }
            }
            if sender.is_none() && kind == wire::MEET {
                let mut node = Node::new(None, HANDSHAKE);
                node.ip = self.ip_of(link, &hdr.myip);
                node.tcp_port = hdr.port;
                node.tls_port = hdr.pport;
                node.cport = hdr.cport;
                self.add_node(node);
                self.todo(TODO_SAVE_CONFIG);
                if let Body::Ping { gossip, .. } = &msg.body {
                    self.process_gossip(hdr, gossip, link);
                }
            }
            self.send_ping(link, wire::PONG);
        }
        if is_ping {
            let link_info = self.links.get(&link).map(|l| (l.inbound, l.node));
            if let Some((false, Some(link_node))) = link_info {
                if self.nodes.get(&link_node).is_some_and(Node::in_handshake) {
                    if let Some(s) = sender {
                        if self.update_address_if_needed(s, link, hdr) {
                            self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
                        }
                        self.del_node(link_node);
                        return;
                    }
                    let Some(new_name) = valid_name(&hdr.sender) else {
                        return;
                    };
                    self.rename_node(link_node, new_name);
                    let node = self.nodes.get_mut(&new_name).expect("renamed");
                    node.flags &= !HANDSHAKE;
                    node.flags |= flags & (MASTER | SLAVE);
                    self.todo(TODO_SAVE_CONFIG);
                } else if link_node != hdr.sender {
                    let node = self.nodes.get_mut(&link_node).expect("known");
                    node.flags |= NOADDR;
                    node.ip.clear();
                    node.tcp_port = 0;
                    node.tls_port = 0;
                    node.cport = 0;
                    self.free_link(link);
                    self.todo(TODO_SAVE_CONFIG);
                    return;
                }
            }
            let sender = self.sender_of(link, hdr);
            if let Some(s) = sender {
                let node = self.nodes.get_mut(&s).expect("known");
                node.flags = (node.flags & !NOFAILOVER) | (flags & NOFAILOVER);
            }
            if let Some(s) = sender
                && kind == wire::PING
                && !self.nodes[&s].in_handshake()
                && self.update_address_if_needed(s, link, hdr)
            {
                self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
            }
            let link_info = self.links.get(&link).map(|l| (l.inbound, l.node));
            if kind == wire::PONG
                && let Some((false, Some(n))) = link_info
                && let Some(node) = self.nodes.get_mut(&n)
            {
                node.pong_received = now;
                node.ping_sent = 0;
                if node.timed_out() {
                    node.flags &= !PFAIL;
                    self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
                } else if node.failed() {
                    self.clear_failure_if_needed(n);
                }
            }
            if let Some(s) = sender {
                if hdr.slaveof == NULL_NAME {
                    self.set_node_as_master(s);
                } else {
                    let master = valid_name(&hdr.slaveof).filter(|m| self.nodes.contains_key(m));
                    if self.nodes[&s].is_master() {
                        self.del_node_slots(s);
                        let node = self.nodes.get_mut(&s).expect("known");
                        node.flags &= !(MASTER | MIGRATE_TO);
                        node.flags |= SLAVE;
                        self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
                    }
                    if let Some(m) = master
                        && self.nodes[&s].slaveof != Some(m)
                    {
                        if let Some(old) = self.nodes[&s].slaveof {
                            self.remove_replica(old, s);
                        }
                        self.add_replica(m, s);
                        self.nodes.get_mut(&s).expect("known").slaveof = Some(m);
                        let shard = self.nodes[&m].shard_id;
                        self.update_shard_id(s, shard);
                        self.todo(TODO_SAVE_CONFIG);
                    }
                }
            }
            // The sender's (or its master's) slots, against what this node knows.
            let mut dirty = false;
            if let Some(s) = sender {
                let sn = &self.nodes[&s];
                let sender_master = if sn.is_master() { Some(s) } else { sn.slaveof };
                if let Some(m) = sender_master.and_then(|m| self.nodes.get(&m)) {
                    dirty = m.slots[..] != hdr.myslots[..];
                }
            }
            if let Some(s) = sender
                && self.nodes[&s].is_master()
                && dirty
            {
                self.update_slots_config_with(s, sender_config_epoch, &hdr.myslots);
            }
            // A sender behind on slots that another owns now: tell it.
            if let Some(s) = sender
                && dirty
            {
                for j in 0..SLOTS {
                    if wire::slot_bit(&hdr.myslots, j) {
                        if self.slots[j] == Some(s) || self.slot_unclaimed(j) {
                            continue;
                        }
                        if self.config_epoch_of(self.slots[j]) > sender_config_epoch {
                            let owner = self.slots[j].expect("claimed");
                            let link = self.nodes.get(&s).and_then(|n| n.link);
                            self.send_update(link, owner);
                            break;
                        }
                    }
                }
            }
            if let Some(s) = sender
                && self.me().is_master()
                && self.nodes[&s].is_master()
                && sender_config_epoch == self.me().config_epoch
            {
                self.handle_epoch_collision(s);
            }
            if sender.is_some()
                && let Body::Ping { gossip, exts } = &msg.body
            {
                self.process_gossip(hdr, gossip, link);
                self.process_ping_exts(hdr, exts, link);
            }
            let _ = sender_current_epoch;
            return;
        }
        match (&msg.body, kind) {
            (Body::Fail(about), wire::FAIL) => {
                if sender.is_some()
                    && let Some(failing) = valid_name(about).filter(|n| self.nodes.contains_key(n))
                {
                    let node = self.nodes.get_mut(&failing).expect("known");
                    if node.flags & (FAIL | MYSELF) == 0 {
                        log(&format!(
                            "FAIL message received from {} about {}",
                            name_str(&hdr.sender),
                            name_str(&failing)
                        ));
                        node.flags |= FAIL;
                        node.fail_time = now;
                        node.flags &= !PFAIL;
                        self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
                    }
                }
            }
            (Body::Publish { channel, message }, wire::PUBLISH | wire::PUBLISHSHARD) => {
                if sender.is_some() {
                    self.actions.push(Action::Publish {
                        channel: channel.clone(),
                        message: message.clone(),
                        shard: kind == wire::PUBLISHSHARD,
                    });
                }
            }
            (_, wire::AUTH_REQUEST) => {
                if let Some(s) = sender {
                    self.send_failover_auth_if_needed(s, hdr);
                }
            }
            (_, wire::AUTH_ACK) => {
                if let Some(s) = sender {
                    let n = &self.nodes[&s];
                    if n.is_master()
                        && n.numslots > 0
                        && sender_current_epoch >= self.failover_auth_epoch
                    {
                        self.failover_auth_count += 1;
                        self.todo(TODO_HANDLE_FAILOVER);
                    }
                }
            }
            (_, wire::MFSTART) => {
                let Some(s) = sender else { return };
                if self.nodes[&s].slaveof != Some(self.myself) {
                    return;
                }
                self.reset_manual_failover();
                self.mf_end = now + MF_TIMEOUT;
                self.mf_slave = Some(s);
                self.actions
                    .push(Action::Pause(now + MF_TIMEOUT * MF_PAUSE_MULT));
                log(&format!(
                    "Manual failover requested by replica {}.",
                    name_str(&s)
                ));
                self.send_ping(link, wire::PING);
            }
            (
                Body::Update {
                    config_epoch,
                    name,
                    slots,
                },
                wire::UPDATE,
            ) => {
                if sender.is_none() {
                    return;
                }
                let Some(n) = valid_name(name).filter(|n| self.nodes.contains_key(n)) else {
                    return;
                };
                if self.nodes[&n].config_epoch >= *config_epoch {
                    return;
                }
                if self.nodes[&n].is_slave() {
                    self.set_node_as_master(n);
                }
                self.nodes.get_mut(&n).expect("known").config_epoch = *config_epoch;
                self.todo(TODO_SAVE_CONFIG | TODO_FSYNC_CONFIG);
                self.update_slots_config_with(n, *config_epoch, slots);
            }
            _ => {}
        }
    }

    // ---- elections ---------------------------------------------------

    /// `clusterSendFailoverAuthIfNeeded`: a master's vote for a replica
    /// asking to take over from its failed master.
    fn send_failover_auth_if_needed(&mut self, node: Name, request: &Header) {
        let me = self.me();
        if me.is_slave() || me.numslots == 0 {
            return;
        }
        if request.current_epoch < self.current_epoch {
            return;
        }
        if self.last_vote_epoch == self.current_epoch {
            return;
        }
        let n = &self.nodes[&node];
        let force_ack = request.mflags[0] & wire::FLAG0_FORCEACK != 0;
        let master = n.slaveof.and_then(|m| self.nodes.get(&m));
        if n.is_master() || master.is_none() || (!master.is_some_and(Node::failed) && !force_ack) {
            return;
        }
        let master = master.expect("checked");
        if mstime() - master.voted_time < self.settings.node_timeout * 2 {
            return;
        }
        for j in 0..SLOTS {
            if !wire::slot_bit(&request.myslots, j) {
                continue;
            }
            if self.slot_unclaimed(j) || self.config_epoch_of(self.slots[j]) <= request.config_epoch
            {
                continue;
            }
            return;
        }
        self.last_vote_epoch = self.current_epoch;
        let master_name = master.name;
        self.nodes.get_mut(&master_name).expect("known").voted_time = mstime();
        self.todo(TODO_SAVE_CONFIG | TODO_FSYNC_CONFIG);
        let link = self.nodes.get(&node).and_then(|n| n.link);
        let msg = self.message(wire::AUTH_ACK, Body::None);
        self.send(link, &msg);
        log(&format!(
            "Failover auth granted to {} for epoch {}",
            name_str(&node),
            self.current_epoch
        ));
    }

    /// `clusterGetSlaveRank`.
    fn replica_rank(&self) -> usize {
        let Some(master) = self.me().slaveof.and_then(|m| self.nodes.get(&m)) else {
            return 0;
        };
        let myoffset = self.repl.offset;
        master
            .replicas
            .iter()
            .filter(|r| **r != self.myself)
            .filter_map(|r| self.nodes.get(r))
            .filter(|r| r.flags & NOFAILOVER == 0 && r.repl_offset > myoffset)
            .count()
    }

    /// `clusterFailoverReplaceYourMaster`.
    pub(crate) fn replace_your_master(&mut self) {
        let myself = self.myself;
        let Some(old) = self.me().slaveof.filter(|_| !self.me().is_master()) else {
            return;
        };
        self.set_node_as_master(myself);
        self.actions.push(Action::Promote);
        for j in 0..SLOTS {
            if self
                .nodes
                .get(&old)
                .is_some_and(|n| wire::slot_bit(&n.slots, j))
            {
                self.del_slot(j);
                self.add_slot(myself, j);
            }
        }
        self.update_state();
        let _ = self.save_config();
        self.broadcast_pong(false);
        self.reset_manual_failover();
    }

    /// `clusterHandleSlaveFailover`.
    pub(crate) fn handle_replica_failover(&mut self) {
        let now = mstime();
        let auth_age = now - self.failover_auth_time;
        let needed = self.size / 2 + 1;
        let manual = self.mf_end != 0 && self.mf_can_start;
        self.todo &= !TODO_HANDLE_FAILOVER;
        let auth_timeout = (self.settings.node_timeout * 2).max(2000);
        let auth_retry = auth_timeout * 2;
        let me = self.me();
        let master = me.slaveof.and_then(|m| self.nodes.get(&m));
        if me.is_master()
            || master.is_none()
            || (!master.is_some_and(Node::failed) && !manual)
            || (self.settings.replica_no_failover && !manual)
            || master.is_some_and(|m| m.numslots == 0)
        {
            return;
        }
        let mut data_age = self.repl.data_age;
        if data_age > self.settings.node_timeout {
            data_age -= self.settings.node_timeout;
        }
        if self.settings.replica_validity_factor != 0
            && data_age
                > self.settings.repl_ping_period * 1000
                    + self.settings.node_timeout * self.settings.replica_validity_factor
            && !manual
        {
            return;
        }
        if auth_age > auth_retry {
            self.failover_auth_time = now + 500 + (random_u64() % 500) as i64;
            self.failover_auth_count = 0;
            self.failover_auth_sent = false;
            self.failover_auth_rank = self.replica_rank();
            self.failover_auth_time += self.failover_auth_rank as i64 * 1000;
            if self.mf_end != 0 {
                self.failover_auth_time = now;
                self.failover_auth_rank = 0;
                self.todo(TODO_HANDLE_FAILOVER);
            }
            log(&format!(
                "Start of election delayed for {} milliseconds (rank #{}, offset {}).",
                self.failover_auth_time - now,
                self.failover_auth_rank,
                self.repl.offset
            ));
            self.broadcast_pong(true);
            return;
        }
        if !self.failover_auth_sent && self.mf_end == 0 {
            let newrank = self.replica_rank();
            if newrank > self.failover_auth_rank {
                self.failover_auth_time += (newrank - self.failover_auth_rank) as i64 * 1000;
                self.failover_auth_rank = newrank;
            }
        }
        if now < self.failover_auth_time || auth_age > auth_timeout {
            return;
        }
        if !self.failover_auth_sent {
            self.current_epoch += 1;
            self.failover_auth_epoch = self.current_epoch;
            log(&format!(
                "Starting a failover election for epoch {}.",
                self.current_epoch
            ));
            let mut msg = self.message(wire::AUTH_REQUEST, Body::None);
            if self.mf_end != 0 {
                msg.header.mflags[0] |= wire::FLAG0_FORCEACK;
            }
            self.broadcast(&msg);
            self.failover_auth_sent = true;
            self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE | TODO_FSYNC_CONFIG);
            return;
        }
        if self.failover_auth_count >= needed {
            log("Failover election won: I'm the new master.");
            let epoch = self.failover_auth_epoch;
            if self.me().config_epoch < epoch {
                self.me_mut().config_epoch = epoch;
            }
            self.replace_your_master();
        }
    }

    pub(crate) fn reset_manual_failover(&mut self) {
        if self.mf_slave.is_some() {
            self.actions.push(Action::Pause(0));
        }
        self.mf_end = 0;
        self.mf_can_start = false;
        self.mf_slave = None;
        self.mf_master_offset = -1;
    }

    /// `CLUSTER FAILOVER [FORCE|TAKEOVER]`, once checked.
    pub(crate) fn start_manual_failover(&mut self, force: bool, takeover: bool) {
        self.reset_manual_failover();
        self.mf_end = mstime() + MF_TIMEOUT;
        if takeover {
            self.bump_epoch();
            self.replace_your_master();
        } else if force {
            self.mf_can_start = true;
        } else {
            let link = self
                .me()
                .slaveof
                .and_then(|m| self.nodes.get(&m))
                .and_then(|m| m.link);
            let msg = self.message(wire::MFSTART, Body::None);
            self.send(link, &msg);
        }
    }

    fn handle_manual_failover(&mut self) {
        if self.mf_end == 0 || self.mf_can_start || self.mf_master_offset == -1 {
            return;
        }
        if self.mf_master_offset == self.repl.offset as i64 {
            self.mf_can_start = true;
            log("All master replication stream processed, manual failover can start.");
            self.todo(TODO_HANDLE_FAILOVER);
            return;
        }
        self.todo(TODO_HANDLE_MANUALFAILOVER);
    }

    // ---- cron ----------------------------------------------------------

    /// `clusterCron`, ten times a second; returns the nodes to connect to:
    /// `(name, ip, cport)`.
    pub(crate) fn cron(&mut self) -> Vec<(Name, String, u16)> {
        self.iteration += 1;
        let now = mstime();
        let handshake_timeout = self.settings.node_timeout.max(1000);
        self.stats_pfail_nodes = 0;
        let mut connect = Vec::new();
        let names: Vec<Name> = self.nodes.keys().copied().collect();
        for name in &names {
            let Some(node) = self.nodes.get(name) else {
                continue;
            };
            if node.flags & (MYSELF | NOADDR) != 0 {
                continue;
            }
            if node.flags & PFAIL != 0 {
                self.stats_pfail_nodes += 1;
            }
            if node.in_handshake() && now - node.ctime > handshake_timeout {
                self.del_node(*name);
                continue;
            }
            if node.link.is_none() && node.cport != 0 && !node.ip.is_empty() {
                connect.push((*name, node.ip.clone(), node.cport));
            }
        }
        // Every second, a ping to whichever of 5 random nodes answered last.
        if self.iteration.is_multiple_of(10) {
            let mut best: Option<(Name, i64)> = None;
            for _ in 0..5 {
                let name = names[(random_u64() as usize) % names.len()];
                let Some(n) = self.nodes.get(&name) else {
                    continue;
                };
                if n.link.is_none() || n.ping_sent != 0 || n.flags & (MYSELF | HANDSHAKE) != 0 {
                    continue;
                }
                if best.is_none_or(|(_, p)| p > n.pong_received) {
                    best = Some((name, n.pong_received));
                }
            }
            if let Some((name, _)) = best
                && let Some(link) = self.nodes[&name].link
            {
                self.send_ping(link, wire::PING);
            }
        }
        let mut update_state = false;
        let timeout = self.settings.node_timeout;
        for name in &names {
            let now = mstime();
            let Some(node) = self.nodes.get(name) else {
                continue;
            };
            if node.flags & (MYSELF | NOADDR | HANDSHAKE) != 0 {
                continue;
            }
            let ping_delay = now - node.ping_sent;
            let data_delay = now - node.data_received;
            let link_ctime = node.link.and_then(|l| self.links.get(&l)).map(|l| l.ctime);
            if let Some(ctime) = link_ctime
                && now - ctime > timeout
                && node.ping_sent != 0
                && ping_delay > timeout / 2
                && data_delay > timeout / 2
            {
                let link = node.link.expect("linked");
                self.free_link(link);
            }
            let node = &self.nodes[name];
            if let Some(link) = node.link
                && node.ping_sent == 0
                && now - node.pong_received > timeout / 2
            {
                self.send_ping(link, wire::PING);
                continue;
            }
            if self.mf_end != 0
                && self.me().is_master()
                && self.mf_slave == Some(*name)
                && let Some(link) = node.link
            {
                self.send_ping(link, wire::PING);
                continue;
            }
            let node = &self.nodes[name];
            if node.ping_sent == 0 {
                continue;
            }
            let delay = ping_delay.min(data_delay);
            if delay > timeout && node.flags & (PFAIL | FAIL) == 0 {
                log(&format!("*** NODE {} possibly failing", name_str(name)));
                self.nodes.get_mut(name).expect("known").flags |= PFAIL;
                update_state = true;
            }
        }
        // A replica that isn't following its master yet: follow it.
        if self.me().is_slave()
            && !self.repl.following
            && let Some(m) = self.me().slaveof.and_then(|m| self.nodes.get(&m))
            && m.has_addr()
            && !m.ip.is_empty()
        {
            let (ip, port) = (m.ip.clone(), m.tcp_port);
            self.actions.push(Action::Follow(ip, port));
        }
        if self.mf_end != 0 && self.mf_end < mstime() {
            self.reset_manual_failover();
        }
        if self.me().is_slave() {
            self.handle_manual_failover();
            self.handle_replica_failover();
        }
        if update_state || self.state == DOWN {
            self.update_state();
        }
        connect
    }

    /// `clusterBeforeSleep`: what the last messages and commands asked for.
    pub(crate) fn before_sleep(&mut self) {
        let flags = std::mem::take(&mut self.todo);
        if flags & TODO_HANDLE_MANUALFAILOVER != 0 {
            if self.me().is_slave() {
                self.handle_manual_failover();
                self.handle_replica_failover();
            }
        } else if flags & TODO_HANDLE_FAILOVER != 0 {
            self.handle_replica_failover();
        }
        if flags & TODO_UPDATE_STATE != 0 {
            self.update_state();
        }
        if flags & TODO_SAVE_CONFIG != 0
            && let Err(e) = self.save_config()
        {
            log(&format!("Fatal: can't update cluster config file: {e}"));
            std::process::exit(1);
        }
        // (What those steps asked for waits for the next time.)
        self.todo &= !(TODO_SAVE_CONFIG | TODO_UPDATE_STATE);
    }

    /// `clusterUpdateState`.
    pub(crate) fn update_state(&mut self) {
        let now = mstime();
        self.todo &= !TODO_UPDATE_STATE;
        if self.first_call_time == 0 {
            self.first_call_time = now;
        }
        if self.me().is_master()
            && self.state == DOWN
            && now - self.first_call_time < WRITABLE_DELAY
        {
            return;
        }
        let mut new_state = OK;
        if self.settings.require_full_coverage {
            for owner in &self.slots {
                match owner.and_then(|o| self.nodes.get(&o)) {
                    None => {
                        new_state = DOWN;
                        break;
                    }
                    Some(n) if n.failed() => {
                        new_state = DOWN;
                        break;
                    }
                    _ => {}
                }
            }
        }
        let mut reachable = 0;
        self.size = 0;
        for n in self.nodes.values() {
            if n.is_master() && n.numslots > 0 {
                self.size += 1;
                if n.flags & (FAIL | PFAIL) == 0 {
                    reachable += 1;
                }
            }
        }
        if reachable < self.size / 2 + 1 {
            new_state = DOWN;
            self.among_minority_time = now;
        }
        if new_state != self.state {
            let rejoin = self
                .settings
                .node_timeout
                .clamp(MIN_REJOIN_DELAY, MAX_REJOIN_DELAY);
            if new_state == OK && self.me().is_master() && now - self.among_minority_time < rejoin {
                return;
            }
            log(&format!(
                "Cluster state changed: {}",
                if new_state == OK { "ok" } else { "fail" }
            ));
            self.state = new_state;
        }
    }

    /// `clusterReset`.
    pub(crate) fn reset(&mut self, hard: bool) {
        let myself = self.myself;
        if self.me().is_slave() {
            self.set_node_as_master(myself);
            self.me_mut().slaveof = None;
            self.actions.push(Action::PromoteAndEmpty);
        }
        self.close_all_slots();
        self.reset_manual_failover();
        for j in 0..SLOTS {
            self.del_slot(j);
        }
        let others: Vec<Name> = self
            .nodes
            .keys()
            .copied()
            .filter(|n| *n != myself)
            .collect();
        for n in others {
            self.del_node(n);
        }
        self.blacklist.clear();
        if hard {
            self.current_epoch = 0;
            self.last_vote_epoch = 0;
            let mut me = self.nodes.remove(&myself).expect("myself");
            me.config_epoch = 0;
            me.name = random_name();
            me.shard_id = random_name();
            self.myself = me.name;
            self.nodes.insert(me.name, me);
        }
        self.todo(TODO_SAVE_CONFIG | TODO_UPDATE_STATE | TODO_FSYNC_CONFIG);
    }

    /// Link events from `bus.rs`.
    pub(crate) fn connected(&mut self, link: u64) {
        let Some(name) = self.links.get(&link).and_then(|l| l.node) else {
            return;
        };
        let Some(node) = self.nodes.get(&name) else {
            return;
        };
        let old_ping_sent = node.ping_sent;
        let kind = if node.flags & MEET != 0 {
            wire::MEET
        } else {
            wire::PING
        };
        self.send_ping(link, kind);
        let node = self.nodes.get_mut(&name).expect("known");
        if old_ping_sent != 0 {
            node.ping_sent = old_ping_sent;
        }
        node.flags &= !MEET;
    }

    pub(crate) fn connect_failed(&mut self, name: Name) {
        if let Some(node) = self.nodes.get_mut(&name)
            && node.ping_sent == 0
        {
            node.ping_sent = mstime();
        }
    }

    // ---- descriptions --------------------------------------------------

    pub(crate) fn flags_str(flags: u16) -> String {
        let mut out = Vec::new();
        for (bit, name) in [
            (MYSELF, "myself"),
            (MASTER, "master"),
            (SLAVE, "slave"),
            (PFAIL, "fail?"),
            (FAIL, "fail"),
            (HANDSHAKE, "handshake"),
            (NOADDR, "noaddr"),
            (NOFAILOVER, "nofailover"),
        ] {
            if flags & bit != 0 {
                out.push(name);
            }
        }
        if out.is_empty() {
            return "noflags".into();
        }
        out.join(",")
    }

    fn slot_ranges(slots: &Slots) -> Vec<(usize, usize)> {
        let mut ranges = Vec::new();
        let mut start = None;
        for j in 0..=SLOTS {
            let bit = j < SLOTS && wire::slot_bit(slots, j);
            match (bit, start) {
                (true, None) => start = Some(j),
                (false, Some(s)) => {
                    ranges.push((s, j - 1));
                    start = None;
                }
                _ => {}
            }
        }
        ranges
    }

    /// `clusterGenNodeDescription`; `for_config` adds what `nodes.conf`
    /// keeps (the aux fields).
    pub(crate) fn describe(&self, node: &Node, for_config: bool) -> String {
        let mut s = String::from(name_str(&node.name));
        let _ = write!(s, " {}:{}@{}", node.ip, node.tcp_port, node.cport);
        if !node.hostname.is_empty() {
            let _ = write!(s, ",{}", node.hostname);
        }
        if for_config {
            if node.hostname.is_empty() {
                s.push(',');
            }
            let _ = write!(s, ",tls-port={}", node.tls_port);
            if !node.human_nodename.is_empty() {
                let _ = write!(s, ",nodename={}", node.human_nodename);
            }
            let _ = write!(s, ",shard-id={}", name_str(&node.shard_id));
        }
        let _ = write!(s, " {} ", State::flags_str(node.flags));
        match node.slaveof {
            Some(m) => s.push_str(name_str(&m)),
            None => s.push('-'),
        }
        let epoch = match node.slaveof.and_then(|m| self.nodes.get(&m)) {
            Some(m) if node.is_slave() => m.config_epoch,
            _ => node.config_epoch,
        };
        let connected = node.link.is_some() || node.flags & MYSELF != 0;
        let _ = write!(
            s,
            " {} {} {epoch} {}",
            node.ping_sent,
            node.pong_received,
            if connected {
                "connected"
            } else {
                "disconnected"
            }
        );
        for (a, b) in State::slot_ranges(&node.slots) {
            if a == b {
                let _ = write!(s, " {a}");
            } else {
                let _ = write!(s, " {a}-{b}");
            }
        }
        if node.flags & MYSELF != 0 {
            for j in 0..SLOTS {
                if let Some(m) = self.migrating[j] {
                    let _ = write!(s, " [{j}->-{}]", name_str(&m));
                } else if let Some(i) = self.importing[j] {
                    let _ = write!(s, " [{j}-<-{}]", name_str(&i));
                }
            }
        }
        s
    }

    /// `CLUSTER NODES` (and `nodes.conf`'s lines, without the nodes in
    /// handshake).
    pub(crate) fn nodes_description(&self, for_config: bool) -> String {
        let mut out = String::new();
        for node in self.nodes.values() {
            if for_config && node.in_handshake() {
                continue;
            }
            out.push_str(&self.describe(node, for_config));
            out.push('\n');
        }
        out
    }

    /// `genClusterInfoString`.
    pub(crate) fn info(&self) -> String {
        let (mut assigned, mut ok, mut pfail, mut fail) = (0, 0, 0, 0);
        for owner in self.slots.iter().flatten() {
            assigned += 1;
            match self.nodes.get(owner) {
                Some(n) if n.failed() => fail += 1,
                Some(n) if n.timed_out() => pfail += 1,
                _ => ok += 1,
            }
        }
        let me = self.me();
        let my_epoch = match me.slaveof.and_then(|m| self.nodes.get(&m)) {
            Some(m) if me.is_slave() => m.config_epoch,
            _ => me.config_epoch,
        };
        let mut s = format!(
            "cluster_state:{}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{ok}\r\n\
             cluster_slots_pfail:{pfail}\r\ncluster_slots_fail:{fail}\r\ncluster_known_nodes:{}\r\n\
             cluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{my_epoch}\r\n",
            if self.state == OK { "ok" } else { "fail" },
            self.nodes.len(),
            self.size,
            self.current_epoch,
        );
        let mut total = 0;
        for (i, n) in self.stats_sent.iter().enumerate() {
            if *n != 0 {
                total += n;
                let _ = write!(
                    s,
                    "cluster_stats_messages_{}_sent:{n}\r\n",
                    wire::type_name(i)
                );
            }
        }
        let _ = write!(s, "cluster_stats_messages_sent:{total}\r\n");
        let mut total = 0;
        for (i, n) in self.stats_received.iter().enumerate() {
            if *n != 0 {
                total += n;
                let _ = write!(
                    s,
                    "cluster_stats_messages_{}_received:{n}\r\n",
                    wire::type_name(i)
                );
            }
        }
        let _ = write!(s, "cluster_stats_messages_received:{total}\r\n");
        s.push_str("total_cluster_links_buffer_limit_exceeded:0\r\n");
        s
    }

    // ---- nodes.conf ----------------------------------------------------

    /// `clusterSaveConfig`: the nodes, then the epochs, written through a
    /// temporary file.
    pub(crate) fn save_config(&self) -> std::io::Result<()> {
        let mut text = self.nodes_description(true);
        let _ = writeln!(
            text,
            "vars currentEpoch {} lastVoteEpoch {}",
            self.current_epoch, self.last_vote_epoch
        );
        let mut tmp = self.config_path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = std::path::PathBuf::from(tmp);
        std::fs::write(&tmp, text)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &self.config_path)
    }

    /// `clusterLoadConfig`: false if there's no file (a new node).
    pub(crate) fn load_config(&mut self) -> Result<bool, String> {
        let text = match std::fs::read_to_string(&self.config_path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.to_string()),
        };
        if text.trim().is_empty() {
            return Ok(false);
        }
        let bad =
            |line: &str| format!("Unrecoverable error: corrupted cluster config file \"{line}\".");
        self.nodes.clear();
        let mut myself = None;
        let mut relations = Vec::new();
        for line in text.lines() {
            let words: Vec<&str> = line.split(' ').filter(|w| !w.is_empty()).collect();
            if words.is_empty() {
                continue;
            }
            if words[0] == "vars" {
                for pair in words[1..].chunks(2) {
                    let value = pair.get(1).and_then(|v| v.parse().ok()).unwrap_or(0);
                    match pair[0] {
                        "currentEpoch" => self.current_epoch = value,
                        "lastVoteEpoch" => self.last_vote_epoch = value,
                        _ => {}
                    }
                }
                continue;
            }
            if words.len() < 8 {
                return Err(bad(line));
            }
            let name = valid_name(words[0].as_bytes()).ok_or_else(|| bad(line))?;
            let mut node = Node::new(Some(name), 0);
            // ip:port@cport[,hostname[,aux=value...]]
            let mut addr = words[1].split(',');
            let main = addr.next().unwrap_or("");
            let hostname = addr.next().unwrap_or("");
            for aux in addr {
                if let Some((k, v)) = aux.split_once('=') {
                    match k {
                        "shard-id" => {
                            if let Some(id) = valid_name(v.as_bytes()) {
                                node.shard_id = id;
                            }
                        }
                        "nodename" => node.human_nodename = v.to_string(),
                        "tls-port" => node.tls_port = v.parse().unwrap_or(0),
                        "tcp-port" => node.tcp_port = v.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
            node.hostname = hostname.to_string();
            let (hostport, cport) = main.split_once('@').ok_or_else(|| bad(line))?;
            let (ip, port) = hostport.rsplit_once(':').ok_or_else(|| bad(line))?;
            node.ip = ip.to_string();
            node.tcp_port = port.parse().map_err(|_| bad(line))?;
            node.cport = cport.parse().map_err(|_| bad(line))?;
            for flag in words[2].split(',') {
                node.flags |= match flag {
                    "myself" => MYSELF,
                    "master" => MASTER,
                    "slave" => SLAVE,
                    "fail?" => PFAIL,
                    "fail" => FAIL,
                    "handshake" => HANDSHAKE,
                    "noaddr" => NOADDR,
                    "nofailover" => NOFAILOVER,
                    _ => 0,
                };
            }
            if node.flags & MYSELF != 0 {
                myself = Some(name);
            }
            if words[3] != "-"
                && let Some(m) = valid_name(words[3].as_bytes())
            {
                relations.push((name, m));
            }
            node.ping_sent = if words[4].parse::<i64>().unwrap_or(0) != 0 {
                mstime()
            } else {
                0
            };
            node.pong_received = if words[5].parse::<i64>().unwrap_or(0) != 0 {
                mstime()
            } else {
                0
            };
            node.config_epoch = words[6].parse().map_err(|_| bad(line))?;
            self.nodes.insert(name, node);
            for range in &words[8..] {
                if let Some(rest) = range.strip_prefix('[') {
                    // [slot->-node] or [slot-<-node]: this node's migrations.
                    let rest = rest.trim_end_matches(']');
                    if let Some((slot, to)) = rest.split_once("->-")
                        && let (Ok(slot), Some(to)) =
                            (slot.parse::<usize>(), valid_name(to.as_bytes()))
                        && slot < SLOTS
                    {
                        self.migrating[slot] = Some(to);
                    } else if let Some((slot, from)) = rest.split_once("-<-")
                        && let (Ok(slot), Some(from)) =
                            (slot.parse::<usize>(), valid_name(from.as_bytes()))
                        && slot < SLOTS
                    {
                        self.importing[slot] = Some(from);
                    }
                    continue;
                }
                let (a, b) = match range.split_once('-') {
                    Some((a, b)) => (a.parse::<usize>(), b.parse::<usize>()),
                    None => (range.parse::<usize>(), range.parse::<usize>()),
                };
                let (Ok(a), Ok(b)) = (a, b) else {
                    return Err(bad(line));
                };
                for slot in a..=b.min(SLOTS - 1) {
                    self.slots[slot] = None;
                    self.add_slot(name, slot);
                }
            }
        }
        let myself = myself.ok_or("Unrecoverable error: no myself in the cluster config file")?;
        self.myself = myself;
        for (replica, master) in relations {
            self.nodes.entry(master).or_insert_with(|| {
                let mut m = Node::new(Some(master), 0);
                m.flags = MASTER;
                m
            });
            self.nodes.get_mut(&replica).expect("loaded").slaveof = Some(master);
            self.add_replica(master, replica);
        }
        // The ports this node is started with win over the file's.
        let (port, cport) = (self.settings.port, self.settings.cport);
        let me = self.me_mut();
        me.tcp_port = port;
        me.cport = cport;
        Ok(true)
    }
}

pub(crate) fn log(line: &str) {
    eprintln!("cluster: {line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_ranges_and_flags_read_as_redis_writes_them() {
        let mut slots = [0u8; SLOT_BYTES];
        for j in [0, 1, 2, 5, 16383] {
            wire::set_slot_bit(&mut slots, j, true);
        }
        assert_eq!(
            State::slot_ranges(&slots),
            vec![(0, 2), (5, 5), (16383, 16383)]
        );
        assert_eq!(State::flags_str(MYSELF | MASTER), "myself,master");
        assert_eq!(State::flags_str(0), "noflags");
        assert_eq!(State::flags_str(SLAVE | PFAIL), "slave,fail?");
    }

    #[test]
    fn nodes_conf_comes_back() {
        let dir =
            std::env::temp_dir().join(format!("tantalum-cluster-conf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nodes.conf");
        let mut state = State::new(Settings::default(), path.clone());
        let me = state.myself;
        for j in 0..100 {
            state.add_slot(me, j);
        }
        let other = random_name();
        let mut n = Node::new(Some(other), SLAVE);
        n.ip = "10.0.0.2".into();
        n.tcp_port = 7001;
        n.cport = 17001;
        n.slaveof = Some(me);
        state.add_node(n);
        state.add_replica(me, other);
        state.migrating[50] = Some(other);
        state.current_epoch = 7;
        state.me_mut().config_epoch = 3;
        state.save_config().unwrap();
        let mut back = State::new(Settings::default(), path);
        assert!(back.load_config().unwrap());
        assert_eq!(back.myself, me);
        assert_eq!(back.current_epoch, 7);
        assert_eq!(back.me().config_epoch, 3);
        assert_eq!(back.me().numslots, 100);
        assert_eq!(back.slots[99], Some(me));
        assert_eq!(back.migrating[50], Some(other));
        assert_eq!(back.nodes[&other].slaveof, Some(me));
        assert_eq!(back.me().replicas, vec![other]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
