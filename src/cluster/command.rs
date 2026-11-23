//! `CLUSTER ...`, `ASKING`, `READONLY` and `READWRITE`, with Redis 7.2's
//! replies (`clusterCommand`).

use super::state::{self, MYSELF, State, name_str};
use super::wire::{self, SLOTS};
use super::{ASKING, Cluster, READONLY, bus};
use crate::cmd::{Args, Reply};
use crate::context::{Context, Session};
use crate::resp;

const DISABLED: &[u8] = b"ERR This instance has cluster support disabled";

fn cluster_of(ctx: &Context, out: &mut Vec<u8>) -> Option<std::sync::Arc<Cluster>> {
    let c = ctx.cluster.clone();
    if c.is_none() {
        resp::write_error(out, DISABLED);
    }
    c
}

pub(crate) fn asking(ctx: &Context, session: &mut Session, out: &mut Vec<u8>) -> Reply {
    if cluster_of(ctx, out).is_some() {
        session.cluster |= ASKING;
        out.extend_from_slice(resp::OK);
    }
    Reply::Written
}

pub(crate) fn readonly(ctx: &Context, session: &mut Session, out: &mut Vec<u8>) -> Reply {
    if cluster_of(ctx, out).is_some() {
        session.cluster |= READONLY;
        out.extend_from_slice(resp::OK);
    }
    Reply::Written
}

pub(crate) fn readwrite(ctx: &Context, session: &mut Session, out: &mut Vec<u8>) -> Reply {
    if cluster_of(ctx, out).is_some() {
        session.cluster &= !READONLY;
        out.extend_from_slice(resp::OK);
    }
    Reply::Written
}

const ARITIES: &[(&str, i32)] = &[
    ("addslots", -3),
    ("addslotsrange", -4),
    ("bumpepoch", 2),
    ("count-failure-reports", 3),
    ("countkeysinslot", 3),
    ("delslots", -3),
    ("delslotsrange", -4),
    ("failover", -2),
    ("flushslots", 2),
    ("forget", 3),
    ("getkeysinslot", 4),
    ("help", 2),
    ("info", 2),
    ("keyslot", 3),
    ("links", 2),
    ("meet", -4),
    ("myid", 2),
    ("myshardid", 2),
    ("nodes", 2),
    ("replicas", 3),
    ("replicate", 3),
    ("reset", -2),
    ("saveconfig", 2),
    ("set-config-epoch", 3),
    ("setslot", -4),
    ("shards", 2),
    ("slaves", 3),
    ("slots", 2),
];

const HELP: &[&str] = &[
    "ADDSLOTS <slot> [<slot> ...]",
    "    Assign slots to current node.",
    "ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
    "    Assign slots which are between <start-slot> and <end-slot> to current node.",
    "BUMPEPOCH",
    "    Advance the cluster config epoch.",
    "COUNT-FAILURE-REPORTS <node-id>",
    "    Return number of failure reports for <node-id>.",
    "COUNTKEYSINSLOT <slot>",
    "    Return the number of keys in <slot>.",
    "DELSLOTS <slot> [<slot> ...]",
    "    Delete slots information from current node.",
    "DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
    "    Delete slots information which are between <start-slot> and <end-slot> from current node.",
    "FAILOVER [FORCE|TAKEOVER]",
    "    Promote current replica node to being a master.",
    "FORGET <node-id>",
    "    Remove a node from the cluster.",
    "GETKEYSINSLOT <slot> <count>",
    "    Return key names stored by current node in a slot.",
    "FLUSHSLOTS",
    "    Delete current node own slots information.",
    "INFO",
    "    Return information about the cluster.",
    "KEYSLOT <key>",
    "    Return the hash slot for <key>.",
    "MEET <ip> <port> [<bus-port>]",
    "    Connect nodes into a working cluster.",
    "MYID",
    "    Return the node id.",
    "MYSHARDID",
    "    Return the node's shard id.",
    "NODES",
    "    Return cluster configuration seen by node. Output format:",
    "    <id> <ip:port@bus-port[,hostname]> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
    "REPLICATE <node-id>",
    "    Configure current node as replica to <node-id>.",
    "RESET [HARD|SOFT]",
    "    Reset current node (default: soft).",
    "SET-CONFIG-EPOCH <epoch>",
    "    Set config epoch of current node.",
    "SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)",
    "    Set slot state.",
    "REPLICAS <node-id>",
    "    Return <node-id> replicas.",
    "SAVECONFIG",
    "    Force saving cluster configuration on disk.",
    "SLOTS",
    "    Return information about slots range mappings. Each range is made of:",
    "    start, end, master and replicas IP addresses, ports and ids",
    "SHARDS",
    "    Return information about slot range mappings and the nodes associated with them.",
    "LINKS",
    "    Return information about all network links between this node and its peers.",
    "    Output format is an array where each array element is a map containing attributes of a link",
];

fn verbatim(out: &mut Vec<u8>, session: &Session, text: &str) {
    if session.resp3 {
        out.extend_from_slice(format!("={}\r\ntxt:", text.len() + 4).as_bytes());
        out.extend_from_slice(text.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        resp::write_bulk(out, text.as_bytes());
    }
}

fn map_header(out: &mut Vec<u8>, session: &Session, n: usize) {
    if session.resp3 {
        out.extend_from_slice(format!("%{n}\r\n").as_bytes());
    } else {
        resp::write_array_header(out, n * 2);
    }
}

/// `getSlotOrReply`.
fn slot_of(arg: &[u8], out: &mut Vec<u8>) -> Option<usize> {
    match resp::parse_int(arg) {
        Some(s) if (0..SLOTS as i64).contains(&s) => Some(s as usize),
        _ => {
            resp::write_error(out, b"ERR Invalid or out of range slot");
            None
        }
    }
}

fn error(out: &mut Vec<u8>, msg: &str) {
    resp::write_error(out, msg.as_bytes());
}

fn arity(out: &mut Vec<u8>, sub: &[u8]) {
    error(
        out,
        &format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try CLUSTER HELP.",
            String::from_utf8_lossy(sub)
        ),
    );
}

pub(crate) fn cluster(
    ctx: &Context,
    session: &mut Session,
    args: &Args<'_>,
    out: &mut Vec<u8>,
) -> Reply {
    let Some(cluster) = cluster_of(ctx, out) else {
        return Reply::Written;
    };
    let n = args.len();
    let sub = String::from_utf8_lossy(args.get(1)).to_ascii_lowercase();
    let text = |i: usize| String::from_utf8_lossy(args.get(i)).into_owned();
    // Each subcommand's arity, as Redis' command table has it.
    if let Some((_, arity)) = ARITIES.iter().find(|(name, _)| *name == sub)
        && ((*arity > 0 && n as i32 != *arity) || (n as i32) < -*arity)
    {
        error(
            out,
            &format!("ERR wrong number of arguments for 'cluster|{sub}' command"),
        );
        return Reply::Written;
    }
    {
        let mut st = cluster.state.lock();
        st.repl.offset = ctx.repl.offset();
        let st = &mut *st;
        match (sub.as_str(), n) {
            ("help", 2) => {
                resp::write_array_header(out, HELP.len() + 3);
                resp::write_simple(
                    out,
                    b"CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                );
                for line in HELP {
                    resp::write_simple(out, line.as_bytes());
                }
                resp::write_simple(out, b"HELP");
                resp::write_simple(out, b"    Print this help.");
            }
            ("meet", 4 | 5) => {
                let Some(port) = resp::parse_int(args.get(3)) else {
                    error(
                        out,
                        &format!("ERR Invalid base port specified: {}", text(3)),
                    );
                    return Reply::Written;
                };
                let cport = if n == 5 {
                    let Some(c) = resp::parse_int(args.get(4)) else {
                        error(out, &format!("ERR Invalid bus port specified: {}", text(4)));
                        return Reply::Written;
                    };
                    c
                } else {
                    port + 10_000
                };
                match st.start_handshake(&text(2), port, cport) {
                    Err(true) => error(
                        out,
                        &format!(
                            "ERR Invalid node address specified: {}:{}",
                            text(2),
                            text(3)
                        ),
                    ),
                    _ => out.extend_from_slice(resp::OK),
                }
            }
            ("nodes", 2) => verbatim(out, session, &st.nodes_description(false)),
            ("myid", 2) => resp::write_bulk(out, &st.myself),
            ("myshardid", 2) => resp::write_bulk(out, &st.me().shard_id),
            ("slots", 2) => slots_reply(st, out, session),
            ("shards", 2) => shards_reply(st, out, session),
            ("flushslots", 2) => {
                if !ctx.store.is_empty() {
                    error(out, "ERR DB must be empty to perform CLUSTER FLUSHSLOTS.");
                    return Reply::Written;
                }
                let me = st.myself;
                st.del_node_slots(me);
                st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                out.extend_from_slice(resp::OK);
            }
            ("addslots" | "delslots", n) if n >= 3 => {
                let del = sub == "delslots";
                let mut wanted = vec![0u8; SLOTS];
                let mut slots = Vec::new();
                for i in 2..n {
                    let Some(s) = slot_of(args.get(i), out) else {
                        return Reply::Written;
                    };
                    slots.push(s);
                }
                for s in slots {
                    if !check_slot(st, &mut wanted, del, s, s, out) {
                        return Reply::Written;
                    }
                }
                update_slots(st, &wanted, del);
                st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                out.extend_from_slice(resp::OK);
            }
            ("addslotsrange" | "delslotsrange", n) if n >= 4 => {
                if n % 2 == 1 {
                    error(
                        out,
                        &format!("ERR wrong number of arguments for 'cluster|{sub}' command"),
                    );
                    return Reply::Written;
                }
                let del = sub == "delslotsrange";
                let mut wanted = vec![0u8; SLOTS];
                for i in (2..n).step_by(2) {
                    let Some(a) = slot_of(args.get(i), out) else {
                        return Reply::Written;
                    };
                    let Some(b) = slot_of(args.get(i + 1), out) else {
                        return Reply::Written;
                    };
                    if a > b {
                        error(
                            out,
                            &format!(
                                "ERR start slot number {a} is greater than end slot number {b}"
                            ),
                        );
                        return Reply::Written;
                    }
                    if !check_slot(st, &mut wanted, del, a, b, out) {
                        return Reply::Written;
                    }
                }
                update_slots(st, &wanted, del);
                st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                out.extend_from_slice(resp::OK);
            }
            ("setslot", n) if n >= 4 => {
                if !setslot(ctx, st, args, out) {
                    return Reply::Written;
                }
                st.todo(state::TODO_SAVE_CONFIG | state::TODO_UPDATE_STATE);
                out.extend_from_slice(resp::OK);
            }
            ("bumpepoch", 2) => {
                let bumped = st.bump_epoch();
                out.extend_from_slice(
                    format!(
                        "+{} {}\r\n",
                        if bumped { "BUMPED" } else { "STILL" },
                        st.me().config_epoch
                    )
                    .as_bytes(),
                );
            }
            ("info", 2) => verbatim(out, session, &st.info()),
            ("saveconfig", 2) => match st.save_config() {
                Ok(()) => out.extend_from_slice(resp::OK),
                Err(e) => error(
                    out,
                    &format!("ERR error saving the cluster node config: {e}"),
                ),
            },
            ("keyslot", 3) => resp::write_integer(out, i64::from(super::key_slot(args.get(2)))),
            ("countkeysinslot", 3) => {
                let Some(slot) = resp::parse_int(args.get(2)) else {
                    resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                    return Reply::Written;
                };
                if !(0..SLOTS as i64).contains(&slot) {
                    error(out, "ERR Invalid slot");
                    return Reply::Written;
                }
                resp::write_integer(out, super::count_keys_in_slot(ctx, slot as u16) as i64);
            }
            ("getkeysinslot", 4) => {
                let (Some(slot), Some(max)) =
                    (resp::parse_int(args.get(2)), resp::parse_int(args.get(3)))
                else {
                    resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                    return Reply::Written;
                };
                if !(0..SLOTS as i64).contains(&slot) || max < 0 {
                    error(out, "ERR Invalid slot or number of keys");
                    return Reply::Written;
                }
                let keys = super::keys_in_slot(ctx, slot as u16, max as usize);
                resp::write_array_header(out, keys.len());
                for k in keys {
                    resp::write_bulk(out, &k);
                }
            }
            ("forget", 3) => {
                let Some(node) = st.lookup(args.get(2)).map(|n| n.name) else {
                    if st.blacklisted(args.get(2)) {
                        out.extend_from_slice(resp::OK);
                    } else {
                        error(out, &format!("ERR Unknown node {}", text(2)));
                    }
                    return Reply::Written;
                };
                if node == st.myself {
                    error(out, "ERR I tried hard but I can't forget myself...");
                    return Reply::Written;
                }
                if st.me().is_slave() && st.me().slaveof == Some(node) {
                    error(out, "ERR Can't forget my master!");
                    return Reply::Written;
                }
                st.blacklist_add(node);
                st.del_node(node);
                st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                out.extend_from_slice(resp::OK);
            }
            ("replicate", 3) => {
                let Some(node) = st.lookup(args.get(2)).map(|n| (n.name, n.is_slave())) else {
                    error(out, &format!("ERR Unknown node {}", text(2)));
                    return Reply::Written;
                };
                if node.0 == st.myself {
                    error(out, "ERR Can't replicate myself");
                    return Reply::Written;
                }
                if node.1 {
                    error(out, "ERR I can only replicate a master, not a replica.");
                    return Reply::Written;
                }
                if st.me().is_master() && (st.me().numslots != 0 || !ctx.store.is_empty()) {
                    error(
                        out,
                        "ERR To set a master the node must be empty and without assigned slots.",
                    );
                    return Reply::Written;
                }
                st.set_master(node.0);
                st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                out.extend_from_slice(resp::OK);
            }
            ("slaves" | "replicas", 3) => {
                let Some(node) = st.lookup(args.get(2)) else {
                    error(out, &format!("ERR Unknown node {}", text(2)));
                    return Reply::Written;
                };
                if node.is_slave() {
                    error(out, "ERR The specified node is not a master");
                    return Reply::Written;
                }
                let replicas: Vec<String> = node
                    .replicas
                    .iter()
                    .filter_map(|r| st.nodes.get(r))
                    .map(|r| st.describe(r, false))
                    .collect();
                resp::write_array_header(out, replicas.len());
                for r in replicas {
                    resp::write_bulk(out, r.as_bytes());
                }
            }
            ("count-failure-reports", 3) => {
                let Some(node) = st.lookup(args.get(2)).map(|n| n.name) else {
                    error(out, &format!("ERR Unknown node {}", text(2)));
                    return Reply::Written;
                };
                resp::write_integer(out, st.failure_reports(node) as i64);
            }
            ("failover", 2 | 3) => {
                let (mut force, mut takeover) = (false, false);
                if n == 3 {
                    match text(2).to_ascii_lowercase().as_str() {
                        "force" => force = true,
                        "takeover" => {
                            takeover = true;
                            force = true;
                        }
                        _ => {
                            resp::write_error(out, crate::cmd::SYNTAX_ERROR);
                            return Reply::Written;
                        }
                    }
                }
                let me = st.me();
                if me.is_master() {
                    error(out, "ERR You should send CLUSTER FAILOVER to a replica");
                    return Reply::Written;
                }
                let Some(master) = me.slaveof.and_then(|m| st.nodes.get(&m)) else {
                    error(out, "ERR I'm a replica but my master is unknown to me");
                    return Reply::Written;
                };
                if !force && (master.failed() || master.link.is_none()) {
                    error(
                        out,
                        "ERR Master is down or failed, please use CLUSTER FAILOVER FORCE",
                    );
                    return Reply::Written;
                }
                st.start_manual_failover(force, takeover);
                out.extend_from_slice(resp::OK);
            }
            ("set-config-epoch", 3) => {
                let Some(epoch) = resp::parse_int(args.get(2)) else {
                    resp::write_error(out, crate::cmd::NOT_AN_INTEGER);
                    return Reply::Written;
                };
                if epoch < 0 {
                    error(out, &format!("ERR Invalid config epoch specified: {epoch}"));
                } else if st.nodes.len() > 1 {
                    error(
                        out,
                        "ERR The user can assign a config epoch only when the node does not know any other node.",
                    );
                } else if st.me().config_epoch != 0 {
                    error(out, "ERR Node config epoch is already non-zero");
                } else {
                    let me = st.myself;
                    st.nodes.get_mut(&me).expect("myself").config_epoch = epoch as u64;
                    if st.current_epoch < epoch as u64 {
                        st.current_epoch = epoch as u64;
                    }
                    st.todo(state::TODO_UPDATE_STATE | state::TODO_SAVE_CONFIG);
                    out.extend_from_slice(resp::OK);
                }
            }
            ("reset", 2 | 3) => {
                let hard = match (n, text(2).to_ascii_lowercase().as_str()) {
                    (2, _) | (3, "soft") => false,
                    (3, "hard") => true,
                    _ => {
                        resp::write_error(out, crate::cmd::SYNTAX_ERROR);
                        return Reply::Written;
                    }
                };
                if st.me().is_master() && !ctx.store.is_empty() {
                    error(
                        out,
                        "ERR CLUSTER RESET can't be called with master nodes containing keys",
                    );
                    return Reply::Written;
                }
                st.reset(hard);
                out.extend_from_slice(resp::OK);
            }
            ("links", 2) => links_reply(st, out, session),
            _ => {
                if matches!(
                    sub.as_str(),
                    "help"
                        | "meet"
                        | "nodes"
                        | "myid"
                        | "myshardid"
                        | "slots"
                        | "shards"
                        | "flushslots"
                        | "addslots"
                        | "delslots"
                        | "addslotsrange"
                        | "delslotsrange"
                        | "setslot"
                        | "bumpepoch"
                        | "info"
                        | "saveconfig"
                        | "keyslot"
                        | "countkeysinslot"
                        | "getkeysinslot"
                        | "forget"
                        | "replicate"
                        | "slaves"
                        | "replicas"
                        | "count-failure-reports"
                        | "failover"
                        | "set-config-epoch"
                        | "reset"
                        | "links"
                ) {
                    arity(out, args.get(1));
                } else {
                    error(
                        out,
                        &format!(
                            "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
                            String::from_utf8_lossy(args.get(1))
                        ),
                    );
                }
            }
        }
        st.before_sleep();
    }
    bus::after_command(&cluster);
    Reply::Written
}

/// `checkSlotAssignmentsOrReply`.
fn check_slot(
    st: &State,
    wanted: &mut [u8],
    del: bool,
    a: usize,
    b: usize,
    out: &mut Vec<u8>,
) -> bool {
    for slot in a..=b {
        if del && st.slots[slot].is_none() {
            error(out, &format!("ERR Slot {slot} is already unassigned"));
            return false;
        } else if !del && st.slots[slot].is_some() {
            error(out, &format!("ERR Slot {slot} is already busy"));
            return false;
        }
        wanted[slot] += 1;
        if wanted[slot] == 2 {
            error(out, &format!("ERR Slot {slot} specified multiple times"));
            return false;
        }
    }
    true
}

fn update_slots(st: &mut State, wanted: &[u8], del: bool) {
    let me = st.myself;
    for (slot, w) in wanted.iter().enumerate() {
        if *w != 0 {
            st.importing[slot] = None;
            if del {
                st.del_slot(slot);
            } else {
                st.add_slot(me, slot);
            }
        }
    }
}

fn setslot(ctx: &Context, st: &mut State, args: &Args<'_>, out: &mut Vec<u8>) -> bool {
    let n = args.len();
    let text = |i: usize| String::from_utf8_lossy(args.get(i)).into_owned();
    if st.me().is_slave() {
        error(out, "ERR Please use SETSLOT only with masters.");
        return false;
    }
    let Some(slot) = slot_of(args.get(2), out) else {
        return false;
    };
    let me = st.myself;
    let action = text(3).to_ascii_lowercase();
    let target = |st: &State, out: &mut Vec<u8>, unknown: &str| -> Option<wire::Name> {
        let Some(node) = st.lookup(args.get(4)) else {
            error(out, &format!("ERR {unknown} {}", text(4)));
            return None;
        };
        if node.is_slave() {
            error(out, "ERR Target node is not a master");
            return None;
        }
        Some(node.name)
    };
    match (action.as_str(), n) {
        ("migrating", 5) => {
            if st.slots[slot] != Some(me) {
                error(out, &format!("ERR I'm not the owner of hash slot {slot}"));
                return false;
            }
            let Some(t) = target(st, out, "I don't know about node") else {
                return false;
            };
            st.migrating[slot] = Some(t);
        }
        ("importing", 5) => {
            if st.slots[slot] == Some(me) {
                error(
                    out,
                    &format!("ERR I'm already the owner of hash slot {slot}"),
                );
                return false;
            }
            let Some(t) = target(st, out, "I don't know about node") else {
                return false;
            };
            st.importing[slot] = Some(t);
        }
        ("stable", 4) => {
            st.importing[slot] = None;
            st.migrating[slot] = None;
        }
        ("node", 5) => {
            let Some(t) = target(st, out, "Unknown node") else {
                return false;
            };
            let keys = super::count_keys_in_slot(ctx, slot as u16);
            if st.slots[slot] == Some(me) && t != me && keys != 0 {
                error(
                    out,
                    &format!(
                        "ERR Can't assign hashslot {slot} to a different node while I still hold keys for this hash slot."
                    ),
                );
                return false;
            }
            if keys == 0 && st.migrating[slot].is_some() {
                st.migrating[slot] = None;
            }
            let was_mine = st.slots[slot] == Some(me);
            st.del_slot(slot);
            st.add_slot(t, slot);
            if was_mine && t != me && st.me().numslots == 0 && st.settings.allow_replica_migration {
                state::log(&format!(
                    "Configuration change detected. Reconfiguring myself as a replica of {}",
                    name_str(&t)
                ));
                st.set_master(t);
                st.todo(
                    state::TODO_SAVE_CONFIG | state::TODO_UPDATE_STATE | state::TODO_FSYNC_CONFIG,
                );
            }
            if t == me && st.importing[slot].is_some() {
                if st.bump_epoch() {
                    state::log(&format!("configEpoch updated after importing slot {slot}"));
                }
                st.importing[slot] = None;
                st.broadcast_pong(false);
            }
        }
        _ => {
            error(
                out,
                "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP",
            );
            return false;
        }
    }
    true
}

/// `addNodeToNodeReply` (with `cluster-preferred-endpoint-type ip`).
fn node_entry(st: &State, node: &state::Node, out: &mut Vec<u8>, session: &Session) {
    resp::write_array_header(out, 4);
    resp::write_bulk(out, node.ip.as_bytes());
    resp::write_integer(out, i64::from(node.tcp_port));
    resp::write_bulk(out, &node.name);
    let extra = usize::from(!node.hostname.is_empty());
    map_header(out, session, extra);
    if extra > 0 {
        resp::write_bulk(out, b"hostname");
        resp::write_bulk(out, node.hostname.as_bytes());
    }
    let _ = st;
}

/// `isReplicaAvailable`.
fn replica_available(st: &State, node: &state::Node) -> bool {
    if node.failed() {
        return false;
    }
    let offset = if node.flags & MYSELF != 0 {
        st.repl.offset
    } else {
        node.repl_offset
    };
    offset != 0
}

/// Runs of slots with one owner: `(start, end, owner)`.
fn slot_runs(st: &State) -> Vec<(usize, usize, wire::Name)> {
    let mut runs = Vec::new();
    let mut j = 0;
    while j < SLOTS {
        let Some(owner) = st.slots[j] else {
            j += 1;
            continue;
        };
        let start = j;
        while j < SLOTS && st.slots[j] == Some(owner) {
            j += 1;
        }
        runs.push((start, j - 1, owner));
    }
    runs
}

/// `clusterReplyMultiBulkSlots`.
fn slots_reply(st: &State, out: &mut Vec<u8>, session: &Session) {
    let runs = slot_runs(st);
    resp::write_array_header(out, runs.len());
    for (a, b, owner) in runs {
        let Some(node) = st.nodes.get(&owner) else {
            resp::write_array_header(out, 2);
            resp::write_integer(out, a as i64);
            resp::write_integer(out, b as i64);
            continue;
        };
        let replicas: Vec<&state::Node> = node
            .replicas
            .iter()
            .filter_map(|r| st.nodes.get(r))
            .filter(|r| replica_available(st, r))
            .collect();
        resp::write_array_header(out, 3 + replicas.len());
        resp::write_integer(out, a as i64);
        resp::write_integer(out, b as i64);
        node_entry(st, node, out, session);
        for r in replicas {
            node_entry(st, r, out, session);
        }
    }
}

/// `clusterReplyShards`.
fn shards_reply(st: &State, out: &mut Vec<u8>, session: &Session) {
    let mut shards: Vec<(wire::Name, Vec<&state::Node>)> = Vec::new();
    for node in st.nodes.values() {
        match shards.iter_mut().find(|(id, _)| *id == node.shard_id) {
            Some((_, nodes)) => nodes.push(node),
            None => shards.push((node.shard_id, vec![node])),
        }
    }
    let runs = slot_runs(st);
    resp::write_array_header(out, shards.len());
    for (_, nodes) in shards {
        let first = nodes[0];
        let master = first
            .slaveof
            .and_then(|m| st.nodes.get(&m))
            .filter(|_| first.is_slave())
            .unwrap_or(first);
        map_header(out, session, 2);
        resp::write_bulk(out, b"slots");
        let mine: Vec<(usize, usize)> = runs
            .iter()
            .filter(|(_, _, o)| *o == master.name)
            .map(|(a, b, _)| (*a, *b))
            .collect();
        resp::write_array_header(out, mine.len() * 2);
        for (a, b) in mine {
            resp::write_integer(out, a as i64);
            resp::write_integer(out, b as i64);
        }
        resp::write_bulk(out, b"nodes");
        resp::write_array_header(out, nodes.len());
        for node in nodes {
            let offset = if node.flags & MYSELF != 0 {
                st.repl.offset
            } else {
                node.repl_offset
            };
            let mut fields: Vec<(&str, Field)> =
                vec![("id", Field::Text(state::name_str(&node.name).into()))];
            if node.tcp_port != 0 {
                fields.push(("port", Field::Int(i64::from(node.tcp_port))));
            }
            if node.tls_port != 0 {
                fields.push(("tls-port", Field::Int(i64::from(node.tls_port))));
            }
            fields.push(("ip", Field::Text(node.ip.clone())));
            fields.push(("endpoint", Field::Text(node.ip.clone())));
            if !node.hostname.is_empty() {
                fields.push(("hostname", Field::Text(node.hostname.clone())));
            }
            fields.push((
                "role",
                Field::Text(if node.is_slave() { "replica" } else { "master" }.into()),
            ));
            fields.push(("replication-offset", Field::Int(offset as i64)));
            let health = if node.failed() {
                "fail"
            } else if node.is_slave() && offset == 0 {
                "loading"
            } else {
                "online"
            };
            fields.push(("health", Field::Text(health.into())));
            map_header(out, session, fields.len());
            for (k, v) in fields {
                resp::write_bulk(out, k.as_bytes());
                match v {
                    Field::Text(s) => resp::write_bulk(out, s.as_bytes()),
                    Field::Int(i) => resp::write_integer(out, i),
                }
            }
        }
    }
}

enum Field {
    Text(String),
    Int(i64),
}

/// `addReplyClusterLinksDescription`.
fn links_reply(st: &State, out: &mut Vec<u8>, session: &Session) {
    let links: Vec<(&u64, &state::Link)> =
        st.links.iter().filter(|(_, l)| l.node.is_some()).collect();
    resp::write_array_header(out, links.len());
    for (_, link) in links {
        map_header(out, session, 6);
        resp::write_bulk(out, b"direction");
        resp::write_bulk(out, if link.inbound { b"from" } else { b"to" });
        resp::write_bulk(out, b"node");
        resp::write_bulk(out, &link.node.expect("filtered"));
        resp::write_bulk(out, b"create-time");
        resp::write_integer(out, link.ctime);
        resp::write_bulk(out, b"events");
        resp::write_bulk(out, b"rw");
        resp::write_bulk(out, b"send-buffer-allocated");
        resp::write_integer(out, 0);
        resp::write_bulk(out, b"send-buffer-used");
        resp::write_integer(out, 0);
    }
}
