//! Users, and what each may run: Redis 7.2's ACLs, with its rules, how it
//! describes them, its checks and its log.
//!
//! A user has passwords and selectors: the root one, and any added in
//! parentheses. A selector says which commands (a bit per command and
//! subcommand of Redis 7.2.4, from `acl_commands.txt`, so that rules name the
//! commands Redis has, those Tantalum lacks too), keys and channels it allows;
//! a command runs if one of them allows it. Users are kept whole and replaced
//! when they change (`ACL SETUSER`), so a connection reads the one it holds
//! without a lock, and takes up the new one at its next batch of commands
//! (`Context::follow_special`, whose word counts changes here).
//!
//! None of this costs a connection whose user may run everything, as the
//! `default` user may unless told otherwise: its session stays
//! `unrestricted`, and the fast path never looks here. The others' commands
//! are checked on the restricted path.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use crate::cmd::Args;
use crate::command_table::{self, KeySpecs};
use crate::glob;

static TABLE: &str = include_str!("acl_commands.txt");

/// The categories, in Redis' order (`ACL CAT`, and how `COMMAND` lists them).
pub(crate) const CATEGORIES: [&str; 21] = [
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// Words in a selector's command bitmap: a bit per command, and the last for
/// commands to come, which only `+@all` sets (Redis' trick to tell `+@all
/// -get` from a list of everything else).
const WORDS: usize = 6;
const FUTURE: usize = WORDS * 64 - 1;
const NO_PARENT: u16 = u16::MAX;

/// Redis 7.2.4's commands and subcommands, by id.
struct Commands {
    names: Vec<&'static str>,
    categories: Vec<u32>,
    no_auth: Vec<bool>,
    parent: Vec<u16>,
    subcommands: Vec<Vec<u16>>,
    /// (Its keys are Redis' fixed names, so a fast hash will do.)
    ids: HashMap<&'static str, u16, foldhash::fast::FixedState>,
}

fn commands() -> &'static Commands {
    static COMMANDS: OnceLock<Commands> = OnceLock::new();
    COMMANDS.get_or_init(|| {
        let mut table = Commands {
            names: Vec::new(),
            categories: Vec::new(),
            no_auth: Vec::new(),
            parent: Vec::new(),
            subcommands: Vec::new(),
            ids: HashMap::default(),
        };
        for line in TABLE.lines().filter(|l| !l.starts_with('#')) {
            let mut fields = line.split(' ');
            let name = fields.next().expect("a name");
            let categories = fields.next().unwrap_or("");
            let id = table.names.len() as u16;
            table.names.push(name);
            table.categories.push(
                categories
                    .split(',')
                    .filter_map(|c| CATEGORIES.iter().position(|&known| known == c))
                    .fold(0, |bits, i| bits | 1 << i),
            );
            table.no_auth.push(fields.next() == Some("no_auth"));
            table.parent.push(NO_PARENT);
            table.subcommands.push(Vec::new());
            table.ids.insert(name, id);
        }
        assert!(table.names.len() < FUTURE, "more commands than bits");
        for id in 0..table.names.len() {
            if let Some((parent, _)) = table.names[id].split_once('|') {
                let parent = table.ids[parent];
                table.parent[id] = parent;
                table.subcommands[parent as usize].push(id as u16);
            }
        }
        table
    })
}

/// A command (`name`) or subcommand (`name|sub`), as Redis'
/// `lookupCommandBySdsLogic` finds it: no case, one level of subcommands, and
/// a subcommand only of a command that has them.
fn lookup(name: &[u8]) -> Option<u16> {
    let name = std::str::from_utf8(name).ok()?.to_ascii_lowercase();
    let table = commands();
    let mut parts = name.split('|');
    let base = *table.ids.get(parts.next()?)?;
    match (parts.next(), parts.next()) {
        (None, _) if table.parent[base as usize] == NO_PARENT => Some(base),
        (Some(_), None) if !table.subcommands[base as usize].is_empty() => {
            table.ids.get(name.as_str()).copied()
        }
        _ => None,
    }
}

/// A category's bit, from its name in any case.
fn category(name: &[u8]) -> Option<u32> {
    CATEGORIES
        .iter()
        .position(|c| c.as_bytes().eq_ignore_ascii_case(name))
        .map(|i| 1 << i)
}

/// The commands in a category, as `ACL CAT` lists them (`None` for no such
/// category).
pub(crate) fn commands_in(name: &[u8]) -> Option<Vec<&'static str>> {
    let bit = category(name)?;
    let table = commands();
    Some(
        (0..table.names.len())
            .filter(|&id| table.categories[id] & bit != 0)
            .map(|id| table.names[id])
            .collect(),
    )
}

/// Why a rule didn't take: Redis' errno, and its message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuleError {
    Syntax,
    Unknown,
    AfterAllKeys,
    AfterAllChannels,
    NoSuchPassword,
    BadHash,
    SubcommandArg,
}

impl RuleError {
    pub(crate) fn message(self) -> &'static str {
        match self {
            RuleError::Unknown => "Unknown command or category name in ACL",
            RuleError::Syntax => "Syntax error",
            RuleError::AfterAllKeys => {
                "Adding a pattern after the * pattern (or the 'allkeys' flag) is not valid and \
                 does not have any effect. Try 'resetkeys' to start with an empty list of patterns"
            }
            RuleError::AfterAllChannels => {
                "Adding a pattern after the * pattern (or the 'allchannels' flag) is not valid \
                 and does not have any effect. Try 'resetchannels' to start with an empty list \
                 of channels"
            }
            RuleError::NoSuchPassword => {
                "The password you are trying to remove from the user does not exist"
            }
            RuleError::BadHash => {
                "The password hash must be exactly 64 characters and contain only lowercase \
                 hexadecimal characters"
            }
            RuleError::SubcommandArg => "Allowing first-arg of a subcommand is not supported",
        }
    }
}

/// Key pattern permissions.
const READ: u8 = 1;
const WRITE: u8 = 2;
const ALL: u8 = READ | WRITE;

/// Key spec flags (Redis' `CMD_KEY_*`), as `command_table` compiles them.
pub(crate) const KEY_ACCESS: u16 = 1 << 4;
pub(crate) const KEY_UPDATE: u16 = 1 << 5;
pub(crate) const KEY_INSERT: u16 = 1 << 6;
pub(crate) const KEY_DELETE: u16 = 1 << 7;

/// Whitespace or a zero byte, which names and patterns can't have.
fn has_spaces(s: &[u8]) -> bool {
    s.iter()
        .any(|&b| matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0))
}

#[derive(Clone)]
struct KeyPattern {
    flags: u8,
    pattern: Box<[u8]>,
}

impl KeyPattern {
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(match self.flags {
            ALL => b"~",
            READ => b"%R~",
            WRITE => b"%W~",
            _ => b"%~",
        });
        out.extend_from_slice(&self.pattern);
    }
}

/// Commands, keys and channels a user may use: its root selector, or one
/// added in parentheses.
#[derive(Clone)]
struct Selector {
    all_keys: bool,
    all_channels: bool,
    all_commands: bool,
    bits: [u64; WORDS],
    /// Commands allowed only with one of these first arguments (`+select|0`).
    first_args: Vec<(u16, Vec<Box<[u8]>>)>,
    patterns: Vec<KeyPattern>,
    channels: Vec<Box<[u8]>>,
    /// The command rules in the order given, each lowercase with whether it
    /// allows, so they're described as they were set (`command_rules`).
    rules: Vec<(bool, Box<[u8]>)>,
}

impl Selector {
    fn new(all_channels: bool) -> Self {
        Selector {
            all_keys: false,
            all_channels,
            all_commands: false,
            bits: [0; WORDS],
            first_args: Vec::new(),
            patterns: Vec::new(),
            channels: Vec::new(),
            rules: Vec::new(),
        }
    }

    fn bit(&self, id: usize) -> bool {
        self.bits[id / 64] & (1 << (id % 64)) != 0
    }

    fn set_bit(&mut self, id: usize, allow: bool) {
        if allow {
            self.bits[id / 64] |= 1 << (id % 64);
        } else {
            self.bits[id / 64] &= !(1 << (id % 64));
            self.all_commands = false;
        }
    }

    /// Allows or blocks a command, and a container's subcommands with it.
    fn change(&mut self, id: u16, allow: bool) {
        self.set_bit(id as usize, allow);
        self.first_args.retain(|(of, _)| *of != id);
        for &sub in &commands().subcommands[id as usize] {
            self.set_bit(sub as usize, allow);
        }
    }

    /// Redis' `ACLUpdateCommandRules`: the rule replaces the same one, and a
    /// command's replaces its subcommands'.
    fn note_rule(&mut self, rule: &[u8], allow: bool) {
        let rule = rule.to_ascii_lowercase();
        self.rules.retain(|(_, old)| {
            !(**old == *rule
                || (old.len() > rule.len() && old.starts_with(&rule) && old[rule.len()] == b'|'))
        });
        self.rules.push((allow, rule.into()));
    }

    /// Redis' `ACLSetSelector`.
    fn apply(&mut self, op: &[u8]) -> Result<(), RuleError> {
        let is = |name: &str| op.eq_ignore_ascii_case(name.as_bytes());
        let first = op.first().copied().unwrap_or(0);
        let second = op.get(1).copied().unwrap_or(0);
        if is("allkeys") || is("~*") {
            self.all_keys = true;
            self.patterns.clear();
        } else if is("resetkeys") {
            self.all_keys = false;
            self.patterns.clear();
        } else if is("allchannels") || is("&*") {
            self.all_channels = true;
            self.channels.clear();
        } else if is("resetchannels") {
            self.all_channels = false;
            self.channels.clear();
        } else if is("allcommands") || is("+@all") {
            self.bits = [u64::MAX; WORDS];
            self.all_commands = true;
            self.rules.clear();
            self.first_args.clear();
        } else if is("nocommands") || is("-@all") {
            self.bits = [0; WORDS];
            self.all_commands = false;
            self.rules.clear();
            self.first_args.clear();
        } else if first == b'~' || first == b'%' {
            if self.all_keys {
                return Err(RuleError::AfterAllKeys);
            }
            let (mut flags, mut offset) = (0, 1);
            if first == b'%' {
                while offset < op.len() {
                    let c = op[offset].to_ascii_uppercase();
                    if c == b'R' && flags & READ == 0 {
                        flags |= READ;
                    } else if c == b'W' && flags & WRITE == 0 {
                        flags |= WRITE;
                    } else if op[offset] == b'~' {
                        offset += 1;
                        break;
                    } else {
                        return Err(RuleError::Syntax);
                    }
                    offset += 1;
                }
            } else {
                flags = ALL;
            }
            let pattern = &op[offset..];
            if has_spaces(pattern) {
                return Err(RuleError::Syntax);
            }
            match self.patterns.iter_mut().find(|p| *p.pattern == *pattern) {
                Some(known) => known.flags |= flags,
                None => self.patterns.push(KeyPattern {
                    flags,
                    pattern: pattern.into(),
                }),
            }
            self.all_keys = false;
        } else if first == b'&' {
            if self.all_channels {
                return Err(RuleError::AfterAllChannels);
            }
            let pattern = &op[1..];
            if has_spaces(pattern) {
                return Err(RuleError::Syntax);
            }
            if !self.channels.iter().any(|c| **c == *pattern) {
                self.channels.push(pattern.into());
            }
            self.all_channels = false;
        } else if first == b'+' && second != b'@' {
            let rest = &op[1..];
            match memchr::memrchr(b'|', rest) {
                None => {
                    let id = lookup(rest).ok_or(RuleError::Unknown)?;
                    self.change(id, true);
                    self.note_rule(commands().names[id as usize].as_bytes(), true);
                }
                Some(bar) => {
                    let (base, sub) = (&rest[..bar], &rest[bar + 1..]);
                    let id = lookup(base).ok_or(RuleError::Unknown)?;
                    let table = commands();
                    if table.parent[id as usize] != NO_PARENT {
                        return Err(RuleError::SubcommandArg);
                    }
                    if sub.is_empty() {
                        return Err(RuleError::Syntax);
                    }
                    if !table.subcommands[id as usize].is_empty() {
                        let sub = lookup(rest).ok_or(RuleError::Unknown)?;
                        self.change(sub, true);
                    } else {
                        // Redis warns that this misuses ACLs, and allows it.
                        match self.first_args.iter_mut().find(|(of, _)| *of == id) {
                            Some((_, args)) => {
                                if !args.iter().any(|a| a.eq_ignore_ascii_case(sub)) {
                                    args.push(sub.into());
                                }
                            }
                            None => self.first_args.push((id, vec![sub.into()])),
                        }
                    }
                    self.note_rule(rest, true);
                }
            }
        } else if first == b'-' && second != b'@' {
            let id = lookup(&op[1..]).ok_or(RuleError::Unknown)?;
            self.change(id, false);
            self.note_rule(commands().names[id as usize].as_bytes(), false);
        } else if (first == b'+' || first == b'-') && second == b'@' {
            let bit = category(&op[2..]).ok_or(RuleError::Unknown)?;
            let allow = first == b'+';
            self.note_rule(&op[1..], allow);
            let table = commands();
            for (id, categories) in table.categories.iter().enumerate().take(table.names.len()) {
                if categories & bit != 0 {
                    self.change(id as u16, allow);
                }
            }
        } else {
            return Err(RuleError::Syntax);
        }
        Ok(())
    }

    /// The command rules, as `ACL GETUSER` and `ACL LIST` show them.
    fn describe_commands(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(if self.bit(FUTURE) { b"+@all" } else { b"-@all" });
        for (allow, rule) in &self.rules {
            out.push(b' ');
            out.push(if *allow { b'+' } else { b'-' });
            out.extend_from_slice(rule);
        }
    }

    /// Redis' `ACLDescribeSelector`: keys, channels, then commands.
    fn describe(&self, out: &mut Vec<u8>) {
        if self.all_keys {
            out.extend_from_slice(b"~* ");
        } else {
            for pattern in &self.patterns {
                pattern.write(out);
                out.push(b' ');
            }
        }
        if self.all_channels {
            out.extend_from_slice(b"&* ");
        } else {
            out.extend_from_slice(b"resetchannels ");
            for channel in &self.channels {
                out.push(b'&');
                out.extend_from_slice(channel);
                out.push(b' ');
            }
        }
        self.describe_commands(out);
    }

    fn keys_text(&self) -> Vec<u8> {
        if self.all_keys {
            return b"~*".to_vec();
        }
        let mut out = Vec::new();
        for (i, pattern) in self.patterns.iter().enumerate() {
            if i > 0 {
                out.push(b' ');
            }
            pattern.write(&mut out);
        }
        out
    }

    fn channels_text(&self) -> Vec<u8> {
        if self.all_channels {
            return b"&*".to_vec();
        }
        let mut out = Vec::new();
        for (i, channel) in self.channels.iter().enumerate() {
            if i > 0 {
                out.push(b' ');
            }
            out.push(b'&');
            out.extend_from_slice(channel);
        }
        out
    }

    /// Redis' `ACLSelectorCheckKey`.
    fn may_use_key(&self, key: &[u8], spec_flags: u16) -> bool {
        if self.all_keys {
            return true;
        }
        let wanted = access_of(spec_flags);
        self.patterns
            .iter()
            .any(|p| p.flags & wanted == wanted && glob::matches(&p.pattern, key, false))
    }

    /// Redis' `ACLSelectorHasUnrestrictedKeyAccess`.
    fn has_every_key(&self, spec_flags: u16) -> bool {
        let wanted = access_of(spec_flags);
        self.all_keys
            || self
                .patterns
                .iter()
                .any(|p| p.flags & wanted == wanted && &*p.pattern == b"*")
    }

    /// Redis' `ACLSelectorCheckCmd`.
    fn check(&self, target: &Target, args: &Args<'_>) -> Result<(), Denied> {
        let id = target.id as usize;
        if !self.all_commands && !commands().no_auth[id] && !self.bit(id) {
            let allowed = args.len() >= 2
                && self.first_args.iter().any(|(of, firsts)| {
                    *of as usize == id && firsts.iter().any(|f| f.eq_ignore_ascii_case(args.get(1)))
                });
            if !allowed {
                return Err(Denied::Command);
            }
        }
        if !self.all_keys
            && let Some(specs) = target.keys
        {
            let mut denied = None;
            specs.each_key(args, |at, flags| {
                if self.may_use_key(args.get(at), flags) {
                    true
                } else {
                    denied = Some(at);
                    false
                }
            });
            if let Some(at) = denied {
                return Err(Denied::Key(at));
            }
        }
        if !self.all_channels
            && let Some((pattern, count)) = target.channels
        {
            let end = count.map_or(args.len(), |n| (1 + n).min(args.len()));
            for at in 1..end {
                let channel = args.get(at);
                let allowed = self.channels.iter().any(|known| {
                    if pattern {
                        **known == *channel
                    } else {
                        glob::matches(known, channel, false)
                    }
                });
                if !allowed {
                    return Err(Denied::Channel(at));
                }
            }
        }
        Ok(())
    }
}

/// The key access a key spec's flags ask for.
fn access_of(spec_flags: u16) -> u8 {
    let mut wanted = 0;
    if spec_flags & KEY_ACCESS != 0 {
        wanted |= READ;
    }
    if spec_flags & (KEY_INSERT | KEY_DELETE | KEY_UPDATE) != 0 {
        wanted |= WRITE;
    }
    wanted
}

/// User flags, in the order Redis describes them.
const ENABLED: u8 = 1;
const DISABLED: u8 = 2;
const NOPASS: u8 = 4;
const SKIP_SANITIZE: u8 = 8;
const SANITIZE: u8 = 16;
const FLAG_NAMES: [(u8, &str); 5] = [
    (ENABLED, "on"),
    (DISABLED, "off"),
    (NOPASS, "nopass"),
    (SKIP_SANITIZE, "skip-sanitize-payload"),
    (SANITIZE, "sanitize-payload"),
];

/// A user. Never changed once shared: a change makes a new one.
#[derive(Clone)]
pub struct User {
    name: Box<[u8]>,
    /// Which user of this name: a deleted and remade one is another.
    serial: u64,
    flags: u8,
    /// SHA-256 hashes, in hex.
    passwords: Vec<Box<[u8]>>,
    /// The root selector first.
    selectors: Vec<Selector>,
    /// One selector allows every command, key and channel: a connection of
    /// this user needs no checks.
    everything: bool,
    /// Deleted since a connection took it up: that connection closes.
    pub(crate) deleted: bool,
}

impl User {
    fn new(name: &[u8], serial: u64, all_channels: bool) -> Self {
        User {
            name: name.into(),
            serial,
            flags: DISABLED | SANITIZE,
            passwords: Vec::new(),
            selectors: vec![Selector::new(all_channels)],
            everything: false,
            deleted: false,
        }
    }

    /// Redis' default user: on, no password, everything.
    fn default_user() -> Self {
        let mut user = User::new(b"default", 0, false);
        for op in ["+@all", "~*", "&*", "on", "nopass"] {
            user.apply(op.as_bytes(), false).expect("valid");
        }
        user.finish()
    }

    fn finish(mut self) -> Self {
        self.everything = self
            .selectors
            .iter()
            .any(|s| s.all_commands && s.all_keys && s.all_channels);
        self
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Whether every command with any keys and channels is allowed.
    pub fn may_run_everything(&self) -> bool {
        self.everything
    }

    pub(crate) fn enabled(&self) -> bool {
        self.flags & DISABLED == 0
    }

    pub(crate) fn nopass(&self) -> bool {
        self.flags & NOPASS != 0
    }

    /// Redis' `ACLSetUser`.
    fn apply(&mut self, op: &[u8], all_channels: bool) -> Result<(), RuleError> {
        if op.is_empty() {
            return Ok(());
        }
        let is = |name: &str| op.eq_ignore_ascii_case(name.as_bytes());
        let first = op[0];
        if is("on") {
            self.flags = (self.flags | ENABLED) & !DISABLED;
        } else if is("off") {
            self.flags = (self.flags | DISABLED) & !ENABLED;
        } else if is("skip-sanitize-payload") {
            self.flags = (self.flags | SKIP_SANITIZE) & !SANITIZE;
        } else if is("sanitize-payload") {
            self.flags = (self.flags | SANITIZE) & !SKIP_SANITIZE;
        } else if is("nopass") {
            self.flags |= NOPASS;
            self.passwords.clear();
        } else if is("resetpass") {
            self.flags &= !NOPASS;
            self.passwords.clear();
        } else if first == b'>' || first == b'#' {
            let hash = if first == b'>' {
                hash_password(&op[1..])
            } else {
                checked_hash(&op[1..])?
            };
            if !self.passwords.iter().any(|p| **p == *hash) {
                self.passwords.push(hash.into());
            }
            self.flags &= !NOPASS;
        } else if first == b'<' || first == b'!' {
            let hash = if first == b'<' {
                hash_password(&op[1..])
            } else {
                checked_hash(&op[1..])?
            };
            let at = self
                .passwords
                .iter()
                .position(|p| **p == *hash)
                .ok_or(RuleError::NoSuchPassword)?;
            self.passwords.remove(at);
        } else if first == b'(' && op[op.len() - 1] == b')' {
            let mut selector = Selector::new(all_channels);
            for arg in split_args(&op[1..op.len() - 1]).unwrap_or_default() {
                selector.apply(&arg)?;
            }
            self.selectors.push(selector);
        } else if is("clearselectors") {
            self.selectors.truncate(1);
        } else if is("reset") {
            for op in ["resetpass", "resetkeys", "resetchannels"] {
                self.apply(op.as_bytes(), all_channels)?;
            }
            if all_channels {
                self.apply(b"allchannels", all_channels)?;
            }
            for op in ["off", "sanitize-payload", "clearselectors", "-@all"] {
                self.apply(op.as_bytes(), all_channels)?;
            }
        } else {
            self.selectors[0].apply(op)?;
        }
        Ok(())
    }

    /// Redis' `ACLDescribeUser`, as `ACL LIST` and the ACL file have it.
    pub(crate) fn describe(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (flag, name) in FLAG_NAMES {
            if self.flags & flag != 0 {
                out.extend_from_slice(name.as_bytes());
                out.push(b' ');
            }
        }
        for password in &self.passwords {
            out.push(b'#');
            out.extend_from_slice(password);
            out.push(b' ');
        }
        for (i, selector) in self.selectors.iter().enumerate() {
            if i == 0 {
                selector.describe(&mut out);
            } else {
                out.extend_from_slice(b" (");
                selector.describe(&mut out);
                out.push(b')');
            }
        }
        out
    }

    fn check_password(&self, password: &[u8]) -> bool {
        if self.nopass() {
            return true;
        }
        let hash = hash_password(password);
        // Every one, all the way: the time taken doesn't tell which matched.
        self.passwords
            .iter()
            .fold(false, |found, p| found | constant_time_eq(p, &hash))
    }

    /// Redis' `ACLCheckAllUserCommandPerm`: allowed if a selector allows it;
    /// otherwise the denial that matters most (a channel, then a key, the
    /// last one no selector allowed, then the command).
    pub(crate) fn check(&self, target: &Target, args: &Args<'_>) -> Result<(), Denied> {
        if self.everything {
            return Ok(());
        }
        let mut worst = Denied::Command;
        for selector in &self.selectors {
            match selector.check(target, args) {
                Ok(()) => return Ok(()),
                Err(denied) if denied.rank() > worst.rank() => worst = denied,
                Err(_) => {}
            }
        }
        Err(worst)
    }

    /// Redis' `ACLUserCheckCmdWithUnrestrictedKeyAccess`, for `SORT`'s `BY`
    /// and `GET`: whether a selector that allows the command allows every key
    /// for `spec_flags`'s access too.
    pub(crate) fn may_read_every_key(&self, target: &Target, args: &Args<'_>) -> bool {
        self.everything
            || self
                .selectors
                .iter()
                .any(|s| s.check(target, args).is_ok() && s.has_every_key(KEY_ACCESS))
    }

    fn channel_patterns(&self) -> impl Iterator<Item = &[u8]> {
        self.selectors
            .iter()
            .flat_map(|s| s.channels.iter().map(|c| &**c))
    }
}

/// Why a command was refused, with the argument at fault for a key or channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Denied {
    Command,
    Key(usize),
    Channel(usize),
}

impl Denied {
    /// Redis' `ACL_DENIED_*` codes, and then the later argument, decide which
    /// selector's denial is told.
    fn rank(self) -> (u8, usize) {
        match self {
            Denied::Command => (1, 0),
            Denied::Key(at) => (2, at),
            Denied::Channel(at) => (4, at),
        }
    }

    pub(crate) fn reason(self) -> Reason {
        match self {
            Denied::Command => Reason::Command,
            Denied::Key(_) => Reason::Key,
            Denied::Channel(_) => Reason::Channel,
        }
    }

    /// The argument the log names: the command's full name, or the key or channel.
    pub(crate) fn object(self, target: &Target, args: &Args<'_>) -> Vec<u8> {
        match self {
            Denied::Command => target.name().as_bytes().to_vec(),
            Denied::Key(at) | Denied::Channel(at) => args.get(at).to_vec(),
        }
    }

    /// Redis' `getAclErrorMessage` (`verbose` for `ACL DRYRUN`).
    pub(crate) fn message(
        self,
        user: &User,
        target: &Target,
        args: &Args<'_>,
        verbose: bool,
    ) -> Vec<u8> {
        let name = String::from_utf8_lossy(&user.name);
        match self {
            Denied::Command => format!(
                "User {name} has no permissions to run the '{}' command",
                target.name()
            )
            .into_bytes(),
            Denied::Key(at) if verbose => {
                let mut out =
                    format!("User {name} has no permissions to access the '").into_bytes();
                out.extend_from_slice(args.get(at));
                out.extend_from_slice(b"' key");
                out
            }
            Denied::Channel(at) if verbose => {
                let mut out =
                    format!("User {name} has no permissions to access the '").into_bytes();
                out.extend_from_slice(args.get(at));
                out.extend_from_slice(b"' channel");
                out
            }
            Denied::Key(_) => b"No permissions to access a key".to_vec(),
            Denied::Channel(_) => b"No permissions to access a channel".to_vec(),
        }
    }
}

/// A command as ACLs see it: its id (a container's subcommand's), its keys,
/// and its channels (whether patterns, and how many after the name: `None`
/// for the rest).
pub(crate) struct Target {
    id: u16,
    keys: Option<&'static KeySpecs>,
    channels: Option<(bool, Option<usize>)>,
}

impl Target {
    /// Its key specs, if it has keys.
    pub(crate) fn keys(&self) -> Option<&'static KeySpecs> {
        self.keys
    }

    pub(crate) fn name(&self) -> &'static str {
        commands().names[self.id as usize]
    }
}

/// The id of the command `name` (Tantalum's, lowercase) run with `args`: a
/// container's subcommand's when it names one. `None` for a subcommand it
/// doesn't have.
pub(crate) fn command_id(name: &str, args: &Args<'_>) -> Option<u16> {
    let table = commands();
    let base = *table.ids.get(name)?;
    if !table.subcommands[base as usize].is_empty() && args.len() >= 2 {
        let mut full = String::with_capacity(name.len() + 1 + args.get(1).len());
        full.push_str(name);
        full.push('|');
        full.push_str(&String::from_utf8_lossy(args.get(1)).to_ascii_lowercase());
        table.ids.get(full.as_str()).copied()
    } else {
        Some(base)
    }
}

/// A command's own id (Tantalum's name, lowercase), and whether it has subcommands:
/// for command statistics, which keep it per command.
pub(crate) fn base_id(name: &str) -> Option<(u16, bool)> {
    let table = commands();
    let id = *table.ids.get(name)?;
    Some((id, !table.subcommands[id as usize].is_empty()))
}

/// For command statistics: how many ids there are (commands and subcommands).
pub(crate) fn command_count() -> usize {
    commands().names.len()
}

/// A command's name by id: `config|get` for a subcommand.
pub(crate) fn command_name(id: u16) -> &'static str {
    commands().names[id as usize]
}

/// A command's id from its name, as Redis' `lookupCommandBySds` finds it.
pub(crate) fn find_command_id(name: &[u8]) -> Option<u16> {
    lookup(name)
}

/// A container command's subcommands' ids.
pub(crate) fn subcommand_ids(id: u16) -> &'static [u16] {
    &commands().subcommands[id as usize]
}

/// What ACLs check of the command `name` (Tantalum's, lowercase) run with
/// `args`. `None` for a subcommand it doesn't have: the command replies with
/// its error, as Redis does before any check.
pub(crate) fn target(name: &'static str, args: &Args<'_>) -> Option<Target> {
    let table = commands();
    let id = command_id(name, args)?;
    let full = if table.parent[id as usize] == NO_PARENT {
        name
    } else {
        table.names[id as usize]
    };
    let channels = match name {
        "subscribe" | "ssubscribe" => Some((false, None)),
        "psubscribe" => Some((true, None)),
        "publish" | "spublish" => Some((false, Some(1))),
        _ => None,
    };
    Some(Target {
        id,
        keys: command_table::key_specs(full),
        channels,
    })
}

/// What ACLs say of a command of Redis' that isn't here (`ACL DRYRUN`'s):
/// its rules for the command, as there's nothing to say of its keys.
pub(crate) fn target_elsewhere(args: &Args<'_>) -> Option<Target> {
    let table = commands();
    let base = lookup(args.get(0))?;
    let id = if !table.subcommands[base as usize].is_empty() && args.len() >= 2 {
        let mut full = args.get(0).to_vec();
        full.push(b'|');
        full.extend_from_slice(args.get(1));
        lookup(&full)?
    } else {
        base
    };
    Some(Target {
        id,
        keys: None,
        channels: None,
    })
}

/// Why something was denied, for the log.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Reason {
    Command,
    Key,
    Auth,
    Channel,
}

impl Reason {
    fn name(self) -> &'static str {
        match self {
            Reason::Command => "command",
            Reason::Key => "key",
            Reason::Auth => "auth",
            Reason::Channel => "channel",
        }
    }
}

/// Where a denied command was run.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Where {
    Toplevel,
    Multi,
    Lua,
}

impl Where {
    fn name(self) -> &'static str {
        match self {
            Where::Toplevel => "toplevel",
            Where::Multi => "multi",
            Where::Lua => "lua",
        }
    }
}

/// An `ACL LOG` entry.
pub(crate) struct Entry {
    pub(crate) count: u64,
    pub(crate) reason: Reason,
    pub(crate) context: Where,
    pub(crate) object: Box<[u8]>,
    pub(crate) username: Box<[u8]>,
    /// Milliseconds since the epoch: of the last time, and the first.
    pub(crate) updated: i64,
    pub(crate) created: i64,
    pub(crate) client_info: Vec<u8>,
    pub(crate) id: u64,
}

impl Entry {
    pub(crate) fn reason_name(&self) -> &'static str {
        self.reason.name()
    }

    pub(crate) fn context_name(&self) -> &'static str {
        self.context.name()
    }
}

/// Entries this close together in time, and otherwise the same, are one.
const GROUPING_MILLIS: i64 = 60_000;

#[derive(Default)]
struct Log {
    /// Newest first.
    entries: VecDeque<Entry>,
    next_id: u64,
}

/// Users, the log, and the counts `INFO` shows.
pub struct Acl {
    users: Mutex<BTreeMap<Box<[u8]>, Arc<User>>>,
    next_serial: AtomicU64,
    log: Mutex<Log>,
    log_max: AtomicUsize,
    /// `acl-pubsub-default allchannels`: new users may use every channel.
    all_channels: AtomicBool,
    file: Option<PathBuf>,
    /// Denied: authentications, commands, keys and channels.
    denied: [AtomicU64; 4],
    /// The default user is on and has no password: connections start
    /// authenticated (Redis' `authRequired`).
    open: AtomicBool,
}

/// What `ACL SETUSER` did, for the connections it concerns.
pub(crate) struct Changed {
    pub(crate) old: Option<Arc<User>>,
    pub(crate) new: Arc<User>,
}

impl Acl {
    /// The default user, with `password` if there is one.
    pub fn new(password: Option<&[u8]>, file: Option<PathBuf>) -> Self {
        let acl = Acl {
            users: Mutex::new(BTreeMap::new()),
            next_serial: AtomicU64::new(1),
            log: Mutex::new(Log::default()),
            log_max: AtomicUsize::new(128),
            all_channels: AtomicBool::new(false),
            file,
            denied: Default::default(),
            open: AtomicBool::new(true),
        };
        let user = User::default_user();
        acl.users.lock().insert(user.name.clone(), Arc::new(user));
        acl.set_default_password(password);
        acl
    }

    /// Loads the ACL file, if there's one: at startup, where an error stops
    /// the server (as in Redis).
    pub fn load_at_startup(&self) -> Result<(), String> {
        match &self.file {
            Some(_) => self.load().map(|_| ()),
            None => Ok(()),
        }
    }

    pub(crate) fn file(&self) -> Option<&Path> {
        self.file.as_deref()
    }

    pub(crate) fn get(&self, name: &[u8]) -> Option<Arc<User>> {
        self.users.lock().get(name).cloned()
    }

    pub(crate) fn default_user(&self) -> Arc<User> {
        self.get(b"default").expect("always there")
    }

    /// Whether new connections start authenticated.
    pub fn open(&self) -> bool {
        self.open.load(Relaxed)
    }

    fn note_default(&self, users: &BTreeMap<Box<[u8]>, Arc<User>>) {
        let default = &users[&b"default"[..]];
        self.open
            .store(default.nopass() && default.enabled(), Relaxed);
    }

    /// `requirepass`: the default user's only password, or none needed.
    pub(crate) fn set_default_password(&self, password: Option<&[u8]>) {
        let mut users = self.users.lock();
        let mut user = (*users[&b"default"[..]]).clone();
        let all = self.all_channels.load(Relaxed);
        user.apply(b"resetpass", all).expect("valid");
        match password {
            Some(password) => {
                let mut op = vec![b'>'];
                op.extend_from_slice(password);
                user.apply(&op, all).expect("valid");
            }
            None => user.apply(b"nopass", all).expect("valid"),
        }
        users.insert(user.name.clone(), Arc::new(user.finish()));
        self.note_default(&users);
    }

    pub(crate) fn set_all_channels(&self, all: bool) {
        self.all_channels.store(all, Relaxed);
    }

    pub(crate) fn set_log_max(&self, max: usize) {
        self.log_max.store(max, Relaxed);
    }

    /// The user these credentials are good for (Redis' `ACLCheckUserCredentials`).
    pub(crate) fn authenticate(&self, name: &[u8], password: &[u8]) -> Option<Arc<User>> {
        let user = self.get(name)?;
        (user.enabled() && user.check_password(password)).then_some(user)
    }

    /// `ACL SETUSER`: all of `ops` apply, or none does.
    pub(crate) fn set_user(&self, name: &[u8], ops: &[&[u8]]) -> Result<Changed, Vec<u8>> {
        let ops = merge_selectors(ops).map_err(|at| {
            let mut msg = b"ERR Unmatched parenthesis in acl selector starting at '".to_vec();
            msg.extend_from_slice(ops[at]);
            msg.extend_from_slice(b"'.");
            msg
        })?;
        let all = self.all_channels.load(Relaxed);
        let mut users = self.users.lock();
        let old = users.get(name).cloned();
        let mut user = match &old {
            Some(old) => (**old).clone(),
            None => User::new(name, self.next_serial.fetch_add(1, Relaxed), all),
        };
        for op in &ops {
            if let Err(e) = user.apply(op, all) {
                let mut msg = b"ERR Error in ACL SETUSER modifier '".to_vec();
                msg.extend_from_slice(op);
                msg.extend_from_slice(b"': ");
                msg.extend_from_slice(e.message().as_bytes());
                return Err(msg);
            }
        }
        let new = Arc::new(user.finish());
        users.insert(new.name.clone(), new.clone());
        self.note_default(&users);
        Ok(Changed { old, new })
    }

    /// `ACL DELUSER`: the users there were.
    pub(crate) fn delete(&self, names: &[&[u8]]) -> Vec<Arc<User>> {
        let mut users = self.users.lock();
        names
            .iter()
            .filter_map(|name| users.remove(*name))
            .collect()
    }

    /// Every user, by name.
    pub(crate) fn users(&self) -> Vec<Arc<User>> {
        self.users.lock().values().cloned().collect()
    }

    /// Notes a denial, for `INFO`, and for `ACL LOG` (Redis' `addACLLogEntry`).
    pub(crate) fn log(
        &self,
        reason: Reason,
        context: Where,
        object: &[u8],
        username: &[u8],
        client_info: Vec<u8>,
    ) {
        let slot = match reason {
            Reason::Auth => 0,
            Reason::Command => 1,
            Reason::Key => 2,
            Reason::Channel => 3,
        };
        self.denied[slot].fetch_add(1, Relaxed);
        let now = now_millis();
        let mut log = self.log.lock();
        let same = log.entries.iter().take(10).position(|e| {
            e.reason == reason
                && e.context == context
                && (e.updated - now).abs() <= GROUPING_MILLIS
                && *e.object == *object
                && *e.username == *username
        });
        if let Some(at) = same {
            let mut entry = log.entries.remove(at).expect("there");
            entry.client_info = client_info;
            entry.updated = now;
            entry.count += 1;
            log.entries.push_front(entry);
            return;
        }
        let id = log.next_id;
        log.next_id += 1;
        log.entries.push_front(Entry {
            count: 1,
            reason,
            context,
            object: object.into(),
            username: username.into(),
            updated: now,
            created: now,
            client_info,
            id,
        });
        let max = self.log_max.load(Relaxed);
        log.entries.truncate(max);
    }

    /// The newest `count` entries, as `ACL LOG` writes them with `write`.
    pub(crate) fn with_log<R>(&self, f: impl FnOnce(&mut dyn Iterator<Item = &Entry>) -> R) -> R {
        let log = self.log.lock();
        f(&mut log.entries.iter())
    }

    pub(crate) fn reset_log(&self) {
        self.log.lock().entries.clear();
    }

    /// Denied authentications, commands, keys and channels, for `INFO`.
    pub(crate) fn denied(&self) -> [u64; 4] {
        std::array::from_fn(|i| self.denied[i].load(Relaxed))
    }

    /// `ACL LOAD`: the file's users replace these, or nothing changes. The
    /// users that were replaced (all but `default`, whose connections stay).
    pub(crate) fn load(&self) -> Result<Vec<Arc<User>>, String> {
        let path = self.file.as_ref().expect("an ACL file");
        let shown = path.display().to_string();
        let text = std::fs::read(path).map_err(|e| {
            format!(
                "Error loading ACLs, opening file '{shown}': {}",
                strerror(&e)
            )
        })?;
        let all = self.all_channels.load(Relaxed);
        let mut loaded: BTreeMap<Box<[u8]>, User> = BTreeMap::new();
        let mut errors = String::new();
        for (i, line) in text.split(|&b| b == b'\n').enumerate() {
            let number = i + 1;
            let line = trim(line, b" \t\r\n");
            if line.is_empty() {
                continue;
            }
            let args: Vec<&[u8]> = line.split(|&b| b == b' ').collect();
            if args[0] != b"user" || args.len() < 2 {
                let _ = write!(
                    errors,
                    "{shown}:{number} should start with user keyword followed by the username. "
                );
                continue;
            }
            let name = args[1];
            if has_spaces(name) {
                let _ = write!(
                    errors,
                    "'{shown}:{number}: username '{}' contains invalid characters. ",
                    String::from_utf8_lossy(name)
                );
                continue;
            }
            if loaded.contains_key(name) {
                let _ = write!(
                    errors,
                    "WARNING: Duplicate user '{}' found on line {number}. ",
                    String::from_utf8_lossy(name)
                );
                continue;
            }
            let serial = if name == b"default" {
                0
            } else {
                self.next_serial.fetch_add(1, Relaxed)
            };
            let mut user = User::new(name, serial, all);
            let ops = match merge_selectors(&args[2..]) {
                Ok(ops) => ops,
                Err(_) => {
                    let _ = write!(
                        errors,
                        "{shown}:{number}: Unmatched parenthesis in selector definition."
                    );
                    Vec::new()
                }
            };
            let mut syntax_error = false;
            for op in ops {
                let op = trim(&op, b"\t\r\n");
                if let Err(e) = user.apply(op, all) {
                    if e == RuleError::Unknown {
                        let _ = write!(
                            errors,
                            "{shown}:{number}: Error in applying operation '{}': {}. ",
                            String::from_utf8_lossy(op),
                            e.message()
                        );
                    } else if !syntax_error {
                        let _ = write!(errors, "{shown}:{number}: {}. ", e.message());
                        syntax_error = true;
                    }
                }
            }
            loaded.insert(name.into(), user);
        }
        if !errors.is_empty() {
            errors.push_str(
                "WARNING: ACL errors detected, no change to the previously active ACL rules was performed",
            );
            return Err(errors);
        }
        let mut users = self.users.lock();
        let mut fresh: BTreeMap<Box<[u8]>, Arc<User>> = loaded
            .into_iter()
            .map(|(name, user)| (name, Arc::new(user.finish())))
            .collect();
        fresh
            .entry(b"default"[..].into())
            .or_insert_with(|| Arc::new(User::default_user()));
        let old = std::mem::replace(&mut *users, fresh);
        self.note_default(&users);
        Ok(old
            .into_iter()
            .filter(|(name, _)| **name != *b"default")
            .map(|(_, user)| user)
            .collect())
    }

    /// `ACL SAVE`: written whole to a temporary file, then put in place.
    pub(crate) fn save(&self) -> std::io::Result<()> {
        let path = self.file.as_ref().expect("an ACL file");
        let mut text = Vec::new();
        for user in self.users() {
            text.extend_from_slice(b"user ");
            text.extend_from_slice(&user.name);
            text.push(b' ');
            text.extend_from_slice(&user.describe());
            text.push(b'\n');
        }
        let mut temp = path.as_os_str().to_owned();
        temp.push(format!(".tmp-{}-{}", std::process::id(), now_millis()));
        let temp = PathBuf::from(temp);
        let written = (|| {
            use std::io::Write;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&text)?;
            file.sync_all()?;
            std::fs::rename(&temp, path)?;
            if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
                std::fs::File::open(dir)?.sync_all()?;
            }
            Ok(())
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        written
    }
}

/// Whether a user's subscriptions would lose channels to this change, so
/// that its subscribed connections are checked (Redis'
/// `ACLKillPubsubClientsIfNeeded`: none if the new user has every channel,
/// or kept every pattern the old one had).
pub(crate) fn channels_revoked(old: &User, new: &User) -> bool {
    if new.selectors.iter().any(|s| s.all_channels) {
        return false;
    }
    let kept: Vec<&[u8]> = new.channel_patterns().collect();
    old.selectors
        .iter()
        .any(|s| s.all_channels || s.channels.iter().any(|c| !kept.contains(&&**c)))
}

/// Whether a subscription a connection holds is still allowed: a channel, or
/// a pattern, which must be one of the user's literally. Matched against the
/// new user's patterns all together, as Redis does.
pub(crate) fn may_keep(user: &User, name: &[u8], pattern: bool) -> bool {
    user.channel_patterns().any(|known| {
        if pattern {
            known == name
        } else {
            glob::matches(known, name, false)
        }
    })
}

impl User {
    pub(crate) fn serial(&self) -> u64 {
        self.serial
    }

    /// A deleted user's stand-in: its connections close.
    pub(crate) fn tombstone(&self) -> Arc<User> {
        let mut user = User::new(&self.name, self.serial, false);
        user.deleted = true;
        Arc::new(user)
    }

    /// `ACL GETUSER`'s reply.
    pub(crate) fn write_details(&self, out: &mut Vec<u8>, resp3: bool) {
        use crate::resp;
        resp::write_map_header(out, 6, resp3);
        resp::write_bulk(out, b"flags");
        let flags: Vec<&str> = FLAG_NAMES
            .iter()
            .filter(|(flag, _)| self.flags & flag != 0)
            .map(|(_, name)| *name)
            .collect();
        resp::write_set_header(out, flags.len(), resp3);
        for flag in flags {
            resp::write_bulk(out, flag.as_bytes());
        }
        resp::write_bulk(out, b"passwords");
        resp::write_array_header(out, self.passwords.len());
        for password in &self.passwords {
            resp::write_bulk(out, password);
        }
        let selector_fields = |out: &mut Vec<u8>, selector: &Selector| {
            resp::write_bulk(out, b"commands");
            let mut commands = Vec::new();
            selector.describe_commands(&mut commands);
            resp::write_bulk(out, &commands);
            resp::write_bulk(out, b"keys");
            resp::write_bulk(out, &selector.keys_text());
            resp::write_bulk(out, b"channels");
            resp::write_bulk(out, &selector.channels_text());
        };
        selector_fields(out, &self.selectors[0]);
        resp::write_bulk(out, b"selectors");
        resp::write_array_header(out, self.selectors.len() - 1);
        for selector in &self.selectors[1..] {
            resp::write_map_header(out, 3, resp3);
            selector_fields(out, selector);
        }
    }
}

/// Redis' `ACLMergeSelectorArguments`: a selector split over arguments is
/// one. `Err` names the argument where one starts and never ends.
fn merge_selectors(args: &[&[u8]]) -> Result<Vec<Vec<u8>>, usize> {
    let mut merged = Vec::with_capacity(args.len());
    let mut open: Option<(usize, Vec<u8>)> = None;
    for (i, &arg) in args.iter().enumerate() {
        if let Some((_, selector)) = &mut open {
            selector.push(b' ');
            selector.extend_from_slice(arg);
            if arg.last() == Some(&b')') {
                merged.push(open.take().expect("open").1);
            }
            continue;
        }
        if arg.first() == Some(&b'(') && arg.last() != Some(&b')') {
            open = Some((i, arg.to_vec()));
            continue;
        }
        merged.push(arg.to_vec());
    }
    match open {
        Some((at, _)) => Err(at),
        None => Ok(merged),
    }
}

/// Redis' `sdssplitargs`: words, double-quoted strings with escapes, and
/// single-quoted ones. `None` for unbalanced quotes.
pub(crate) fn split_args(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0;
    let space = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    loop {
        while i < line.len() && space(line[i]) {
            i += 1;
        }
        if i >= line.len() {
            return Some(args);
        }
        let mut current = Vec::new();
        let (mut in_double, mut in_single) = (false, false);
        loop {
            if in_double {
                if i >= line.len() {
                    return None;
                }
                let c = line[i];
                if c == b'\\'
                    && i + 3 < line.len()
                    && line[i + 1] == b'x'
                    && line[i + 2].is_ascii_hexdigit()
                    && line[i + 3].is_ascii_hexdigit()
                {
                    let hex = |b: u8| (b as char).to_digit(16).expect("hex") as u8;
                    current.push(hex(line[i + 2]) * 16 + hex(line[i + 3]));
                    i += 3;
                } else if c == b'\\' && i + 1 < line.len() {
                    i += 1;
                    current.push(match line[i] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        other => other,
                    });
                } else if c == b'"' {
                    // A closing quote must be followed by a space or the end.
                    if i + 1 < line.len() && !space(line[i + 1]) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    current.push(c);
                }
            } else if in_single {
                if i >= line.len() {
                    return None;
                }
                let c = line[i];
                if c == b'\\' && i + 1 < line.len() && line[i + 1] == b'\'' {
                    i += 1;
                    current.push(b'\'');
                } else if c == b'\'' {
                    if i + 1 < line.len() && !space(line[i + 1]) {
                        return None;
                    }
                    i += 1;
                    break;
                } else {
                    current.push(c);
                }
            } else {
                if i >= line.len() {
                    break;
                }
                match line[i] {
                    b' ' | b'\n' | b'\r' | b'\t' | 0 => break,
                    b'"' => in_double = true,
                    b'\'' => in_single = true,
                    c => current.push(c),
                }
            }
            i += 1;
        }
        args.push(current);
    }
}

fn trim<'a>(mut s: &'a [u8], chars: &[u8]) -> &'a [u8] {
    while let Some((first, rest)) = s.split_first()
        && chars.contains(first)
    {
        s = rest;
    }
    while let Some((last, rest)) = s.split_last()
        && chars.contains(last)
    {
        s = rest;
    }
    s
}

fn strerror(e: &std::io::Error) -> String {
    // Without Rust's " (os error N)".
    let text = e.to_string();
    match text.find(" (os error") {
        Some(at) => text[..at].to_string(),
        None => text,
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

pub(crate) fn millis_now() -> i64 {
    now_millis()
}

/// A password's SHA-256, in lowercase hex.
fn hash_password(password: &[u8]) -> Vec<u8> {
    let digest = ring::digest::digest(&ring::digest::SHA256, password);
    hex(digest.as_ref())
}

fn hex(bytes: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize]);
        out.push(DIGITS[(b & 15) as usize]);
    }
    out
}

/// A hash given with `#` or `!`: 64 lowercase hex digits.
fn checked_hash(hash: &[u8]) -> Result<Vec<u8>, RuleError> {
    if hash.len() == 64 && hash.iter().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(hash.to_vec())
    } else {
        Err(RuleError::BadHash)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `ACL GENPASS`: `chars` random hex digits.
pub(crate) fn random_hex(chars: usize) -> Vec<u8> {
    use ring::rand::SecureRandom;
    let mut bytes = vec![0; chars.div_ceil(2)];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system's random numbers");
    let mut out = hex(&bytes);
    out.truncate(chars);
    out
}

#[cfg(test)]
mod tests;
