//! The cluster bus's messages, byte for byte as Redis 7.2 lays out its
//! `clusterMsg` (`cluster.h`), so Tantalum and Redis nodes can be in one
//! cluster: a 2256-byte header (all numbers big-endian), then by type a
//! gossip section and extensions (`PING`, `PONG`, `MEET`), a node name
//! (`FAIL`), a channel and message (`PUBLISH`), or a node's slots
//! (`UPDATE`).

pub(crate) const NAMELEN: usize = 40;
pub(crate) const SLOTS: usize = 16384;
pub(crate) const SLOT_BYTES: usize = SLOTS / 8;
const IPLEN: usize = 46;
/// The header, without the data after it (`CLUSTERMSG_MIN_LEN`).
pub(crate) const HEADER: usize = 2256;
const GOSSIP: usize = 104;

pub(crate) const PING: u16 = 0;
pub(crate) const PONG: u16 = 1;
pub(crate) const MEET: u16 = 2;
pub(crate) const FAIL: u16 = 3;
pub(crate) const PUBLISH: u16 = 4;
pub(crate) const AUTH_REQUEST: u16 = 5;
pub(crate) const AUTH_ACK: u16 = 6;
pub(crate) const UPDATE: u16 = 7;
pub(crate) const MFSTART: u16 = 8;
pub(crate) const MODULE: u16 = 9;
pub(crate) const PUBLISHSHARD: u16 = 10;
pub(crate) const TYPES: usize = 11;

/// `mflags[0]`.
pub(crate) const FLAG0_PAUSED: u8 = 1;
pub(crate) const FLAG0_FORCEACK: u8 = 2;
pub(crate) const FLAG0_EXT_DATA: u8 = 4;

const EXT_HOSTNAME: u16 = 0;
const EXT_HUMAN_NODENAME: u16 = 1;
const EXT_FORGOTTEN_NODE: u16 = 2;
const EXT_SHARDID: u16 = 3;

/// `clusterGetMessageTypeString`.
pub(crate) fn type_name(t: usize) -> &'static str {
    match t as u16 {
        PING => "ping",
        PONG => "pong",
        MEET => "meet",
        FAIL => "fail",
        PUBLISH => "publish",
        PUBLISHSHARD => "publishshard",
        AUTH_REQUEST => "auth-req",
        AUTH_ACK => "auth-ack",
        UPDATE => "update",
        MFSTART => "mfstart",
        MODULE => "module",
        _ => "unknown",
    }
}

pub(crate) type Name = [u8; NAMELEN];
pub(crate) const NULL_NAME: Name = [0; NAMELEN];
pub(crate) type Slots = [u8; SLOT_BYTES];

pub(crate) fn slot_bit(slots: &Slots, slot: usize) -> bool {
    slots[slot / 8] & (1 << (slot & 7)) != 0
}

pub(crate) fn set_slot_bit(slots: &mut Slots, slot: usize, on: bool) {
    if on {
        slots[slot / 8] |= 1 << (slot & 7);
    } else {
        slots[slot / 8] &= !(1 << (slot & 7));
    }
}

/// A message's header (`clusterMsg` up to `data`).
#[derive(Clone)]
pub(crate) struct Header {
    pub(crate) kind: u16,
    pub(crate) count: u16,
    pub(crate) port: u16,
    pub(crate) current_epoch: u64,
    pub(crate) config_epoch: u64,
    pub(crate) offset: u64,
    pub(crate) sender: Name,
    pub(crate) myslots: Box<Slots>,
    pub(crate) slaveof: Name,
    /// The sender's announced IP; empty if it doesn't announce one.
    pub(crate) myip: String,
    pub(crate) extensions: u16,
    pub(crate) pport: u16,
    pub(crate) cport: u16,
    pub(crate) flags: u16,
    pub(crate) state: u8,
    pub(crate) mflags: [u8; 3],
}

impl Default for Header {
    fn default() -> Self {
        Header {
            kind: 0,
            count: 0,
            port: 0,
            current_epoch: 0,
            config_epoch: 0,
            offset: 0,
            sender: NULL_NAME,
            myslots: Box::new([0; SLOT_BYTES]),
            slaveof: NULL_NAME,
            myip: String::new(),
            extensions: 0,
            pport: 0,
            cport: 0,
            flags: 0,
            state: 0,
            mflags: [0; 3],
        }
    }
}

/// One node in a ping's gossip section.
#[derive(Clone, Debug)]
pub(crate) struct Gossip {
    pub(crate) name: Name,
    pub(crate) ping_sent: u32,
    pub(crate) pong_received: u32,
    pub(crate) ip: String,
    pub(crate) port: u16,
    pub(crate) cport: u16,
    pub(crate) flags: u16,
    pub(crate) pport: u16,
}

/// A ping's extension.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Ext {
    Hostname(String),
    HumanNodename(String),
    ForgottenNode(Name, u64),
    ShardId(Name),
    Other,
}

/// What follows the header.
#[derive(Clone)]
pub(crate) enum Body {
    Ping {
        gossip: Vec<Gossip>,
        exts: Vec<Ext>,
    },
    Fail(Name),
    Publish {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    Update {
        config_epoch: u64,
        name: Name,
        slots: Box<Slots>,
    },
    /// `AUTH_REQUEST`, `AUTH_ACK`, `MFSTART`, and kinds not known here.
    None,
}

pub(crate) struct Msg {
    pub(crate) header: Header,
    pub(crate) body: Body,
}

fn put_str(out: &mut Vec<u8>, s: &str, len: usize) {
    let bytes = s.as_bytes();
    let n = bytes.len().min(len - 1);
    out.extend_from_slice(&bytes[..n]);
    out.resize(out.len() + len - n, 0);
}

fn get_str(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end]).into_owned()
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_be_bytes([b[at], b[at + 1]])
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

fn name_at(b: &[u8], at: usize) -> Name {
    b[at..at + NAMELEN].try_into().expect("a name")
}

fn aligned(n: usize) -> usize {
    n.div_ceil(8) * 8
}

impl Msg {
    /// The message's bytes, with its length in its header.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let h = &self.header;
        let mut out = Vec::with_capacity(HEADER + 512);
        out.extend_from_slice(b"RCmb");
        out.extend_from_slice(&[0; 4]); // totlen, below
        out.extend_from_slice(&1u16.to_be_bytes());
        out.extend_from_slice(&h.port.to_be_bytes());
        out.extend_from_slice(&h.kind.to_be_bytes());
        let mut count = h.count;
        let mut extensions = 0u16;
        let mut mflags = h.mflags;
        if let Body::Ping { gossip, exts } = &self.body {
            count = gossip.len() as u16;
            extensions = exts.len() as u16;
            if !exts.is_empty() {
                mflags[0] |= FLAG0_EXT_DATA;
            }
        }
        out.extend_from_slice(&count.to_be_bytes());
        out.extend_from_slice(&h.current_epoch.to_be_bytes());
        out.extend_from_slice(&h.config_epoch.to_be_bytes());
        out.extend_from_slice(&h.offset.to_be_bytes());
        out.extend_from_slice(&h.sender);
        out.extend_from_slice(&h.myslots[..]);
        out.extend_from_slice(&h.slaveof);
        put_str(&mut out, &h.myip, IPLEN);
        out.extend_from_slice(&extensions.to_be_bytes());
        out.extend_from_slice(&[0; 30]);
        out.extend_from_slice(&h.pport.to_be_bytes());
        out.extend_from_slice(&h.cport.to_be_bytes());
        out.extend_from_slice(&h.flags.to_be_bytes());
        out.push(h.state);
        out.extend_from_slice(&mflags);
        debug_assert_eq!(out.len(), HEADER);
        match &self.body {
            Body::Ping { gossip, exts } => {
                for g in gossip {
                    out.extend_from_slice(&g.name);
                    out.extend_from_slice(&g.ping_sent.to_be_bytes());
                    out.extend_from_slice(&g.pong_received.to_be_bytes());
                    put_str(&mut out, &g.ip, IPLEN);
                    out.extend_from_slice(&g.port.to_be_bytes());
                    out.extend_from_slice(&g.cport.to_be_bytes());
                    out.extend_from_slice(&g.flags.to_be_bytes());
                    out.extend_from_slice(&g.pport.to_be_bytes());
                    out.extend_from_slice(&[0; 2]);
                }
                for ext in exts {
                    let (kind, data): (u16, Vec<u8>) = match ext {
                        Ext::Hostname(s) => (EXT_HOSTNAME, [s.as_bytes(), &[0]].concat()),
                        Ext::HumanNodename(s) => {
                            (EXT_HUMAN_NODENAME, [s.as_bytes(), &[0]].concat())
                        }
                        Ext::ForgottenNode(name, ttl) => {
                            (EXT_FORGOTTEN_NODE, [&name[..], &ttl.to_be_bytes()].concat())
                        }
                        Ext::ShardId(id) => (EXT_SHARDID, id.to_vec()),
                        Ext::Other => continue,
                    };
                    let len = 8 + aligned(data.len());
                    out.extend_from_slice(&(len as u32).to_be_bytes());
                    out.extend_from_slice(&kind.to_be_bytes());
                    out.extend_from_slice(&[0; 2]);
                    out.extend_from_slice(&data);
                    out.resize(out.len() + aligned(data.len()) - data.len(), 0);
                }
            }
            Body::Fail(name) => out.extend_from_slice(name),
            Body::Publish { channel, message } => {
                out.extend_from_slice(&(channel.len() as u32).to_be_bytes());
                out.extend_from_slice(&(message.len() as u32).to_be_bytes());
                out.extend_from_slice(channel);
                out.extend_from_slice(message);
            }
            Body::Update {
                config_epoch,
                name,
                slots,
            } => {
                out.extend_from_slice(&config_epoch.to_be_bytes());
                out.extend_from_slice(name);
                out.extend_from_slice(&slots[..]);
            }
            Body::None => {}
        }
        let total = out.len() as u32;
        out[4..8].copy_from_slice(&total.to_be_bytes());
        out
    }

    /// A whole message (`buf` is exactly its `totlen` bytes), checked as
    /// `clusterProcessPacket` checks one; `None` if it's not well formed.
    pub(crate) fn decode(buf: &[u8]) -> Option<Msg> {
        if buf.len() < HEADER || &buf[..4] != b"RCmb" || u16_at(buf, 8) != 1 {
            return None;
        }
        let total = u32_at(buf, 4) as usize;
        if total != buf.len() {
            return None;
        }
        let header = Header {
            port: u16_at(buf, 10),
            kind: u16_at(buf, 12),
            count: u16_at(buf, 14),
            current_epoch: u64_at(buf, 16),
            config_epoch: u64_at(buf, 24),
            offset: u64_at(buf, 32),
            sender: name_at(buf, 40),
            myslots: Box::new(buf[80..80 + SLOT_BYTES].try_into().expect("slots")),
            slaveof: name_at(buf, 2128),
            myip: get_str(&buf[2168..2168 + IPLEN]),
            extensions: u16_at(buf, 2214),
            pport: u16_at(buf, 2246),
            cport: u16_at(buf, 2248),
            flags: u16_at(buf, 2250),
            state: buf[2252],
            mflags: [buf[2253], buf[2254], buf[2255]],
        };
        let data = &buf[HEADER..];
        let body = match header.kind {
            PING | PONG | MEET => {
                let count = header.count as usize;
                let gossip_len = count * GOSSIP;
                if data.len() < gossip_len {
                    return None;
                }
                let gossip = (0..count)
                    .map(|i| {
                        let g = &data[i * GOSSIP..(i + 1) * GOSSIP];
                        Gossip {
                            name: name_at(g, 0),
                            ping_sent: u32_at(g, 40),
                            pong_received: u32_at(g, 44),
                            ip: get_str(&g[48..48 + IPLEN]),
                            port: u16_at(g, 94),
                            cport: u16_at(g, 96),
                            flags: u16_at(g, 98),
                            pport: u16_at(g, 100),
                        }
                    })
                    .collect();
                let mut exts = Vec::new();
                let mut at = gossip_len;
                if header.mflags[0] & FLAG0_EXT_DATA != 0 {
                    for _ in 0..header.extensions {
                        if data.len() < at + 8 {
                            return None;
                        }
                        let len = u32_at(data, at) as usize;
                        let kind = u16_at(data, at + 4);
                        if !len.is_multiple_of(8) || len < 8 || data.len() < at + len {
                            return None;
                        }
                        let payload = &data[at + 8..at + len];
                        exts.push(match kind {
                            EXT_HOSTNAME => Ext::Hostname(get_str(payload)),
                            EXT_HUMAN_NODENAME => Ext::HumanNodename(get_str(payload)),
                            EXT_FORGOTTEN_NODE if payload.len() >= NAMELEN + 8 => {
                                Ext::ForgottenNode(name_at(payload, 0), u64_at(payload, NAMELEN))
                            }
                            EXT_SHARDID if payload.len() >= NAMELEN => {
                                Ext::ShardId(name_at(payload, 0))
                            }
                            _ => Ext::Other,
                        });
                        at += len;
                    }
                }
                if at != data.len() {
                    return None;
                }
                Body::Ping { gossip, exts }
            }
            FAIL => {
                if data.len() != NAMELEN {
                    return None;
                }
                Body::Fail(name_at(data, 0))
            }
            PUBLISH | PUBLISHSHARD => {
                if data.len() < 8 {
                    return None;
                }
                let (c, m) = (u32_at(data, 0) as usize, u32_at(data, 4) as usize);
                if data.len() != 8 + c + m {
                    return None;
                }
                Body::Publish {
                    channel: data[8..8 + c].to_vec(),
                    message: data[8 + c..].to_vec(),
                }
            }
            AUTH_REQUEST | AUTH_ACK | MFSTART => {
                if !data.is_empty() {
                    return None;
                }
                Body::None
            }
            UPDATE => {
                if data.len() != 8 + NAMELEN + SLOT_BYTES {
                    return None;
                }
                Body::Update {
                    config_epoch: u64_at(data, 0),
                    name: name_at(data, 8),
                    slots: Box::new(data[48..].try_into().expect("slots")),
                }
            }
            _ => Body::None,
        };
        Some(Msg { header, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_come_back_as_they_went() {
        let mut header = Header {
            kind: PING,
            port: 7000,
            cport: 17000,
            current_epoch: 5,
            config_epoch: 3,
            offset: 1234,
            sender: [b'a'; NAMELEN],
            myip: "10.0.0.1".into(),
            flags: 1,
            ..Header::default()
        };
        set_slot_bit(&mut header.myslots, 0, true);
        set_slot_bit(&mut header.myslots, 16383, true);
        let msg = Msg {
            header,
            body: Body::Ping {
                gossip: vec![Gossip {
                    name: [b'b'; NAMELEN],
                    ping_sent: 1,
                    pong_received: 2,
                    ip: "10.0.0.2".into(),
                    port: 7001,
                    cport: 17001,
                    flags: 2,
                    pport: 0,
                }],
                exts: vec![Ext::Hostname("host".into()), Ext::ShardId([b'c'; NAMELEN])],
            },
        };
        let bytes = msg.encode();
        // As Redis lays it out: the header, one gossip entry, then the
        // hostname (8 + 8) and shard ID (8 + 40) extensions.
        assert_eq!(bytes.len(), HEADER + GOSSIP + 16 + 48);
        assert_eq!(&bytes[2168..2176], b"10.0.0.1");
        let back = Msg::decode(&bytes).unwrap();
        assert_eq!(back.header.port, 7000);
        assert_eq!(back.header.offset, 1234);
        assert!(slot_bit(&back.header.myslots, 16383) && !slot_bit(&back.header.myslots, 1));
        let Body::Ping { gossip, exts } = back.body else {
            panic!()
        };
        assert_eq!(gossip[0].ip, "10.0.0.2");
        assert_eq!(gossip[0].cport, 17001);
        assert_eq!(
            exts,
            vec![Ext::Hostname("host".into()), Ext::ShardId([b'c'; NAMELEN])]
        );
        // Cut short or padded, it's refused.
        assert!(Msg::decode(&bytes[..bytes.len() - 8]).is_none());
        let upd = Msg {
            header: Header {
                kind: UPDATE,
                ..Header::default()
            },
            body: Body::Update {
                config_epoch: 9,
                name: [b'd'; NAMELEN],
                slots: Box::new([0xff; SLOT_BYTES]),
            },
        };
        let Body::Update { config_epoch, .. } = Msg::decode(&upd.encode()).unwrap().body else {
            panic!()
        };
        assert_eq!(config_epoch, 9);
    }
}
