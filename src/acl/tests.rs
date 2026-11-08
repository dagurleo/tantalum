use super::*;

fn args_of(words: &[&str]) -> (Vec<u8>, Vec<std::ops::Range<usize>>) {
    let mut buf = Vec::new();
    let mut ranges = Vec::new();
    for word in words {
        let start = buf.len();
        buf.extend_from_slice(word.as_bytes());
        ranges.push(start..buf.len());
    }
    (buf, ranges)
}

/// The user `name` after `ACL SETUSER name rules...`, as `ACL LIST` shows it.
fn described(acl: &Acl, name: &str, rules: &[&str]) -> String {
    let ops: Vec<&[u8]> = rules.iter().map(|r| r.as_bytes()).collect();
    acl.set_user(name.as_bytes(), &ops).expect("valid rules");
    String::from_utf8(acl.get(name.as_bytes()).expect("there").describe()).expect("text")
}

fn commands_of(acl: &Acl, name: &str, rules: &[&str]) -> String {
    let ops: Vec<&[u8]> = rules.iter().map(|r| r.as_bytes()).collect();
    acl.set_user(name.as_bytes(), &ops).expect("valid rules");
    let user = acl.get(name.as_bytes()).expect("there");
    let mut out = Vec::new();
    user.selectors[0].describe_commands(&mut out);
    String::from_utf8(out).expect("text")
}

fn error_of(acl: &Acl, name: &str, rules: &[&str]) -> String {
    let ops: Vec<&[u8]> = rules.iter().map(|r| r.as_bytes()).collect();
    match acl.set_user(name.as_bytes(), &ops) {
        Ok(_) => panic!("{rules:?} should be refused"),
        Err(e) => String::from_utf8(e).expect("text"),
    }
}

/// Whether `user` may run the command `words`.
fn may(acl: &Acl, user: &str, words: &[&str]) -> Result<(), Denied> {
    let user = acl.get(user.as_bytes()).expect("there");
    let (buf, ranges) = args_of(words);
    let args = Args::new(&buf, &ranges);
    let name = commands().names[lookup(words[0].as_bytes()).expect("a command") as usize];
    let target = target(name, &args).expect("known");
    user.check(&target, &args)
}

#[test]
fn users_are_described_as_redis_describes_them() {
    let acl = Acl::new(None, None);
    let default = String::from_utf8(acl.default_user().describe()).unwrap();
    assert_eq!(default, "on nopass sanitize-payload ~* &* +@all");
    assert_eq!(
        described(&acl, "fresh", &[]),
        "off sanitize-payload resetchannels -@all"
    );
    assert_eq!(
        described(
            &acl,
            "hpuser",
            &["on", "nopass", "resetchannels", "&foo", "+@all"]
        ),
        "on nopass sanitize-payload resetchannels &foo +@all"
    );
    assert_eq!(
        described(&acl, "keys", &["on", "~a*", "%R~r*", "%W~w*", "%RW~b"]),
        "on sanitize-payload ~a* %R~r* %W~w* ~b resetchannels -@all"
    );
    let with_password = described(&acl, "pw", &[">secret"]);
    assert_eq!(
        with_password,
        "off sanitize-payload #2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b \
         resetchannels -@all"
    );
    assert_eq!(
        described(
            &acl,
            "sel",
            &[
                "on",
                "nopass",
                "+get",
                "(~x* +set)",
                "(",
                "%R~y",
                "+get",
                ")"
            ]
        ),
        "on nopass sanitize-payload resetchannels -@all +get (~x* resetchannels -@all +set) \
         (%R~y resetchannels -@all +get)"
    );
    // `reset` is what a new user is.
    assert_eq!(
        described(&acl, "sel", &["reset"]),
        "off sanitize-payload resetchannels -@all"
    );
}

#[test]
fn command_rules_compact_as_redis_compacts_them() {
    let acl = Acl::new(None, None);
    let cases: &[(&[&str], &str)] = &[
        (
            &["+@all", "-@hash", "-@slow", "+hget"],
            "+@all -@hash -@slow +hget",
        ),
        (&["-@hash"], "+@all -@slow +hget -@hash"),
        (&["+@hash"], "+@all -@slow +hget +@hash"),
        (&["-@all"], "-@all"),
        (&["-@string", "-@slow", "+@all"], "+@all"),
        (&["-@all", "+@HASH", "+@hash", "+@HaSh"], "-@all +@hash"),
        (&["-@all", "+HGET", "+hget", "+hGeT"], "-@all +hget"),
        (
            &[
                "-@all", "+@hash", "+@slow", "+@set", "+@set", "+@slow", "+@hash",
            ],
            "-@all +@set +@slow +@hash",
        ),
        (
            &["-@all", "+hget", "-hset", "+hset", "-hget"],
            "-@all +hset -hget",
        ),
        (
            &[
                "-@all",
                "+client|list",
                "+client|list",
                "+config|get",
                "+config",
                "+acl|list",
                "-acl",
            ],
            "-@all +client|list +config -acl",
        ),
        (
            &[
                "-@all",
                "+select|0",
                "+select|0",
                "+debug|segfault",
                "+debug",
            ],
            "-@all +select|0 +debug",
        ),
        (&["-@all", "-@dangerous"], "-@all -@dangerous"),
        (
            &["-@all", "+config", "+config|get", "-config|set", "+config"],
            "-@all +config",
        ),
        (&["+@all", "-client|kill"], "+@all -client|kill"),
        (&["reset", "+memory|doctor"], "-@all +memory|doctor"),
        (&["+memory"], "-@all +memory"),
        (&["+@all", "+client|id"], "+@all +client|id"),
    ];
    for (rules, expected) in cases {
        assert_eq!(commands_of(&acl, "adv", rules), *expected, "{rules:?}");
    }
    for category in CATEGORIES {
        let minus = format!("-@{category}");
        assert_eq!(
            commands_of(&acl, "a", &["reset", "+@all", &minus]),
            format!("+@all {minus}")
        );
        let plus = format!("+@{category}");
        assert_eq!(
            commands_of(&acl, "r", &["reset", "-@all", &plus]),
            format!("-@all {plus}")
        );
    }
}

#[test]
fn bad_rules_say_what_redis_says() {
    let acl = Acl::new(None, None);
    let cases: &[(&[&str], &str)] = &[
        (
            &["+nosuch"],
            "ERR Error in ACL SETUSER modifier '+nosuch': Unknown command or category name in ACL",
        ),
        (
            &["+@nosuch"],
            "ERR Error in ACL SETUSER modifier '+@nosuch': Unknown command or category name in ACL",
        ),
        (
            &["+config|asdf"],
            "ERR Error in ACL SETUSER modifier '+config|asdf': Unknown command or category name in ACL",
        ),
        (
            &["-config|asdf"],
            "ERR Error in ACL SETUSER modifier '-config|asdf': Unknown command or category name in ACL",
        ),
        (
            &["+get|key1|key2"],
            "ERR Error in ACL SETUSER modifier '+get|key1|key2': Unknown command or category name in ACL",
        ),
        (
            &["-select|0"],
            "ERR Error in ACL SETUSER modifier '-select|0': Unknown command or category name in ACL",
        ),
        (
            &["+config|get|appendonly"],
            "ERR Error in ACL SETUSER modifier '+config|get|appendonly': Allowing first-arg of a subcommand is not supported",
        ),
        (
            &["+get|"],
            "ERR Error in ACL SETUSER modifier '+get|': Syntax error",
        ),
        (
            &["bogus"],
            "ERR Error in ACL SETUSER modifier 'bogus': Syntax error",
        ),
        (
            &["~a b"],
            "ERR Error in ACL SETUSER modifier '~a b': Syntax error",
        ),
        (
            &["%X~a"],
            "ERR Error in ACL SETUSER modifier '%X~a': Syntax error",
        ),
        (
            &["%RR~a"],
            "ERR Error in ACL SETUSER modifier '%RR~a': Syntax error",
        ),
        (
            &["allkeys", "~a"],
            "ERR Error in ACL SETUSER modifier '~a': Adding a pattern after the * pattern (or the \
             'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start \
             with an empty list of patterns",
        ),
        (
            &["allchannels", "&a"],
            "ERR Error in ACL SETUSER modifier '&a': Adding a pattern after the * pattern (or the \
             'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to \
             start with an empty list of channels",
        ),
        (
            &["<nothere"],
            "ERR Error in ACL SETUSER modifier '<nothere': The password you are trying to remove \
             from the user does not exist",
        ),
        (
            &["#abc"],
            "ERR Error in ACL SETUSER modifier '#abc': The password hash must be exactly 64 \
             characters and contain only lowercase hexadecimal characters",
        ),
        (
            &["(+get", "~x"],
            "ERR Unmatched parenthesis in acl selector starting at '(+get'.",
        ),
        (
            &["(+nosuch)"],
            "ERR Error in ACL SETUSER modifier '(+nosuch)': Unknown command or category name in ACL",
        ),
    ];
    for (rules, expected) in cases {
        assert_eq!(error_of(&acl, "bad", rules), *expected, "{rules:?}");
    }
    // None of them made the user.
    assert!(acl.get(b"bad").is_none());
}

#[test]
fn passwords_hash_and_check() {
    let acl = Acl::new(Some(b"secret"), None);
    assert!(!acl.open());
    assert!(acl.authenticate(b"default", b"secret").is_some());
    assert!(acl.authenticate(b"default", b"wrong").is_none());
    acl.set_user(b"u", &[b">a", b">b", b"on"]).unwrap();
    assert!(acl.authenticate(b"u", b"a").is_some());
    assert!(acl.authenticate(b"u", b"b").is_some());
    acl.set_user(b"u", &[b"<a"]).unwrap();
    assert!(acl.authenticate(b"u", b"a").is_none());
    acl.set_user(b"u", &[b"off"]).unwrap();
    assert!(acl.authenticate(b"u", b"b").is_none());
    acl.set_user(b"u", &[b"on", b"nopass"]).unwrap();
    assert!(acl.authenticate(b"u", b"anything").is_some());
    assert!(acl.authenticate(b"nobody", b"x").is_none());
    acl.set_default_password(None);
    assert!(acl.open());
}

#[test]
fn checks_find_what_redis_finds() {
    let acl = Acl::new(None, None);
    let rules: &[&[u8]] = &[
        b"on", b"nopass", b"+@all", b"-ping", b"%R~r*", b"%W~w*", b"~both",
    ];
    acl.set_user(b"u", rules).unwrap();
    assert_eq!(may(&acl, "u", &["ping"]), Err(Denied::Command));
    assert_eq!(may(&acl, "u", &["get", "r1"]), Ok(()));
    assert_eq!(may(&acl, "u", &["get", "w1"]), Err(Denied::Key(1)));
    assert_eq!(may(&acl, "u", &["set", "w1", "v"]), Ok(()));
    assert_eq!(may(&acl, "u", &["set", "r1", "v"]), Err(Denied::Key(1)));
    // `SET ... GET` reads too.
    assert_eq!(
        may(&acl, "u", &["set", "w1", "v", "GET"]),
        Err(Denied::Key(1))
    );
    assert_eq!(may(&acl, "u", &["set", "both", "v", "GET"]), Ok(()));
    assert_eq!(may(&acl, "u", &["copy", "r1", "w1"]), Ok(()));
    assert_eq!(may(&acl, "u", &["copy", "w1", "r1"]), Err(Denied::Key(1)));
    assert_eq!(
        may(&acl, "u", &["mset", "w1", "1", "r2", "2"]),
        Err(Denied::Key(3))
    );
    // `EXISTS` asks for neither: any pattern will do.
    assert_eq!(may(&acl, "u", &["exists", "r1", "w1"]), Ok(()));
    assert_eq!(may(&acl, "u", &["exists", "x"]), Err(Denied::Key(1)));
    // Channels: none, by default.
    assert_eq!(
        may(&acl, "u", &["publish", "c", "m"]),
        Err(Denied::Channel(1))
    );
    acl.set_user(b"u", &[b"&news.*", b"&lit"]).unwrap();
    assert_eq!(may(&acl, "u", &["publish", "news.1", "m"]), Ok(()));
    assert_eq!(
        may(&acl, "u", &["subscribe", "news.1", "other"]),
        Err(Denied::Channel(2))
    );
    // A pattern subscribed to must be one of the user's literally.
    assert_eq!(may(&acl, "u", &["psubscribe", "news.*"]), Ok(()));
    assert_eq!(
        may(&acl, "u", &["psubscribe", "news.1*"]),
        Err(Denied::Channel(1))
    );
    // Subcommands, and first arguments.
    acl.set_user(b"u", &[b"-@all", b"+client|id", b"+select|0", b"allkeys"])
        .unwrap();
    assert_eq!(may(&acl, "u", &["client", "id"]), Ok(()));
    assert_eq!(
        may(&acl, "u", &["client", "kill", "1.2.3.4:5"]),
        Err(Denied::Command)
    );
    assert_eq!(may(&acl, "u", &["select", "0"]), Ok(()));
    assert_eq!(may(&acl, "u", &["select", "1"]), Err(Denied::Command));
    // `AUTH` and the like need no permission.
    assert_eq!(may(&acl, "u", &["auth", "a", "b"]), Ok(()));
}

#[test]
fn selectors_allow_what_one_of_them_allows() {
    let acl = Acl::new(None, None);
    let rules: &[&[u8]] = &[b"on", b"nopass", b"+get", b"~a*", b"(+set ~b*)"];
    acl.set_user(b"u", rules).unwrap();
    assert_eq!(may(&acl, "u", &["get", "a1"]), Ok(()));
    assert_eq!(may(&acl, "u", &["set", "b1", "v"]), Ok(()));
    // Each selector on its own: GET of b* isn't allowed by either.
    assert_eq!(may(&acl, "u", &["get", "b1"]), Err(Denied::Key(1)));
    assert_eq!(may(&acl, "u", &["del", "a1"]), Err(Denied::Command));
}

#[test]
fn categories_are_those_command_lists() {
    // Each command here has the categories COMMAND gives it.
    for name in crate::cmd::command_names() {
        let id = lookup(name.as_bytes()).unwrap_or_else(|| panic!("{name} isn't in the table"));
        for (i, category) in CATEGORIES.iter().enumerate() {
            let listed = commands().categories[id as usize] & (1 << i) != 0;
            assert_eq!(
                listed,
                crate::command_table::in_category(name, category.as_bytes()),
                "{name} @{category}"
            );
        }
    }
}

#[test]
fn rules_split_as_redis_splits_them() {
    let split = |s: &str| {
        split_args(s.as_bytes()).map(|args| {
            args.into_iter()
                .map(|a| String::from_utf8(a).unwrap())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(split("+get  ~a* "), Some(vec!["+get".into(), "~a*".into()]));
    assert_eq!(
        split(r#""a b" 'c d' "\x41\n""#),
        Some(vec!["a b".into(), "c d".into(), "A\n".into()])
    );
    assert_eq!(split(r#""unclosed"#), None);
    assert_eq!(split(r#""a"b"#), None);
}

#[test]
fn the_acl_file_loads_what_was_saved() {
    let dir = std::env::temp_dir().join(format!("tantalum-acl-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("users.acl");
    let acl = Acl::new(None, Some(path.clone()));
    acl.set_user(
        b"alice",
        &[b"on", b">p", b"~cached:*", b"+get", b"(+set ~x)"],
    )
    .unwrap();
    acl.save().unwrap();
    let text = String::from_utf8(std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        text,
        "user alice on sanitize-payload #148de9c5a7a44d19e56cd9ae1a554bf67847afb0c58f6e12fa29ac7ddfca9940 \
         ~cached:* resetchannels -@all +get (~x resetchannels -@all +set)\n\
         user default on nopass sanitize-payload ~* &* +@all\n"
    );
    let other = Acl::new(None, Some(path.clone()));
    let replaced = other.load().unwrap();
    assert!(replaced.is_empty());
    assert_eq!(
        other.get(b"alice").unwrap().describe(),
        acl.get(b"alice").unwrap().describe()
    );
    std::fs::write(&path, "user bob +nosuch\nnot a user\n").unwrap();
    let Err(errors) = other.load() else {
        panic!("a bad file loads");
    };
    let shown = path.display();
    assert_eq!(
        errors,
        format!(
            "{shown}:1: Error in applying operation '+nosuch': Unknown command or category name in ACL. \
             {shown}:2 should start with user keyword followed by the username. \
             WARNING: ACL errors detected, no change to the previously active ACL rules was performed"
        )
    );
    // Nothing changed.
    assert!(other.get(b"alice").is_some());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_log_groups_alike_denials() {
    let acl = Acl::new(None, None);
    acl.log(
        Reason::Command,
        Where::Toplevel,
        b"get",
        b"u",
        b"one".to_vec(),
    );
    acl.log(
        Reason::Command,
        Where::Toplevel,
        b"get",
        b"u",
        b"two".to_vec(),
    );
    acl.log(Reason::Key, Where::Multi, b"k", b"u", b"three".to_vec());
    acl.with_log(|entries| {
        let entries: Vec<_> = entries.collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            (entries[0].reason, entries[0].count, entries[0].id),
            (Reason::Key, 1, 1)
        );
        assert_eq!((entries[1].count, entries[1].id), (2, 0));
        assert_eq!(entries[1].client_info, b"two");
    });
    assert_eq!(acl.denied(), [0, 2, 1, 0]);
    acl.set_log_max(1);
    acl.log(Reason::Auth, Where::Toplevel, b"AUTH", b"x", Vec::new());
    acl.with_log(|entries| assert_eq!(entries.count(), 1));
}
