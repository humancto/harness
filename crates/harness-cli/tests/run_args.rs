//! `harness run` clap parsing + target-selector plumbing.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use clap::Parser;
use harness_cli::{run::run_run, Cli, Command};

fn parse(args: &[&str]) -> Cli {
    Cli::try_parse_from(args).expect("parse")
}

#[test]
fn t15_parses_run_with_double_dash() {
    let cli = parse(&["harness", "run", "--", "ls", "-la"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    assert_eq!(args.argv, vec!["ls".to_string(), "-la".to_string()]);
    assert!(!args.all);
    assert!(args.on.is_none());
}

#[test]
fn t16_parses_run_with_inner_double_dash() {
    let cli = parse(&["harness", "run", "--", "echo", "--", "foo"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    assert_eq!(
        args.argv,
        vec!["echo".to_string(), "--".to_string(), "foo".to_string()]
    );
}

#[test]
fn t17_run_all_is_a_live_target_no_fanout_bail() {
    // 3.3-fanout: --all no longer short-circuits with a "deferred"
    // error — it proceeds to auth/mesh resolution (which fails here
    // because no daemon is running, proving we got past validation).
    let cli = parse(&["harness", "run", "--all", "--", "uname"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let err = rt.block_on(run_run(args)).expect_err("no daemon running");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains("3.3-fanout"),
        "fanout bail must be gone: {msg}"
    );
    assert!(
        msg.contains("authenticate") || msg.contains("mesh view"),
        "must fail at auth/mesh resolution, got: {msg}"
    );
}

#[test]
fn t18_arg_group_rejects_combined_targets() {
    // --all and --on together violate the ArgGroup constraint.
    let res = Cli::try_parse_from(["harness", "run", "--all", "--on", "self", "--", "foo"]);
    assert!(res.is_err(), "clap must reject --all + --on");
}

#[test]
fn t19_run_no_argv_rejected_at_parse_time() {
    let res = Cli::try_parse_from(["harness", "run"]);
    assert!(res.is_err(), "clap must reject empty argv");
}

#[test]
fn t20_run_on_self_does_not_error_at_parse() {
    let cli = parse(&["harness", "run", "--on", "self", "--", "uname"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    assert_eq!(args.on.as_deref(), Some("self"));
    assert_eq!(args.argv, vec!["uname".to_string()]);
}

#[test]
fn t21_run_on_remote_host_resolves_against_mesh() {
    // Remote --on targets resolve against the live mesh view now;
    // with no daemon the failure is at auth/mesh fetch, not a bail.
    let cli = parse(&["harness", "run", "--on", "macbook-archy", "--", "uname"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let err = rt.block_on(run_run(args)).expect_err("no daemon running");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains("3.3-fanout"),
        "fanout bail must be gone: {msg}"
    );
}

#[test]
fn t22_run_where_parses_and_resolves_against_mesh() {
    let cli = parse(&["harness", "run", "--where", "cap:echo", "--", "uname"]);
    let Command::Run(args) = cli.command else {
        panic!("expected Run");
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let err = rt.block_on(run_run(args)).expect_err("no daemon running");
    let msg = format!("{err:#}");
    assert!(
        !msg.contains("3.3-fanout"),
        "fanout bail must be gone: {msg}"
    );
}
