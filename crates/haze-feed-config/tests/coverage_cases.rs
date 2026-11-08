//! Runs the shared cases in contracts/feedconfig/v1/coverage-cases.json, which
//! the Go feedconfig package also runs.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use haze_feed_config::{compile_feed_xml, load_snapshot, FeedCoverage};
use serde::Deserialize;

#[derive(Deserialize)]
struct Cases {
    cases: Vec<Case>,
    invalid: Vec<InvalidCase>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    xml: String,
    feed: FeedCoverage,
    covers: BTreeMap<String, bool>,
    headers: Vec<HeaderCase>,
    full_header: Vec<String>,
}

#[derive(Deserialize)]
struct HeaderCase {
    codes: Vec<String>,
    expect: Vec<String>,
    dropped: Vec<String>,
}

#[derive(Deserialize)]
struct InvalidCase {
    name: String,
    xml: String,
    error_contains: String,
}

fn cases() -> Cases {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/feedconfig/v1/coverage-cases.json");
    serde_json::from_str(&fs::read_to_string(&path).expect("read coverage cases"))
        .expect("parse coverage cases")
}

fn feed_xml(regions: &str) -> String {
    format!("<feed><locations><coverage>{regions}</coverage></locations></feed>")
}

#[test]
fn shared_cases_compile_and_evaluate() {
    for case in cases().cases {
        let compiled = compile_feed_xml("test", &feed_xml(&case.xml))
            .unwrap_or_else(|err| panic!("{}: {err}", case.name));
        assert_eq!(compiled, case.feed, "{}: compiled entries", case.name);
        for (code, covered) in &case.covers {
            assert_eq!(compiled.covers(code), *covered, "{}: covers {code}", case.name);
        }
        for header in &case.headers {
            let result = compiled.same_header(header.codes.iter().map(String::as_str));
            assert_eq!(result.codes, header.expect, "{}: header", case.name);
            assert_eq!(result.dropped, header.dropped, "{}: dropped", case.name);
        }
        assert_eq!(compiled.full_header().codes, case.full_header, "{}: full header", case.name);
    }
}

#[test]
fn shared_invalid_cases_are_rejected() {
    for case in cases().invalid {
        let err = compile_feed_xml("test", &feed_xml(&case.xml))
            .expect_err(&format!("{} was accepted", case.name));
        assert!(
            err.to_string().contains(&case.error_contains),
            "{}: {err}",
            case.name
        );
    }
}

#[test]
fn a_feeds_directory_takes_ids_from_file_names_and_rejects_duplicates() {
    let dir = tempfile::tempdir().expect("temporary feeds directory");
    fs::write(
        dir.path().join("cwxr-sk01.xml"),
        feed_xml(r#"<region id="065100" source="eccc"/>"#),
    )
    .expect("write feed");
    fs::write(
        dir.path().join("named.xml"),
        r#"<feed id="sk-0001"><locations><coverage/></locations></feed>"#,
    )
    .expect("write feed");
    fs::write(dir.path().join("README.md"), "not a feed").expect("write readme");

    let snapshot = load_snapshot(dir.path()).expect("load feeds");
    let ids = snapshot.feeds.iter().map(|feed| feed.feed_id.as_str()).collect::<Vec<_>>();
    assert_eq!(ids, ["cwxr-sk01", "sk-0001"]);
    assert!(snapshot.feed("cwxr-sk01").expect("feed").covers("065122"));
    assert_eq!(snapshot.generation.len(), 16);

    fs::write(
        dir.path().join("copy.xml"),
        r#"<feed id="cwxr-sk01"><locations><coverage/></locations></feed>"#,
    )
    .expect("write duplicate");
    let err = load_snapshot(dir.path()).expect_err("duplicate feed ids were accepted");
    assert!(err.to_string().contains("defined twice"), "{err}");
}

#[test]
fn an_invalid_feed_names_its_file_and_value() {
    let dir = tempfile::tempdir().expect("temporary feeds directory");
    fs::write(
        dir.path().join("cwxr-sk02.xml"),
        feed_xml(r#"<region id="06*55" source="eccc"/>"#),
    )
    .expect("write feed");
    let err = load_snapshot(dir.path()).expect_err("invalid pattern was accepted");
    let message = err.to_string();
    assert!(message.contains("cwxr-sk02") && message.contains("06*55"), "{message}");
}
