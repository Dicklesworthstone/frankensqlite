//! bd-vacuum-desc-index-self-reject-y2aog: VACUUM INTO must produce a database
//! that passes fsqlite's OWN `PRAGMA integrity_check`. Reproduction from the
//! hfdt production store (2026-07-31): a composite index whose second term is
//! DESC, duplicate leading keys present — the real 48-row `source_handles`
//! key set verbatim. C SQLite (rusqlite) adjudicates the SAME candidate bytes
//! as the file-format oracle, so a failure here discriminates writer-vs-checker:
//! fsqlite-RED + oracle-OK  => fsqlite's integrity checker false-positives;
//! fsqlite-RED + oracle-RED => fsqlite's vacuum writer emits mis-ordered leaves.

use fsqlite::{Connection, SqliteValue};

const ROWS: &[(&str, &str)] = &[
    (
        "firecrawl:url:https://ir.crowdstrike.com/",
        "2026-07-04T04:03:20Z",
    ),
    (
        "firecrawl:url:https://ir.crowdstrike.com/",
        "2026-07-04T04:06:22Z",
    ),
    ("sec_edgar:cik:0000094049", "2026-07-10T22:11:39Z"),
    ("EVT", "2026-07-12T20:38:37Z"),
    ("SPFI", "2026-07-12T21:38:14Z"),
    ("SPFI", "2026-07-12T21:42:13Z"),
    ("SPFI", "2026-07-12T21:43:05Z"),
    ("SPFI", "2026-07-12T21:45:48.681268916+00:00"),
    ("SPFI", "2026-07-12T21:48:59Z"),
    ("HAYW", "2026-07-14T05:05:02Z"),
    ("CICB", "2026-07-14T22:43:50.234493102+00:00"),
    ("CTRN", "2026-07-16T21:43:09Z"),
    ("ROST", "2026-07-16T21:48:42Z"),
    ("MAS", "2026-07-17T00:30:32Z"),
    ("AOS", "2026-07-17T00:33:08Z"),
    ("CAVA", "2026-07-17T00:38:58Z"),
    ("NUE", "2026-07-17T01:01:01Z"),
    ("AMD", "2026-07-17T01:01:31Z"),
    ("WDC", "2026-07-17T01:01:40Z"),
    ("MU", "2026-07-17T01:01:50Z"),
    ("AMD", "2026-07-17T01:04:26Z"),
    ("AMD", "2026-07-17T01:05:53Z"),
    ("WRBY", "2026-07-17T01:19:36Z"),
    ("AFRM", "2026-07-17T01:20:36Z"),
    ("NET", "2026-07-17T01:20:42Z"),
    ("HTFL", "2026-07-17T01:20:49Z"),
    ("GWW", "2026-07-17T03:19:59Z"),
    (
        "sec_edgar:cik:0001929872",
        "2026-07-17T03:30:18.411986072+00:00",
    ),
    (
        "sec_edgar:cik:0001181114",
        "2026-07-17T03:30:18.412203934+00:00",
    ),
    (
        "sec_edgar:cik:0001787802",
        "2026-07-17T03:30:18.412221207+00:00",
    ),
    (
        "sec_edgar:cik:0001279505",
        "2026-07-17T03:30:18.412230885+00:00",
    ),
    (
        "sec_edgar:cik:0001182054",
        "2026-07-17T03:30:18.412240814+00:00",
    ),
    (
        "sec_edgar:cik:0001248040",
        "2026-07-17T03:30:18.412249660+00:00",
    ),
    (
        "sec_edgar:cik:0001515709",
        "2026-07-17T03:30:18.412257916+00:00",
    ),
    (
        "sec_edgar:cik:0001307654",
        "2026-07-17T03:30:18.412268566+00:00",
    ),
    ("sec_edgar:cik:0000320193", "2026-07-18T04:38:32Z"),
    ("sec_edgar:cik:0000320193", "2026-07-18T04:38:41Z"),
    ("000106798326000008", "2026-05-15T19:31:00Z"),
    ("NKE", "2026-07-18T04:59:01Z"),
    ("TSM", "2026-07-18T04:59:38Z"),
    ("NKE", "2026-07-18T05:00:08Z"),
    ("NDSN", "2026-07-18T05:51:10Z"),
    ("WSO", "2026-07-18T05:51:18Z"),
    ("ROK", "2026-07-18T06:09:55Z"),
    ("DOV", "2026-07-18T06:24:44Z"),
    (
        "google_trends:keyword:Stepan",
        "2026-07-23T21:53:07.782820912Z",
    ),
    ("sec_edgar:cik:0001046179", "2026-07-24T07:14:09Z"),
    ("api_ninjas:ticker:AZTA", "2026-07-26T02:44:23Z"),
];

fn check_output(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| row.join("|"))
        .collect::<Vec<_>>()
        .join("; ")
}

#[test]
fn vacuum_into_candidate_passes_own_integrity_check_with_desc_composite_index() {
    asupersync::test_utils::run_test(|| async {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("src.sqlite").to_string_lossy().into_owned();
        let cand = dir
            .path()
            .join("cand.sqlite")
            .to_string_lossy()
            .into_owned();

        let conn = Connection::open(&src).await.expect("open source");
        conn.execute(
            "CREATE TABLE source_handles(provider_subject_id TEXT NOT NULL, known_at TEXT NOT NULL)",
        )
        .await
        .expect("create table");
        conn.execute(
            "CREATE INDEX idx_source_handles_provider_subject_id \
             ON source_handles(provider_subject_id, known_at DESC)",
        )
        .await
        .expect("create desc composite index");
        for (subject, known_at) in ROWS {
            conn.execute_with_params(
                "INSERT INTO source_handles VALUES (?1, ?2)",
                &[
                    SqliteValue::Text((*subject).into()),
                    SqliteValue::Text((*known_at).into()),
                ],
            )
            .await
            .expect("insert row");
        }

        let source_verdict = conn
            .query("PRAGMA integrity_check")
            .await
            .expect("source integrity_check runs")
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(|v| format!("{v:?}"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert!(
            check_output(&source_verdict).contains("ok"),
            "fsqlite-written SOURCE already fails its own integrity_check: {}",
            check_output(&source_verdict)
        );

        conn.execute_with_params("VACUUM INTO ?1", &[SqliteValue::Text(cand.as_str().into())])
            .await
            .expect("VACUUM INTO succeeds at the statement level");
        conn.close().await.expect("close source");

        let oracle = rusqlite::Connection::open(&cand).expect("oracle opens candidate");
        let oracle_verdict: String = oracle
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("oracle integrity_check");

        let cand_conn = Connection::open(&cand)
            .await
            .expect("fsqlite reopens candidate");
        let cand_verdict = cand_conn
            .query("PRAGMA integrity_check")
            .await
            .expect("candidate integrity_check runs")
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(|v| format!("{v:?}"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert!(
            check_output(&cand_verdict).contains("ok"),
            "DISCRIMINATOR: fsqlite verdict on its own VACUUM INTO candidate = {} ;; \
             C-SQLite oracle verdict on the SAME bytes = {oracle_verdict:?}",
            check_output(&cand_verdict)
        );
        assert_eq!(oracle_verdict, "ok", "oracle rejects candidate bytes");
        cand_conn.close().await.expect("close candidate");
    });
}
