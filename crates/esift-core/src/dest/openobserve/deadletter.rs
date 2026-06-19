//! Persist rejected documents for later inspection or replay.
//!
//! When `options.dead_letter_path` is set, each rejected document is appended
//! to that file as one NDJSON line. When it is `None`, rejects are dropped here
//! (the orchestrator still logs them).

use super::config::OpenObserveOptions;
use super::types::RejectedDoc;
use crate::error::Result;
use std::fs::OpenOptions;
use std::io::Write;

/// Write rejected documents to the dead-letter sink, if configured.
pub(crate) fn write(options: &OpenObserveOptions, rejected: &[RejectedDoc]) -> Result<()> {
    let Some(path) = &options.dead_letter_path else {
        return Ok(());
    };

    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    for r in rejected {
        let line = serde_json::to_string(&serde_json::json!({
            "stream": r.stream,
            "reason": r.reason,
            "body": r.body,
        }))?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn writes_one_ndjson_line_per_rejected_doc() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dead-letter.ndjson");

        let options = OpenObserveOptions {
            dead_letter_path: Some(path.clone()),
            ..Default::default()
        };
        let rejected = vec![
            RejectedDoc {
                stream: "logs".to_string(),
                reason: "mapper_parsing_exception".to_string(),
                body: serde_json::json!({ "id": 1, "msg": "first" }),
            },
            RejectedDoc {
                stream: "metrics".to_string(),
                reason: "version_conflict".to_string(),
                body: serde_json::json!({ "id": 2, "msg": "second" }),
            },
        ];

        write(&options, &rejected).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(contents.contains("\"stream\":\"logs\""));
        assert!(contents.contains("\"reason\":\"version_conflict\""));

        // Each line must be valid JSON carrying the three expected fields.
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["stream"], "logs");
        assert_eq!(first["reason"], "mapper_parsing_exception");
        assert_eq!(first["body"]["msg"], "first");
    }

    #[test]
    fn appends_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dead-letter.ndjson");

        let options = OpenObserveOptions {
            dead_letter_path: Some(path.clone()),
            ..Default::default()
        };
        let doc = |id: u32| RejectedDoc {
            stream: "logs".to_string(),
            reason: "boom".to_string(),
            body: serde_json::json!({ "id": id }),
        };

        write(&options, &[doc(1)]).unwrap();
        write(&options, &[doc(2)]).unwrap();

        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 2);
    }

    #[test]
    fn no_path_is_a_noop() {
        let options = OpenObserveOptions::default();
        let rejected = vec![RejectedDoc {
            stream: "logs".to_string(),
            reason: "boom".to_string(),
            body: serde_json::json!({ "id": 1 }),
        }];

        // Must not error and must not create any file.
        write(&options, &rejected).unwrap();
    }
}
