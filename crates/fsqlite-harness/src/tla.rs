//! TLA+ export for MVCC / SSI protocol traces.
//!
//! This is deliberately small and dependency-free so it can be used from any
//! crate's tests without pulling in a full TLA+ toolchain at build time.

use std::collections::BTreeMap;
use std::fmt;

/// A rendered TLA+ module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlaModule {
    /// Module name (`---- MODULE <name> ----`).
    pub name: String,
    /// Full TLA+ source.
    pub source: String,
}

impl fmt::Display for TlaModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

/// Minimal TLA+ value model for emitting states as records/sequences/sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlaValue {
    /// Natural number (non-negative).
    Nat(u64),
    /// Integer (signed).
    Int(i64),
    /// Boolean.
    Bool(bool),
    /// String literal.
    Str(String),
    /// TLA+ sequence literal `<<...>>`.
    Seq(Vec<TlaValue>),
    /// TLA+ set literal `{...}`.
    Set(Vec<TlaValue>),
    /// TLA+ record literal `[k |-> v, ...]`.
    Record(BTreeMap<String, TlaValue>),
}

impl TlaValue {
    fn push_tla(&self, out: &mut String) {
        match self {
            Self::Nat(n) => {
                out.push_str(&n.to_string());
            }
            Self::Int(i) => {
                out.push_str(&i.to_string());
            }
            Self::Bool(b) => {
                out.push_str(if *b { "TRUE" } else { "FALSE" });
            }
            Self::Str(s) => {
                out.push('"');
                push_escaped_tla_string(out, s);
                out.push('"');
            }
            Self::Seq(items) => {
                out.push_str("<<");
                for (idx, item) in items.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    item.push_tla(out);
                }
                out.push_str(">>");
            }
            Self::Set(items) => {
                out.push('{');
                for (idx, item) in items.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    item.push_tla(out);
                }
                out.push('}');
            }
            Self::Record(fields) => {
                out.push('[');
                for (idx, (k, v)) in fields.iter().enumerate() {
                    if idx != 0 {
                        out.push_str(", ");
                    }
                    out.push_str(k);
                    out.push_str(" |-> ");
                    v.push_tla(out);
                }
                out.push(']');
            }
        }
    }
}

impl fmt::Display for TlaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        self.push_tla(&mut s);
        f.write_str(&s)
    }
}

fn push_escaped_tla_string(out: &mut String, s: &str) {
    // TLA+ strings support C-style escapes.
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
}

/// A single MVCC state snapshot rendered as a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccStateSnapshot {
    /// Human label for the step (shows up in the trace state record).
    pub label: String,
    /// Variables captured at this step.
    pub vars: BTreeMap<String, TlaValue>,
}

impl MvccStateSnapshot {
    /// Convert the snapshot into a single TLA+ record value.
    #[must_use]
    pub fn to_record_value(&self) -> TlaValue {
        let mut fields = BTreeMap::new();
        fields.insert("label".to_string(), TlaValue::Str(self.label.clone()));
        for (k, v) in &self.vars {
            fields.insert(k.clone(), v.clone());
        }
        TlaValue::Record(fields)
    }
}

/// Exports a concrete MVCC trace (sequence of snapshots) as a bounded TLA+ behavior.
#[derive(Debug, Clone)]
pub struct MvccTlaExporter {
    snapshots: Vec<MvccStateSnapshot>,
}

impl MvccTlaExporter {
    /// Construct an exporter from snapshots.
    #[must_use]
    pub fn from_snapshots(snapshots: Vec<MvccStateSnapshot>) -> Self {
        Self { snapshots }
    }

    /// Export a behavior module with `States == << ... >>` and `Init`/`Next`.
    ///
    /// This is designed for bounded model checking: `Next` is the disjunction of
    /// the concrete steps observed in the trace.
    #[must_use]
    pub fn export_behavior(&self, name: &str) -> TlaModule {
        let mut src = String::new();
        src.push_str("---- MODULE ");
        src.push_str(name);
        src.push_str(" ----\n");
        src.push_str("EXTENDS Integers, Sequences, TLC\n\n");

        src.push_str("VARIABLES step, state\n\n");

        // States constant
        src.push_str("States == ");
        let mut states = Vec::with_capacity(self.snapshots.len());
        for s in &self.snapshots {
            states.push(s.to_record_value());
        }
        TlaValue::Seq(states).push_tla(&mut src);
        src.push_str("\n\n");

        if self.snapshots.is_empty() {
            src.push_str("Init == FALSE\n");
            src.push_str("Next == FALSE\n\n");
        } else {
            src.push_str("Init ==\n");
            src.push_str("    /\\ step = 1\n");
            src.push_str("    /\\ state = States[1]\n\n");

            src.push_str("Next ==\n");
            if self.snapshots.len() == 1 {
                src.push_str("    FALSE\n\n");
            } else {
                for i in 2..=self.snapshots.len() {
                    src.push_str("    \\/ ");
                    src.push_str("/\\ step = ");
                    src.push_str(&(i - 1).to_string());
                    src.push_str(" /\\ step' = ");
                    src.push_str(&i.to_string());
                    src.push('\n');
                    src.push_str("       /\\ state' = States[");
                    src.push_str(&i.to_string());
                    src.push_str("]\n");
                }
                src.push('\n');
            }
        }

        src.push_str("Spec == Init /\\ [][Next]_<<step, state>>\n\n");
        src.push_str("====\n");

        TlaModule {
            name: name.to_string(),
            source: src,
        }
    }
}
