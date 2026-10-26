//! SQLite date/time functions (§13.3).
//!
//! Implements: date(), time(), datetime(), julianday(), unixepoch(),
//! strftime(), timediff().
//!
//! Internal representation is Julian Day Number (f64).  All functions
//! parse input to JDN, apply modifiers left-to-right, then format.
//!
//! Invalid inputs return NULL (never error).
#![allow(
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::items_after_statements,
    clippy::match_same_arms,
    clippy::float_cmp,
    clippy::suboptimal_flops,
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::unnecessary_wraps,
    clippy::cognitive_complexity,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::unreadable_literal,
    clippy::manual_range_contains,
    clippy::range_plus_one,
    clippy::format_push_string,
    clippy::redundant_else
)]

use std::{
    borrow::Cow,
    fmt::{Arguments, Write as _},
};

use fsqlite_error::Result;
use fsqlite_types::{SmallText, SqliteValue};

use crate::{FunctionRegistry, ScalarFunction};

// ── Per-datetime UTC Offset ───────────────────────────────────────────────
//
// Used by the 'localtime' and 'utc' modifiers.  We compute the UTC offset
// for the *specific* datetime being converted so that DST transitions are
// handled correctly (matching C SQLite's per-call localtime_r behaviour).
// wasm32 has no stable host-local timezone provider through this crate path,
// so wasm builds keep these modifiers as explicit UTC no-ops.

/// Return the UTC offset in seconds, interpreting the components as **UTC** time.
///
/// Used by the `localtime` modifier (UTC → local): the input JDN is UTC,
/// so we convert to a UTC instant and ask chrono what the local offset is
/// at that moment. This correctly handles DST transitions where the UTC
/// time and the resulting local time fall in different DST phases.
#[cfg(not(target_arch = "wasm32"))]
fn utc_offset_for_utc_datetime(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    use chrono::{Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    let date = NaiveDate::from_ymd_opt(y, mo, d).unwrap_or_default();
    let time = NaiveTime::from_hms_opt(h, mi, s).unwrap_or_default();
    let naive = NaiveDateTime::new(date, time);
    let utc_dt = Utc.from_utc_datetime(&naive);
    let local_dt = utc_dt.with_timezone(&Local);
    local_dt.offset().local_minus_utc() as i64
}

#[cfg(target_arch = "wasm32")]
fn utc_offset_for_utc_datetime(_y: i32, _mo: u32, _d: u32, _h: u32, _mi: u32, _s: u32) -> i64 {
    0
}

/// First instant outside the range date.c trusts `localtime_r()` with:
/// before 1970-01-01 or after 2038-01-18 00:00:00 UTC.
const LOCALTIME_JDN_MIN: f64 = 2_440_587.5;
const LOCALTIME_JDN_MAX: f64 = 2_465_441.5;

/// Compute the UTC offset for the `localtime` modifier (UTC → local).
///
/// date.c `toLocaltime`: outside 1970-01-01..2038-01-18 it asks
/// `localtime_r()` about the same wall-clock date and time in year
/// `2000 + Y%4` (same leap-ness), so ancient and far-future dates get that
/// year's rules rather than the zone's history (LMT, wartime offsets).
fn utc_offset_for_utc_jdn(jdn: f64) -> i64 {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, _frac) = jdn_to_hms(jdn);
    let y = if (LOCALTIME_JDN_MIN..=LOCALTIME_JDN_MAX).contains(&jdn) {
        y
    } else {
        2000 + y % 4
    };
    utc_offset_for_utc_datetime(y as i32, mo as u32, d as u32, h as u32, mi as u32, s as u32)
}

/// Convert a local-time JDN to UTC for the `utc` modifier, mirroring SQLite's
/// iterative fixed-point solver (date.c, the `utc` modifier case): find the UTC
/// instant whose localtime() reproduces the given local wall-clock, iterating up
/// to 4 times. Unlike a single-offset lookup, this resolves DST-transition
/// ambiguity to the pre-transition offset exactly as stock does — both the
/// spring-forward GAP (a nonexistent local time) and the fall-back FOLD (a local
/// time that occurs twice) — while staying exact for all unambiguous times.
/// bd-tl6ly.
#[cfg(not(target_arch = "wasm32"))]
fn local_jdn_to_utc_jdn(local_jdn: f64) -> f64 {
    let orig = local_jdn;
    let mut guess = orig;
    let mut err = 0.0_f64;
    let mut cnt = 0u32;
    loop {
        guess -= err;
        // localtime(guess-as-UTC), expressed as a JDN, is `guess` plus the local
        // UTC offset in effect at that instant.
        let off = utc_offset_for_utc_jdn(guess);
        let localized = guess + off as f64 / 86400.0;
        err = localized - orig;
        // Mirror sqlite's `while( iErr && cnt++<3 )`: real-timezone offsets are
        // whole minutes, so a sub-second residual is convergence; the 4-iteration
        // cap makes the gap's ±1h oscillation terminate on the same UTC value
        // stock lands on.
        if err.abs() < 0.5 / 86400.0 || cnt >= 3 {
            break;
        }
        cnt += 1;
    }
    guess
}

#[cfg(target_arch = "wasm32")]
fn local_jdn_to_utc_jdn(local_jdn: f64) -> f64 {
    // wasm has no host-local timezone provider through this path; `utc` is a no-op.
    local_jdn
}

// ── Julian Day Number Conversions ─────────────────────────────────────────
//
// Algorithms from Meeus, "Astronomical Algorithms" (1991).

/// Gregorian (y, m, d, h, min, sec, frac_sec) → Julian Day Number.
///
/// SQLite's computeJD (date.c) exactly: its century term `(Y+4800)/100` stays
/// positive for every supported year, where Meeus's `Y/100` with truncating
/// division put BCE dates a day late.
fn ymd_to_jdn(y: i64, m: i64, d: i64) -> f64 {
    let (y, m) = if m <= 2 {
        (y.saturating_sub(1), m.saturating_add(12))
    } else {
        (y, m)
    };
    let a = y.saturating_add(4800) / 100;
    let b = 38_i64.saturating_sub(a).saturating_add(a / 4);
    let x1 = 36525_i64.saturating_mul(y.saturating_add(4716)) / 100;
    let x2 = 306_001_i64.saturating_mul(m.saturating_add(1)) / 10000;
    x1.saturating_add(x2).saturating_add(d).saturating_add(b) as f64 - 1524.5
}

/// Julian Day Number → Gregorian (year, month, day).
///
/// Uses saturating/wrapping-safe arithmetic so that extreme JDN values
/// (from overflowed modifier chains) produce deterministic garbage rather
/// than panicking.  Callers that care about validity should bounds-check
/// the JDN before calling.
fn jdn_to_ymd(jdn: f64) -> (i64, i64, i64) {
    // Mirror SQLite's computeYMD (date.c): a proleptic-Gregorian conversion that
    // ALWAYS applies the centurial leap correction, using C-style truncation
    // toward zero (Rust `as i64` casts, not `floor`) so that extreme pre-historic
    // JDN values near 0 agree with SQLite (JDN 0 → -4713-11-24 12:00) instead of
    // diverging via a mixed Julian/Gregorian calendar split.
    let z = (jdn + 0.5).floor() as i64;
    let alpha = ((z as f64 - 1_867_216.25) / 36524.25) as i64;
    let a = z
        .saturating_add(1)
        .saturating_add(alpha)
        .saturating_sub(alpha / 4);
    let b = a.saturating_add(1524);
    let c = ((b as f64 - 122.1) / 365.25) as i64;
    let d = (365.25 * c as f64) as i64;
    let e = ((b.saturating_sub(d)) as f64 / 30.6001) as i64;

    let day = b
        .saturating_sub(d)
        .saturating_sub((30.6001 * e as f64) as i64);
    let month = if e < 14 {
        e.saturating_sub(1)
    } else {
        e.saturating_sub(13)
    };
    let year = if month > 2 {
        c.saturating_sub(4716)
    } else {
        c.saturating_sub(4715)
    };
    (year, month, day)
}

/// Julian Day Number → (hour, minute, second, fractional_sec).
fn jdn_to_hms(jdn: f64) -> (i64, i64, i64, f64) {
    let frac = jdn + 0.5 - (jdn + 0.5).floor();
    // Round to nearest millisecond to avoid floating-point drift.
    let total_ms = (frac * 86_400_000.0).round() as i64;
    let h = total_ms / 3_600_000;
    let rem = total_ms % 3_600_000;
    let m = rem / 60_000;
    let rem = rem % 60_000;
    let s = rem / 1000;
    let ms_frac = (rem % 1000) as f64 / 1000.0;
    (h, m, s, ms_frac)
}

/// Build a JDN from date + time components.
fn ymdhms_to_jdn(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, frac: f64) -> f64 {
    ymd_to_jdn(y, mo, d) + (h as f64 * 3600.0 + mi as f64 * 60.0 + s as f64 + frac) / 86400.0
}

/// Unix epoch as JDN.
const UNIX_EPOCH_JDN: f64 = 2_440_587.5;
/// Upper bound for values interpreted as Julian day by the `auto` modifier.
const AUTO_JDN_MAX: f64 = 5_373_484.499_999;
/// date.c validJulianDay: the last representable instant,
/// 9999-12-31 23:59:59.999, in milliseconds since Julian day 0.
const MAX_VALID_JULIAN_DAY_MS: f64 = 464_269_060_799_999.0;

/// date.c validJulianDay on the millisecond value stock computes from `jdn`.
fn is_valid_julian_day(jdn: f64) -> bool {
    (0.0..=MAX_VALID_JULIAN_DAY_MS).contains(&(jdn * 86_400_000.0).round())
}
/// Unix timestamp bounds used by SQLite's `auto` modifier.
const AUTO_UNIX_MIN: f64 = -210_866_760_000.0;
const AUTO_UNIX_MAX: f64 = 253_402_300_799.0;

fn jdn_to_unix_millis(jdn: f64) -> i64 {
    ((jdn - UNIX_EPOCH_JDN) * 86_400_000.0).round() as i64
}

fn jdn_to_unix(jdn: f64) -> i64 {
    // SQLite first normalizes its internal Julian day to milliseconds, then
    // floors to the containing Unix second. `div_euclid` is important before
    // the epoch: -1ms belongs to Unix second -1, not 0.
    jdn_to_unix_millis(jdn).div_euclid(1000)
}

fn jdn_to_unix_subsec(jdn: f64) -> f64 {
    jdn_to_unix_millis(jdn) as f64 / 1000.0
}

fn unix_to_jdn(ts: f64) -> f64 {
    ts / 86400.0 + UNIX_EPOCH_JDN
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn day_of_year(y: i64, m: i64, d: i64) -> i64 {
    let mut doy = d;
    for mo in 1..m {
        doy = doy.saturating_add(days_in_month(y, mo));
    }
    doy
}

// ── Time String Parsing ───────────────────────────────────────────────────

/// A time string parsed the way date.c's `parseDateOrTime` leaves its
/// `DateTime`: the Julian day plus the state stock keeps beside it.
#[derive(Clone, Copy)]
struct ParsedTimestring {
    jdn: f64,
    /// `isUtc`: the value is 'now'/'subsec' or the text carried `Z` or a
    /// `±HH:MM` offset, so a later 'utc' modifier leaves it alone.
    is_utc: bool,
    /// The `Y-M-D` fields as written (`validYMD`), which stock prints without
    /// normalizing while no modifier recomputes them.
    ymd: Option<(i64, i64, i64)>,
    /// The time fields as written (`validHMS`).
    hms: Option<UnmodifiedHms>,
    /// `nFloor` for the date as written: its day-of-month overflow, which a
    /// 'floor' modifier subtracts (`2023-02-30` floors to `2023-02-28`).
    n_floor: i64,
}

impl ParsedTimestring {
    fn julian_day(jdn: f64, is_utc: bool) -> Self {
        Self {
            jdn,
            is_utc,
            ymd: None,
            hms: None,
            n_floor: 0,
        }
    }
}

/// Parse a SQLite time string into a JDN.
fn parse_timestring(s: &str) -> Option<ParsedTimestring> {
    // sqlite3_value_text() exposes a NUL-terminated C string to date.c. Bytes
    // after the first embedded NUL are therefore not part of the time value.
    let s = sqlite_c_string_str(s);

    // Special values are exact (apart from ASCII case): unlike numeric input,
    // SQLite does not ignore surrounding whitespace around these keywords.
    // C SQLite (date.c:451) captures time once per sqlite3_step() via the VFS
    // and caches it; we use SystemTime::now() which gives per-call resolution.
    // A future refinement (Track: Cx time source) could freeze time at
    // statement start for full C SQLite compatibility.
    if s.eq_ignore_ascii_case("now")
        || s.eq_ignore_ascii_case("subsec")
        || s.eq_ignore_ascii_case("subsecond")
    {
        return Some(ParsedTimestring::julian_day(current_time_jdn(), true));
    }

    // Try as a Julian Day Number (bare float).
    // Reject non-finite values (NaN/Inf) — sqlite3AtoF doesn't recognize them.
    let numeric = s.trim_matches(|c: char| c.is_ascii_whitespace());
    if let Ok(jdn) = numeric.parse::<f64>()
        && jdn >= 0.0
        && jdn.is_finite()
    {
        return Some(ParsedTimestring::julian_day(jdn, false));
    }

    parse_yyyy_mm_dd(s.as_bytes()).or_else(|| parse_bare_time(s.as_bytes()))
}

fn current_time_jdn() -> f64 {
    // GH #175: C SQLite reads the wall clock once per sqlite3_step() and reuses
    // it for every `'now'`/`CURRENT_*` within that statement. Capture lazily on
    // the first use this statement and cache it, so `julianday('now')` is stable
    // across the rows a single statement produces (the Connection resets the
    // cache at each statement start).
    if let Some(cached) = crate::builtins::statement_now() {
        return cached;
    }
    use fsqlite_types::sync_primitives::SystemTime;

    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let jdn = UNIX_EPOCH_JDN + secs / 86_400.0;
    crate::builtins::set_statement_now(jdn);
    jdn
}

fn sqlite_c_string_bytes(bytes: &[u8]) -> &[u8] {
    bytes
        .iter()
        .position(|&byte| byte == 0)
        .map_or(bytes, |nul| &bytes[..nul])
}

fn sqlite_c_string_str(text: &str) -> &str {
    let bytes = sqlite_c_string_bytes(text.as_bytes());
    // `bytes` is a prefix of an already-valid UTF-8 string. A NUL is one byte
    // and therefore always lies on a character boundary.
    std::str::from_utf8(bytes).unwrap_or("")
}

fn sqlite_value_datetime_text(value: &SqliteValue) -> Option<Cow<'_, str>> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Text(text) => Some(Cow::Borrowed(sqlite_c_string_str(text))),
        SqliteValue::Blob(bytes) => std::str::from_utf8(sqlite_c_string_bytes(bytes))
            .ok()
            .map(Cow::Borrowed),
        SqliteValue::Integer(_) | SqliteValue::Float(_) => Some(Cow::Owned(value.to_text())),
    }
}

/// date.c `parseYyyyMmDd`: `[-]YYYY-MM-DD` (a leading '-' makes the year
/// BCE), then any run of spaces and `T`s, then either nothing or a time that
/// [`parse_hh_mm_ss`] accepts.
fn parse_yyyy_mm_dd(s: &[u8]) -> Option<ParsedTimestring> {
    let (negative, s) = match s {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, s),
    };
    let (y, s) = get_digits(s, 4, 0, 9999, Some(b'-'))?;
    let (m, s) = get_digits(s, 2, 1, 12, Some(b'-'))?;
    let (d, s) = get_digits(s, 2, 1, 31, None)?;
    let separator = s
        .iter()
        .take_while(|&&b| is_sqlite_space(b) || b == b'T')
        .count();
    let s = &s[separator..];
    let y = if negative { -y } else { y };
    let n_floor = compute_floor(y, m, d);
    if s.is_empty() {
        return Some(ParsedTimestring {
            jdn: ymd_to_jdn(y, m, d),
            is_utc: false,
            ymd: Some((y, m, d)),
            hms: None,
            n_floor,
        });
    }
    let time = parse_hh_mm_ss(s)?;
    let hms = time.hms;
    let jdn = ymdhms_to_jdn(y, m, d, hms.hour, hms.minute, hms.second, hms.fraction)
        - time.tz_minutes as f64 / 1440.0;
    // A non-zero offset makes stock recompute every field from the Julian day.
    let as_written = time.tz_minutes == 0;
    Some(ParsedTimestring {
        jdn,
        is_utc: time.has_tz,
        ymd: as_written.then_some((y, m, d)),
        hms: as_written.then_some(hms),
        n_floor,
    })
}

/// A time with no date, which date.c places on 2000-01-01.
fn parse_bare_time(s: &[u8]) -> Option<ParsedTimestring> {
    let time = parse_hh_mm_ss(s)?;
    let hms = time.hms;
    let jdn = ymdhms_to_jdn(2000, 1, 1, hms.hour, hms.minute, hms.second, hms.fraction)
        - time.tz_minutes as f64 / 1440.0;
    Some(ParsedTimestring {
        jdn,
        is_utc: time.has_tz,
        ymd: None,
        hms: (time.tz_minutes == 0).then_some(hms),
        n_floor: 0,
    })
}

/// What date.c's `parseHhMmSs` reads from a time and its timezone suffix.
#[derive(Clone, Copy)]
struct ParsedHms {
    hms: UnmodifiedHms,
    /// Minutes east of UTC given by a `±HH:MM` suffix.
    tz_minutes: i64,
    /// Whether a `Z` or `±HH:MM` suffix was present.
    has_tz: bool,
}

/// date.c `parseHhMmSs`: `HH:MM[:SS[.F...]]`, each field exactly two digits,
/// hours 0..24 and minutes and seconds 0..59. The fraction needs a digit after
/// the '.' and is truncated at .999. A timezone suffix may follow.
fn parse_hh_mm_ss(s: &[u8]) -> Option<ParsedHms> {
    let (hour, s) = get_digits(s, 2, 0, 24, Some(b':'))?;
    let (minute, mut s) = get_digits(s, 2, 0, 59, None)?;
    let mut second = 0;
    let mut fraction = 0.0_f64;
    if let [b':', rest @ ..] = s {
        let (whole, rest) = get_digits(rest, 2, 0, 59, None)?;
        second = whole;
        s = rest;
        if let [b'.', first, ..] = s
            && first.is_ascii_digit()
        {
            let count = s[1..].iter().take_while(|b| b.is_ascii_digit()).count();
            let mut scale = 1.0_f64;
            for &b in &s[1..=count] {
                fraction = fraction * 10.0 + f64::from(b - b'0');
                scale *= 10.0;
            }
            fraction /= scale;
            if fraction > 0.999 {
                fraction = 0.999;
            }
            s = &s[1 + count..];
        }
    }
    let (tz_minutes, has_tz) = parse_timezone(s)?;
    Some(ParsedHms {
        hms: UnmodifiedHms {
            hour,
            minute,
            second,
            fraction,
        },
        tz_minutes,
        has_tz,
    })
}

/// date.c `parseTimezone`: optional spaces, then nothing, `Z`/`z`, or
/// `±HH:MM` with hours 0..14, then optional spaces and the end of the text.
/// Returns the offset in minutes east of UTC and whether a suffix was present.
fn parse_timezone(s: &[u8]) -> Option<(i64, bool)> {
    let (tz_minutes, rest) = match skip_sqlite_spaces(s) {
        [] => return Some((0, false)),
        [b'Z' | b'z', rest @ ..] => (0, rest),
        [sign @ (b'+' | b'-'), rest @ ..] => {
            let (hours, rest) = get_digits(rest, 2, 0, 14, Some(b':'))?;
            let (minutes, rest) = get_digits(rest, 2, 0, 59, None)?;
            let offset = hours * 60 + minutes;
            (if *sign == b'-' { -offset } else { offset }, rest)
        }
        _ => return None,
    };
    skip_sqlite_spaces(rest)
        .is_empty()
        .then_some((tz_minutes, true))
}

/// One field of date.c `getDigits`: exactly `n` ASCII digits with a value in
/// `min..=max`, followed by `separator` when one is given. Returns the value
/// and the text after the field and its separator.
fn get_digits(
    s: &[u8],
    n: usize,
    min: i64,
    max: i64,
    separator: Option<u8>,
) -> Option<(i64, &[u8])> {
    let digits = s.get(..n)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value = digits
        .iter()
        .fold(0_i64, |acc, &b| acc * 10 + i64::from(b - b'0'));
    if !(min..=max).contains(&value) {
        return None;
    }
    let rest = &s[n..];
    match separator {
        None => Some((value, rest)),
        Some(separator) => match rest {
            [first, after @ ..] if *first == separator => Some((value, after)),
            _ => None,
        },
    }
}

/// `sqlite3Isspace`: space, `\t`, `\n`, `\v`, `\f` and `\r`.
fn is_sqlite_space(b: u8) -> bool {
    b == b' ' || (b'\t'..=b'\r').contains(&b)
}

fn skip_sqlite_spaces(s: &[u8]) -> &[u8] {
    let count = s.iter().take_while(|&&b| is_sqlite_space(b)).count();
    &s[count..]
}

// ── Modifier Pipeline ─────────────────────────────────────────────────────

/// Apply a single modifier string to a JDN.  Returns None if invalid.
fn apply_modifier(jdn: f64, modifier: &str, written_ymd: Option<(i64, i64, i64)>) -> Option<f64> {
    if has_outer_ascii_whitespace(modifier) {
        return None;
    }
    let m = modifier.to_ascii_lowercase();

    // 'start of month' / 'start of year' / 'start of day'
    // Like date.c's computeYMD when validYMD is still set, take Y-M-D from the
    // written fields when no prior modifier has recomputed them (bd-a20bp).
    if m == "start of month" {
        let (y, mo, _d) = written_ymd.unwrap_or_else(|| jdn_to_ymd(jdn));
        return Some(ymd_to_jdn(y, mo, 1));
    }
    if m == "start of year" {
        let (y, _mo, _d) = written_ymd.unwrap_or_else(|| jdn_to_ymd(jdn));
        return Some(ymd_to_jdn(y, 1, 1));
    }
    if m == "start of day" {
        let (y, mo, d) = written_ymd.unwrap_or_else(|| jdn_to_ymd(jdn));
        return Some(ymd_to_jdn(y, mo, d));
    }

    // 'unixepoch' — reinterpret input as Unix timestamp.
    if m == "unixepoch" {
        return Some(unix_to_jdn(jdn));
    }

    // 'julianday' — input is already a JDN (no-op here, but the spec says
    // it forces interpretation as JDN).
    if m == "julianday" {
        return Some(jdn);
    }

    // 'auto' — apply SQLite numeric auto-detection:
    //   0.0..=5373484.499999          => Julian day number
    //   -210866760000..=253402300799  => Unix timestamp
    //   otherwise                      => NULL
    if m == "auto" {
        if (0.0..=AUTO_JDN_MAX).contains(&jdn) {
            return Some(jdn);
        }
        if (AUTO_UNIX_MIN..=AUTO_UNIX_MAX).contains(&jdn) {
            return Some(unix_to_jdn(jdn));
        }
        return None;
    }

    // 'localtime' — convert UTC to local time.  The input JDN is UTC, so we
    // must compute the offset by interpreting the datetime as UTC (not local).
    if m == "localtime" {
        let offset = utc_offset_for_utc_jdn(jdn);
        return Some(jdn + offset as f64 / 86400.0);
    }
    // 'utc' — convert local time to UTC via sqlite's iterative fixed-point
    // solver, so a DST-transition-ambiguous local time resolves to the
    // pre-transition offset exactly as stock does (bd-tl6ly).
    if m == "utc" {
        return Some(local_jdn_to_utc_jdn(jdn));
    }

    // 'subsec' / 'subsecond' — this is a flag that affects output formatting,
    // not the JDN.  We pass it through unchanged.
    if m == "subsec" || m == "subsecond" {
        return Some(jdn);
    }

    // 'weekday N' — advance to the next day that is weekday N (0=Sunday).
    if let Some(rest) = m.strip_prefix("weekday ") {
        let wd = rest.trim().parse::<i64>().ok()?;
        if !(0..=6).contains(&wd) {
            return None;
        }
        // Current day of week: 0=Monday in JDN, but SQLite uses 0=Sunday.
        let current_jdn_int = (jdn + 0.5).floor() as i64;
        let current_wd = (current_jdn_int + 1) % 7; // 0=Sunday
        let mut diff = wd - current_wd;
        if diff < 0 {
            diff += 7;
        }
        // If already the target weekday, this is a no-op (SQLite behavior).
        return Some(jdn + diff as f64);
    }

    // Arithmetic: '+NNN days', '-NNN hours', etc.
    parse_arithmetic_modifier(&m).map(|delta| jdn + delta)
}

/// Parse "NNN unit" / "+NNN unit" / "-NNN unit" and return the JDN delta.
/// SQLite's date grammar makes the sign optional: a bare `NNN units` modifier is
/// positive (`date('now', '7 days')` == `date('now', '+7 days')`).
fn parse_arithmetic_modifier(m: &str) -> Option<f64> {
    let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
        (1.0, r.trim())
    } else if let Some(r) = m.strip_prefix('-') {
        (-1.0, r.trim())
    } else {
        (1.0, m.trim())
    };

    let mut parts = rest.splitn(2, ' ');
    let num_str = parts.next()?;
    let unit = parts.next()?.trim();

    // Reject non-finite (NaN/Inf) — sqlite3AtoF doesn't recognize them.
    let num = num_str.parse::<f64>().ok().filter(|f| f.is_finite())?;
    let delta = num * sign;

    match unit.trim_end_matches('s') {
        "day" => Some(delta),
        "hour" => Some(delta / 24.0),
        "minute" => Some(delta / 1440.0),
        "second" => Some(delta / 86400.0),
        "month" => Some(apply_month_delta(delta)),
        "year" => Some(apply_month_delta(delta * 12.0)),
        _ => None,
    }
}

/// For month/year arithmetic, we can't simply add a JDN delta because months
/// have variable lengths.  This returns a JDN delta that is *approximately*
/// correct.  A fully correct implementation requires decomposing and
/// recomposing, which we handle in `apply_modifier_full` for month/year cases.
fn apply_month_delta(months: f64) -> f64 {
    // Average month ≈ 30.436875 days.
    months * 30.436875
}

fn has_outer_ascii_whitespace(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_whitespace)
        || value.as_bytes().last().is_some_and(u8::is_ascii_whitespace)
}

/// SQLite's `computeFloor` (date.c): given a (possibly day-of-month-overflowing)
/// Y-M-D, return how many days must be subtracted to bring the date back to the
/// last valid day of month `m`.  Consumed by the `'floor'` datetime modifier.
fn compute_floor(y: i64, m: i64, d: i64) -> i64 {
    if d <= 28 {
        0
    } else if ((1_i64 << m) & 0x15aa) != 0 {
        // 31-day month (Jan, Mar, May, Jul, Aug, Oct, Dec): days 29-31 all valid.
        0
    } else if m != 2 {
        // 30-day month (Apr, Jun, Sep, Nov): only day 31 overflows, by 1.
        i64::from(d == 31)
    } else if y % 4 != 0 || (y % 100 == 0 && y % 400 != 0) {
        d - 28 // non-leap February
    } else {
        d - 29 // leap February
    }
}

/// The time value after its modifiers.
struct ModifiedTime {
    jdn: f64,
    subsec: bool,
    /// No modifier recomputed the fields as written: 'subsec' and a 'utc' on
    /// a value already in UTC leave them alone.
    fields_kept: bool,
}

/// Apply a sequence of modifiers, also tracking the 'subsec' flag.
///
/// Like date.c's `DateTime`, this carries `isUtc` and `isLocal` (so 'utc' on
/// a value already in UTC, and a second 'localtime', change nothing) and
/// `nFloor`, the day-of-month overflow a later 'floor' subtracts.
fn apply_modifiers(start: &ParsedTimeValue, modifiers: &[String]) -> Option<ModifiedTime> {
    let mut j = start.jdn;
    let mut raw_numeric = start.raw_numeric;
    let mut is_utc = start.is_utc;
    let mut is_local = false;
    let mut subsec = false;
    let mut fields_kept = true;
    // Pending day-of-month overflow from the date as written or the most
    // recent month/year shift (SQLite's DateTime.nFloor).
    let mut n_floor = start.n_floor;
    for (index, m) in modifiers.iter().enumerate() {
        if has_outer_ascii_whitespace(m) {
            return None;
        }
        let m_lower = m.to_ascii_lowercase();
        if matches!(m_lower.as_str(), "unixepoch" | "julianday" | "auto") {
            // These reinterpretations are valid only as the first modifier of
            // a raw numeric time value (date.c parseModifier's `idx>1` and
            // `rawS` checks).
            if index != 0 || !raw_numeric {
                return None;
            }
            raw_numeric = false;
        } else if m_lower != "subsec" && m_lower != "subsecond" {
            raw_numeric = false;
        }
        if m_lower == "subsec" || m_lower == "subsecond" {
            subsec = true;
            continue;
        }
        if m_lower == "utc" && is_utc {
            continue;
        }
        let written_ymd = fields_kept.then_some(start.unmodified_ymd).flatten();
        fields_kept = false;
        // 'ceiling' is the default day-of-month-overflow behavior (roll forward
        // into the next month); it merely clears any pending floor adjustment.
        // 'floor' rolls the date back to the last day of the prior month by
        // subtracting the pending overflow days, which it leaves pending.
        if m_lower == "ceiling" {
            n_floor = 0;
            continue;
        }
        if m_lower == "floor" {
            j -= n_floor as f64;
            continue;
        }
        if m_lower == "localtime" {
            if !is_local {
                j += utc_offset_for_utc_jdn(j) as f64 / 86400.0;
            }
            is_local = true;
            is_utc = false;
            continue;
        }
        if m_lower == "utc" {
            // Stock rebuilds its DateTime from the solved Julian day (a
            // memset), which also drops the pending floor and an earlier
            // 'subsec'.
            j = local_jdn_to_utc_jdn(j);
            n_floor = 0;
            subsec = false;
            is_utc = true;
            is_local = false;
            continue;
        }
        // (+|-)YYYY-MM-DD[ HH:MM...] and [+|-]HH:MM[:SS[.FFF]] offsets.
        match apply_signed_offset_modifier(j, &m_lower) {
            Ok(Some((new_jdn, floor))) => {
                j = new_jdn;
                if let Some(floor) = floor {
                    n_floor = floor;
                }
                continue;
            }
            Ok(None) => return None,
            Err(()) => {}
        }
        // Month/year modifiers need special handling for exact date math.
        // If exact arithmetic overflows (returns None), the modifier is
        // out of representable range — return NULL rather than falling
        // through to the approximate path which would produce overflow
        // panics in jdn_to_ymd.
        if is_month_year_modifier(&m_lower) {
            match apply_month_year_exact(j, &m_lower) {
                Ok(Some((new_jdn, nf))) => {
                    j = new_jdn;
                    n_floor = nf;
                    continue;
                }
                Ok(None) => return None,
                Err(()) => {
                    // Fall through to `apply_modifier` for fractional values
                }
            }
        }
        // A `NNN units` shift clears the pending floor; the named modifiers
        // ('start of ...', 'weekday N', ...) keep it, as in date.c.
        if m_lower.starts_with(|c: char| c == '+' || c == '-' || c.is_ascii_digit()) {
            n_floor = 0;
        }
        j = apply_modifier(j, m, written_ymd)?;
    }
    Some(ModifiedTime {
        jdn: j,
        subsec,
        fields_kept,
    })
}

fn is_month_year_modifier(m: &str) -> bool {
    // A month/year arithmetic modifier, signed OR unsigned (SQLite treats a bare
    // `NNN months` as positive). Named modifiers that merely contain the words
    // ("start of month"/"start of year") reach `apply_month_year_exact`, fail its
    // numeric parse, and fall through to `apply_modifier` unchanged.
    m.contains("month") || m.contains("year")
}

/// Exact month/year arithmetic by decomposing to YMD.
/// Returns Ok(Some((jdn, n_floor))) for exact application, where `n_floor` is
/// the day-of-month overflow (days to subtract for a later `'floor'` modifier).
/// Returns Ok(None) for overflow.
/// Returns Err(()) if the modifier is not an integer, so it should fall back.
fn apply_month_year_exact(jdn: f64, m: &str) -> std::result::Result<Option<(f64, i64)>, ()> {
    let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
        (1_i64, r.trim())
    } else if let Some(r) = m.strip_prefix('-') {
        (-1_i64, r.trim())
    } else {
        // Unsigned `NNN months/years` is positive (SQLite grammar). A non-numeric
        // leading token (e.g. "start of month") fails the parse below and the
        // caller falls through to `apply_modifier`.
        (1_i64, m.trim())
    };

    let mut parts = rest.splitn(2, ' ');
    let num_str = parts.next().ok_or(())?;
    let unit = parts.next().ok_or(())?.trim();

    // SQLite splits a month/year modifier into an INTEGER count, applied by
    // calendar month/year recomposition, plus a fractional remainder applied as
    // FLAT days AFTER that recomposition: a fractional month adds `frac * 30`
    // days, a fractional year adds `frac * 365` days, signed like the modifier
    // (bd-datetime-fractional-month-year-ag5fo). So `+1.5 months` on 2020-01-15 is +1 month (→ 02-15) then +15
    // days (→ 03-01), NOT `1.5 * average-month-length`. Integer-only values leave
    // the fractional term at zero and take the pure calendar path unchanged.
    let (int_num, num_frac) = if let Ok(n) = num_str.parse::<i64>() {
        (n, 0.0_f64)
    } else if let Ok(f) = num_str.parse::<f64>() {
        let int_part = f.trunc();
        if !f.is_finite() || int_part < i64::MIN as f64 || int_part > i64::MAX as f64 {
            return Err(());
        }
        // `num_str` is the unsigned magnitude (the sign was stripped above), so
        // `int_part >= 0` and `num_frac` is in [0, 1); the modifier `sign` gives
        // both the recomposition direction and the fractional-day direction.
        (int_part as i64, f - int_part)
    } else {
        return Err(());
    };

    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);

    let unit = unit.trim_end_matches('s');
    let (total_months, frac_days) = match unit {
        "month" => {
            let Some(months) = int_num.checked_mul(sign) else {
                return Ok(None);
            };
            (months, num_frac * sign as f64 * 30.0)
        }
        "year" => {
            let Some(months) = int_num.checked_mul(sign).and_then(|v| v.checked_mul(12)) else {
                return Ok(None);
            };
            (months, num_frac * sign as f64 * 365.0)
        }
        _ => return Err(()),
    };

    // (y * 12 + (mo - 1)) + total_months
    let current_months = if let Some(val) = y.checked_mul(12).and_then(|v| v.checked_add(mo - 1)) {
        val
    } else {
        return Ok(None);
    };
    let new_total = if let Some(val) = current_months.checked_add(total_months) {
        val
    } else {
        return Ok(None);
    };

    let new_y = new_total.div_euclid(12);
    let new_mo = new_total.rem_euclid(12) + 1;
    // Do NOT clamp `d` to the target month's day count.  C SQLite lets
    // out-of-range days overflow via JDN arithmetic (e.g. Feb 31 → Mar 3) for
    // the default/`'ceiling'` behavior; the `'floor'` modifier later subtracts
    // the reported `n_floor` overflow days to clamp back to end-of-month.
    let n_floor = compute_floor(new_y, new_mo, d);
    Ok(Some((
        ymdhms_to_jdn(new_y, new_mo, d, h, mi, s, frac) + frac_days,
        n_floor,
    )))
}

/// Exactly `count` ASCII digits at the start of `s`, as a number.
fn leading_digits(s: &str, count: usize) -> Option<i64> {
    let digits = s.get(..count)?;
    digits
        .bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| digits.parse().ok())
        .flatten()
}

/// SQLite's signed date/time offset modifiers (date.c `parseModifier`):
///
/// * `(+|-)YYYY-MM-DD` (4 or 5 year digits) shifts by years, months (0..11) and
///   days (0..30): years and months by calendar recomposition, days flat. It
///   may be followed by ` HH:MM[:SS[.FFF]]`, applied as below with the same
///   sign. The day-of-month overflow is reported for a later `'floor'`.
/// * `[+|-]HH:MM[:SS[.FFF]]` shifts the time by that much, taken modulo one
///   day as stock's computation does (`+24:00` is a no-op).
///
/// Returns `Err(())` when `m` has neither form, so the caller tries the
/// `NNN units` grammar; `Ok(None)` when it has one but a field is invalid
/// (stock's NULL). `Ok(Some((jdn, floor)))` carries the new pending floor
/// for the date form and `None` (unchanged) for the time-only form.
fn apply_signed_offset_modifier(
    jdn: f64,
    m: &str,
) -> std::result::Result<Option<(f64, Option<i64>)>, ()> {
    let sign = match m.as_bytes().first() {
        Some(b'+') => Some(1_i64),
        Some(b'-') => Some(-1_i64),
        Some(b) if b.is_ascii_digit() => None,
        _ => return Err(()),
    };
    let body = if sign.is_some() { &m[1..] } else { m };

    // (+|-)YYYY-MM-DD[ HH:MM[:SS[.FFF]]]
    let year_digits = [4_usize, 5]
        .into_iter()
        .find(|&n| body.as_bytes().get(n) == Some(&b'-') && leading_digits(body, n).is_some());
    if let (Some(sign), Some(year_digits)) = (sign, year_digits) {
        let years = leading_digits(body, year_digits).ok_or(())?;
        let rest = &body[year_digits + 1..];
        let (Some(months), Some(b'-'), Some(days)) = (
            leading_digits(rest, 2),
            rest.as_bytes().get(2),
            rest.get(3..).and_then(|tail| leading_digits(tail, 2)),
        ) else {
            return Ok(None);
        };
        if months >= 12 || days >= 31 {
            return Ok(None);
        }
        let (y, mo, d) = jdn_to_ymd(jdn);
        let (h, mi, s, frac) = jdn_to_hms(jdn);
        let shifted_months = (y * 12 + (mo - 1)) + sign * (years * 12 + months);
        let new_y = shifted_months.div_euclid(12);
        let new_mo = shifted_months.rem_euclid(12) + 1;
        let n_floor = compute_floor(new_y, new_mo, d);
        let shifted = ymdhms_to_jdn(new_y, new_mo, d, h, mi, s, frac) + (sign * days) as f64;
        let time = &rest[5..];
        if time.is_empty() {
            return Ok(Some((shifted, Some(n_floor))));
        }
        let Some(time) = time.strip_prefix(' ') else {
            return Ok(None);
        };
        return Ok(
            time_of_day_offset(time).map(|delta| (shifted + sign as f64 * delta, Some(n_floor)))
        );
    }

    // [+|-]HH:MM[:SS[.FFF]]: a numeric prefix ending at ':'.
    let Some(colon) = body.find(':') else {
        return Err(());
    };
    if colon == 0 || !body[..colon].bytes().all(|b| b.is_ascii_digit()) {
        return Err(());
    }
    Ok(time_of_day_offset(body).map(|delta| (jdn + sign.unwrap_or(1) as f64 * delta, None)))
}

/// `HH:MM[:SS[.FFF]]` as a fraction of a day for the offset modifiers,
/// following date.c: hours 0..24, minutes and seconds 0..59, fractional
/// seconds truncated at .999, rounded to the millisecond, modulo one day.
fn time_of_day_offset(time: &str) -> Option<f64> {
    let hours = leading_digits(time, 2).filter(|&h| h <= 24)?;
    if time.as_bytes().get(2) != Some(&b':') {
        return None;
    }
    let minutes = leading_digits(&time[3..], 2).filter(|&m| m <= 59)?;
    let mut rest = &time[5..];
    let seconds = if let Some(after_colon) = rest.strip_prefix(':') {
        let whole = leading_digits(after_colon, 2).filter(|&s| s <= 59)?;
        rest = &after_colon[2..];
        let mut fraction = 0.0_f64;
        if let Some(digits) = rest.strip_prefix('.')
            && digits.as_bytes().first().is_some_and(u8::is_ascii_digit)
        {
            let count = digits.bytes().take_while(u8::is_ascii_digit).count();
            let mut scale = 1.0_f64;
            for b in digits[..count].bytes() {
                fraction = fraction * 10.0 + f64::from(b - b'0');
                scale *= 10.0;
            }
            fraction = (fraction / scale).min(0.999);
            rest = &digits[count..];
        }
        whole as f64 + fraction
    } else {
        0.0
    };
    if !rest.is_empty() {
        return None;
    }
    let millis = hours * 3_600_000 + minutes * 60_000 + (seconds * 1000.0 + 0.5) as i64;
    Some((millis % 86_400_000) as f64 / 86_400_000.0)
}

// ── Output Formatters ─────────────────────────────────────────────────────

/// Fixed-capacity stack `fmt::Write` target. Any date/time output is at most ~26 bytes
/// (a multi-digit year plus `-MM-DD HH:MM:SS.SSS`), so 48 bytes never overflows for a
/// valid date; `write_str` returns `Err` on overflow so `build_small_text` can fall back.
struct StackStr {
    buf: [u8; 48],
    len: usize,
}

impl StackStr {
    fn new() -> Self {
        Self {
            buf: [0; 48],
            len: 0,
        }
    }

    fn as_str(&self) -> &str {
        // Only ASCII digits/separators are ever written, so this is always valid UTF-8.
        core::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl core::fmt::Write for StackStr {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let end = self.len + s.len();
        if end > self.buf.len() {
            return Err(core::fmt::Error);
        }
        self.buf[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// Render `write` into a stack buffer and build an inline `SmallText` (no heap) when it
/// fits the 23-byte inline capacity — the common case for every real date/time. Falls
/// back to a heap `String` only when the stack buffer overflows (an absurdly large year),
/// re-running `write` (hence `Fn`, not `FnOnce`). Mirrors the stack-backed Soundex result
/// (bd-t2sf9.1): the transient `format!` heap allocation that `SmallText` immediately
/// inlined is eliminated for the hot path.
fn build_small_text(write: impl Fn(&mut dyn core::fmt::Write) -> core::fmt::Result) -> SmallText {
    let mut buf = StackStr::new();
    if write(&mut buf).is_ok() {
        SmallText::new(buf.as_str())
    } else {
        let mut heap = String::new();
        let _ = write(&mut heap);
        SmallText::from_string(heap)
    }
}

/// Write a calendar year the way C SQLite's `date()`/`datetime()` render it: a
/// 4-digit zero-padded field, but with the minus sign kept *outside* the padding
/// for negative (proleptic BC) years — e.g. -1 renders as `-0001`, not `-001`
/// (Rust's `{:04}` would count the sign inside the width). `strftime('%Y')`
/// differs — it keeps the sign inside a 4-wide field — and is handled separately.
fn write_year(w: &mut dyn core::fmt::Write, y: i64) -> core::fmt::Result {
    if y < 0 {
        write!(w, "-{:04}", y.unsigned_abs())
    } else {
        write!(w, "{y:04}")
    }
}

fn format_date(jdn: f64, unmodified: Option<(i64, i64, i64)>) -> SmallText {
    let (y, m, d) = unmodified.unwrap_or_else(|| jdn_to_ymd(jdn));
    build_small_text(move |w| {
        write_year(w, y)?;
        write!(w, "-{m:02}-{d:02}")
    })
}

#[derive(Clone, Copy)]
struct UnmodifiedHms {
    hour: i64,
    minute: i64,
    second: i64,
    fraction: f64,
}

fn hms_for_output(jdn: f64, unmodified: Option<UnmodifiedHms>) -> UnmodifiedHms {
    unmodified.unwrap_or_else(|| {
        let (hour, minute, second, fraction) = jdn_to_hms(jdn);
        UnmodifiedHms {
            hour,
            minute,
            second,
            fraction,
        }
    })
}

fn rounded_second_and_millis(hms: UnmodifiedHms) -> (i64, i64) {
    // date.c rounds the seconds field independently of hours/minutes, as
    // `(int)(1000.0*s + 0.5)`. Parsing already truncated the fraction at .999,
    // so text input never rounds up to second 60.
    let total_millis = ((hms.second as f64 + hms.fraction) * 1000.0 + 0.5).floor() as i64;
    (total_millis / 1000, total_millis % 1000)
}

fn format_time(jdn: f64, subsec: bool, unmodified: Option<UnmodifiedHms>) -> SmallText {
    let hms = hms_for_output(jdn, unmodified);
    let (h, m) = (hms.hour, hms.minute);
    if subsec {
        let (s, ms) = rounded_second_and_millis(hms);
        build_small_text(move |w| write!(w, "{h:02}:{m:02}:{s:02}.{ms:03}"))
    } else {
        let s = hms.second;
        build_small_text(move |w| write!(w, "{h:02}:{m:02}:{s:02}"))
    }
}

fn format_datetime(
    jdn: f64,
    subsec: bool,
    unmodified_ymd: Option<(i64, i64, i64)>,
    unmodified: Option<UnmodifiedHms>,
) -> SmallText {
    let (y, mo, d) = unmodified_ymd.unwrap_or_else(|| jdn_to_ymd(jdn));
    let hms = hms_for_output(jdn, unmodified);
    let (h, mi) = (hms.hour, hms.minute);
    if subsec {
        let (s, ms) = rounded_second_and_millis(hms);
        build_small_text(move |w| {
            write_year(w, y)?;
            write!(w, "-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{ms:03}")
        })
    } else {
        let s = hms.second;
        build_small_text(move |w| {
            write_year(w, y)?;
            write!(w, "-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
        })
    }
}

#[inline]
fn push_format(result: &mut String, args: Arguments<'_>) {
    let _ = result.write_fmt(args);
}

#[inline]
fn push_zero_padded_2(result: &mut String, value: i64) {
    if (0..=99).contains(&value) {
        let value = value as u8;
        result.push(char::from(b'0' + value / 10));
        result.push(char::from(b'0' + value % 10));
    } else {
        push_format(result, format_args!("{value:02}"));
    }
}

#[inline]
fn push_space_padded_2(result: &mut String, value: i64) {
    if (0..=99).contains(&value) {
        let value = value as u8;
        if value >= 10 {
            result.push(char::from(b'0' + value / 10));
        } else {
            result.push(' ');
        }
        result.push(char::from(b'0' + value % 10));
    } else {
        push_format(result, format_args!("{value:>2}"));
    }
}

#[inline]
fn push_zero_padded_3(result: &mut String, value: i64) {
    if (0..=999).contains(&value) {
        let value = value as u16;
        result.push(char::from(b'0' + (value / 100) as u8));
        result.push(char::from(b'0' + ((value / 10) % 10) as u8));
        result.push(char::from(b'0' + (value % 10) as u8));
    } else {
        push_format(result, format_args!("{value:03}"));
    }
}

#[inline]
fn push_zero_padded_4(result: &mut String, value: i64) {
    if (0..=9999).contains(&value) {
        let value = value as u16;
        result.push(char::from(b'0' + (value / 1000) as u8));
        result.push(char::from(b'0' + ((value / 100) % 10) as u8));
        result.push(char::from(b'0' + ((value / 10) % 10) as u8));
        result.push(char::from(b'0' + (value % 10) as u8));
    } else {
        push_format(result, format_args!("{value:04}"));
    }
}

/// strftime format engine.
fn format_strftime(
    fmt: &str,
    jdn: f64,
    subsec: bool,
    unmodified_ymd: Option<(i64, i64, i64)>,
    unmodified: Option<UnmodifiedHms>,
) -> Option<String> {
    // Like date.c, the calendar fields (and %j, counted from January 1 of the
    // written year) come from the fields as written; weekdays from the day.
    let (y, mo, d) = unmodified_ymd.unwrap_or_else(|| jdn_to_ymd(jdn));
    let hms = hms_for_output(jdn, unmodified);
    let (h, mi, s, frac) = (hms.hour, hms.minute, hms.second, hms.fraction);
    let doy = day_of_year(y, mo, d);
    // Day of week: 0=Sunday.
    let jdn_int = (jdn + 0.5).floor() as i64;
    let dow = (jdn_int + 1) % 7; // 0=Sunday, 6=Saturday

    let mut result = String::with_capacity(fmt.len().saturating_add(8));
    let bytes = fmt.as_bytes();
    let mut i = 0;
    let mut literal_start = 0;

    while i < bytes.len() {
        if bytes[i] != b'%' || i + 1 >= bytes.len() {
            i += 1;
            continue;
        }

        result.push_str(&fmt[literal_start..i]);

        let spec_suffix = &fmt[i + 1..];
        let Some(spec) = spec_suffix.chars().next() else {
            break;
        };
        i += 1 + spec.len_utf8();
        literal_start = i;

        match spec {
            'd' => push_zero_padded_2(&mut result, d),
            'e' => push_space_padded_2(&mut result, d),
            'F' => {
                // ISO 8601 date: %Y-%m-%d (bd-luvv8).
                push_zero_padded_4(&mut result, y);
                result.push('-');
                push_zero_padded_2(&mut result, mo);
                result.push('-');
                push_zero_padded_2(&mut result, d);
            }
            'f' => {
                // Seconds with fractional part.
                let total = (s as f64 + frac).min(59.999);
                push_format(&mut result, format_args!("{total:06.3}"));
            }
            'H' => push_zero_padded_2(&mut result, h),
            'I' => {
                // 12-hour clock.
                let h12 = if h == 0 {
                    12
                } else if h > 12 {
                    h - 12
                } else {
                    h
                };
                push_zero_padded_2(&mut result, h12);
            }
            'j' => push_zero_padded_3(&mut result, doy),
            'J' => {
                // Julian day number. Stock renders %J as C `%.16g`: 16
                // significant figures, trailing zeros stripped (so an integer
                // JDN has no ".0"). The previous fixed `{:.15}` emitted 15
                // decimal *places* (≈22 significant digits); the general
                // REAL->text form over-renders (17 figs) and keeps ".0".
                // bd-565ji.
                result.push_str(&crate::builtins::format_float_g(jdn, 16, false, false, 16));
            }
            'k' => {
                // Space-padded 24-hour.
                push_space_padded_2(&mut result, h);
            }
            'l' => {
                // Space-padded 12-hour.
                let h12 = if h == 0 {
                    12
                } else if h > 12 {
                    h - 12
                } else {
                    h
                };
                push_space_padded_2(&mut result, h12);
            }
            'm' => push_zero_padded_2(&mut result, mo),
            'M' => push_zero_padded_2(&mut result, mi),
            'p' => {
                result.push_str(if h < 12 { "AM" } else { "PM" });
            }
            'P' => {
                result.push_str(if h < 12 { "am" } else { "pm" });
            }
            'R' => {
                push_zero_padded_2(&mut result, h);
                result.push(':');
                push_zero_padded_2(&mut result, mi);
            }
            's' => {
                if subsec {
                    let unix = jdn_to_unix_subsec(jdn);
                    push_format(&mut result, format_args!("{unix:.3}"));
                } else {
                    let unix = jdn_to_unix(jdn);
                    push_format(&mut result, format_args!("{unix}"));
                }
            }
            'S' => push_zero_padded_2(&mut result, s),
            'T' => {
                push_zero_padded_2(&mut result, h);
                result.push(':');
                push_zero_padded_2(&mut result, mi);
                result.push(':');
                push_zero_padded_2(&mut result, s);
            }
            'u' => {
                // ISO 8601 day of week: 1=Monday, 7=Sunday.
                let u = if dow == 0 { 7 } else { dow };
                push_format(&mut result, format_args!("{u}"));
            }
            'w' => push_format(&mut result, format_args!("{dow}")),
            'W' => {
                // Week of year (Monday as first day of week, 00-53).
                let w = (doy + 6 - ((dow + 6) % 7)) / 7;
                push_zero_padded_2(&mut result, w);
            }
            'U' => {
                // Week of year (Sunday as first day of week, 00-53): the C
                // strftime rule (0-based yday + 7 - wday)/7, with `dow`
                // already 0=Sunday. Days before the first Sunday are week 00.
                // bd-zv4ra.
                let u = (doy + 6 - dow) / 7;
                push_zero_padded_2(&mut result, u);
            }
            'Y' => push_zero_padded_4(&mut result, y),
            'G' | 'g' | 'V' => {
                // ISO 8601 week-based year/week.
                let (iso_y, iso_w) = iso_week(y, mo, d);
                match spec {
                    'G' => push_zero_padded_4(&mut result, iso_y),
                    'g' => push_zero_padded_2(&mut result, iso_y % 100),
                    'V' => push_zero_padded_2(&mut result, iso_w),
                    _ => unreachable!(),
                }
            }
            '%' => result.push('%'),
            // An unknown/undefined specifier makes stock strftime return NULL
            // for the ENTIRE result (it does not pass the specifier through
            // literally). bd-13mqo.
            _ => return None,
        }
    }

    if literal_start < fmt.len() {
        result.push_str(&fmt[literal_start..]);
    }

    Some(result)
}

/// ISO 8601 week number and year.
fn iso_week(y: i64, m: i64, d: i64) -> (i64, i64) {
    let jdn = ymd_to_jdn(y, m, d);
    let jdn_int = (jdn + 0.5).floor() as i64;
    // ISO day of week: 1=Monday, 7=Sunday.
    let dow = (jdn_int + 1) % 7;
    let iso_dow = if dow == 0 { 7 } else { dow };

    // Thursday of the same week determines the year.
    let thu_jdn = jdn_int + (4 - iso_dow);
    let (thu_y, _, _) = jdn_to_ymd(thu_jdn as f64);

    // Jan 4 is always in week 1 (ISO 8601).
    let jan4_jdn = (ymd_to_jdn(thu_y, 1, 4) + 0.5).floor() as i64;
    let jan4_dow = (jan4_jdn + 1) % 7;
    let jan4_iso_dow = if jan4_dow == 0 { 7 } else { jan4_dow };
    let week1_start = jan4_jdn - (jan4_iso_dow - 1);

    let week = (thu_jdn - week1_start) / 7 + 1;
    (thu_y, week)
}

// ── timediff ──────────────────────────────────────────────────────────────

fn timediff_impl(jdn1: f64, jdn2: f64) -> String {
    let (sign, start_jdn, end_jdn) = if jdn1 >= jdn2 {
        ('+', jdn2, jdn1)
    } else {
        ('-', jdn1, jdn2)
    };

    let (start_y, start_mo, start_d) = jdn_to_ymd(start_jdn);
    let (start_h, start_mi, mut start_s, start_frac) = jdn_to_hms(start_jdn);
    let mut start_ms = (start_frac * 1000.0).round() as i64;
    if start_ms >= 1000 {
        start_ms = 0;
        start_s += 1;
    }

    let (end_y, end_mo, end_d) = jdn_to_ymd(end_jdn);
    let (end_h, end_mi, mut end_s, end_frac) = jdn_to_hms(end_jdn);
    let mut end_ms = (end_frac * 1000.0).round() as i64;
    if end_ms >= 1000 {
        end_ms = 0;
        end_s += 1;
    }

    let mut years = end_y - start_y;
    let mut months = end_mo - start_mo;
    let mut days = end_d - start_d;
    let mut hours = end_h - start_h;
    let mut minutes = end_mi - start_mi;
    let mut seconds = end_s - start_s;
    let mut millis = end_ms - start_ms;

    if millis < 0 {
        millis += 1000;
        seconds -= 1;
    }
    if seconds < 0 {
        seconds += 60;
        minutes -= 1;
    }
    if minutes < 0 {
        minutes += 60;
        hours -= 1;
    }
    if hours < 0 {
        hours += 24;
        days -= 1;
    }
    if days < 0 {
        months -= 1;
        let (borrow_y, borrow_mo) = if end_mo == 1 {
            (end_y - 1, 12)
        } else {
            (end_y, end_mo - 1)
        };
        days += days_in_month(borrow_y, borrow_mo);
    }
    if months < 0 {
        months += 12;
        years -= 1;
    }

    format!(
        "{sign}{years:04}-{months:02}-{days:02} {hours:02}:{minutes:02}:{seconds:02}.{millis:03}"
    )
}

// ── Scalar Function Implementations ───────────────────────────────────────

/// Return whether an evaluated built-in date/time call is safe in a schema
/// expression whose value must remain stable for the lifetime of the schema.
///
/// SQLite's date/time functions are conditionally deterministic. Fixed text,
/// numeric, and row-derived values are safe, but the following forms consult
/// wall-clock or host-local state and must not be used while maintaining an
/// expression index, partial index, generated column, or CHECK constraint:
///
/// - an omitted time value (for example, `date()` or `strftime('%Y')`);
/// - `now` as a time value;
/// - `subsec` or `subsecond` in the first time-value position, where SQLite
///   treats it as shorthand for the current time with subsecond precision;
/// - `localtime` or `utc` in a modifier position.
///
/// The argument layout is function-specific: `strftime`'s format occupies
/// `args[0]`, while both `timediff` arguments are time values and neither is a
/// modifier. NULLs preserve SQLite's left-to-right short-circuit behaviour: a
/// NULL encountered before a conditional feature makes the call return NULL
/// without consulting that feature.
///
/// Names other than the seven built-in date/time functions return `true`.
/// Callers that permit user-defined overrides must resolve function identity
/// before relying on this helper; a custom function merely named `date` is not
/// necessarily governed by SQLite's built-in date/time rules.
#[must_use]
pub fn is_datetime_invocation_safe_for_schema(function_name: &str, args: &[SqliteValue]) -> bool {
    if function_name.eq_ignore_ascii_case("strftime") {
        // C SQLite returns NULL before evaluating the time arguments when the
        // format is absent or NULL. With a non-NULL format, an omitted time
        // value means "now".
        return args.first().is_none_or(SqliteValue::is_null)
            || datetime_arguments_are_schema_safe(&args[1..]);
    }

    if function_name.eq_ignore_ascii_case("timediff") {
        // Wrong-arity calls are rejected by function resolution and never
        // invoke timediff. For the valid arity, each argument is independently
        // parsed as a time value; neither position accepts modifiers.
        if args.len() != 2 {
            return true;
        }
        for time_value in args {
            match classify_time_value_for_schema(time_value) {
                SchemaTimeValue::Dynamic => return false,
                SchemaTimeValue::NullOrInvalid => return true,
                SchemaTimeValue::Fixed { .. } => {}
            }
        }
        return true;
    }

    if function_name.eq_ignore_ascii_case("date")
        || function_name.eq_ignore_ascii_case("time")
        || function_name.eq_ignore_ascii_case("datetime")
        || function_name.eq_ignore_ascii_case("julianday")
        || function_name.eq_ignore_ascii_case("unixepoch")
    {
        return datetime_arguments_are_schema_safe(args);
    }

    true
}

/// Check the common `(time-value, modifier...)` date/time call shape.
fn datetime_arguments_are_schema_safe(args: &[SqliteValue]) -> bool {
    let Some(time_value) = args.first() else {
        return false;
    };
    let input = match classify_time_value_for_schema(time_value) {
        SchemaTimeValue::Dynamic => return false,
        SchemaTimeValue::NullOrInvalid => return true,
        SchemaTimeValue::Fixed(input) => input,
    };

    let mut reached_modifiers = Vec::with_capacity(args.len().saturating_sub(1));
    for modifier in &args[1..] {
        if modifier.is_null() {
            return true;
        }
        if sqlite_value_is_keyword(modifier, "localtime")
            || sqlite_value_is_keyword(modifier, "utc")
        {
            return false;
        }
        let Some(modifier) = sqlite_value_datetime_text(modifier) else {
            return true;
        };
        reached_modifiers.push(modifier.into_owned());
        if apply_modifiers(&input, &reached_modifiers).is_none() {
            // Evaluation stops at the first invalid modifier. A conditional
            // token farther to the right is therefore unreachable.
            return true;
        }
    }
    true
}

enum SchemaTimeValue {
    Dynamic,
    NullOrInvalid,
    Fixed(ParsedTimeValue),
}

fn classify_time_value_for_schema(value: &SqliteValue) -> SchemaTimeValue {
    if value.is_null() {
        return SchemaTimeValue::NullOrInvalid;
    }
    if is_implicit_now_time_value(value) {
        return SchemaTimeValue::Dynamic;
    }
    match parse_time_value(value) {
        Some(parsed) => SchemaTimeValue::Fixed(parsed),
        None => SchemaTimeValue::NullOrInvalid,
    }
}

fn is_implicit_now_time_value(value: &SqliteValue) -> bool {
    sqlite_value_is_keyword(value, "now")
        || sqlite_value_is_keyword(value, "subsec")
        || sqlite_value_is_keyword(value, "subsecond")
}

/// Compare SQLite's NUL-terminated text view without allocating. Special
/// date/time words are ASCII-case-insensitive but do not permit surrounding
/// whitespace. Numeric values cannot equal any keyword checked here.
fn sqlite_value_is_keyword(value: &SqliteValue, keyword: &str) -> bool {
    let bytes = match value {
        SqliteValue::Text(text) => sqlite_c_string_bytes(text.as_bytes_direct()),
        SqliteValue::Blob(bytes) => sqlite_c_string_bytes(bytes),
        SqliteValue::Null | SqliteValue::Integer(_) | SqliteValue::Float(_) => return false,
    };
    bytes.eq_ignore_ascii_case(keyword.as_bytes())
}

#[derive(Clone, Copy)]
struct ParsedTimeValue {
    jdn: f64,
    raw_numeric: bool,
    /// date.c `isUtc` before any modifier.
    is_utc: bool,
    /// date.c `nFloor` before any modifier.
    n_floor: i64,
    unmodified_ymd: Option<(i64, i64, i64)>,
    unmodified_hms: Option<UnmodifiedHms>,
}

impl ParsedTimeValue {
    fn raw_number(jdn: f64) -> Self {
        Self {
            jdn,
            raw_numeric: true,
            is_utc: false,
            n_floor: 0,
            unmodified_ymd: None,
            unmodified_hms: None,
        }
    }
}

fn parse_time_value(value: &SqliteValue) -> Option<ParsedTimeValue> {
    match value {
        SqliteValue::Null => None,
        SqliteValue::Integer(integer) => Some(ParsedTimeValue::raw_number(*integer as f64)),
        SqliteValue::Float(float) if float.is_finite() => Some(ParsedTimeValue::raw_number(*float)),
        SqliteValue::Float(_) => None,
        SqliteValue::Text(_) | SqliteValue::Blob(_) => {
            let text = sqlite_value_datetime_text(value)?;
            let numeric = text.trim_matches(|c: char| c.is_ascii_whitespace());
            if let Ok(number) = numeric.parse::<f64>()
                && number.is_finite()
            {
                return Some(ParsedTimeValue::raw_number(number));
            }
            let parsed = parse_timestring(text.as_ref())?;
            Some(ParsedTimeValue {
                jdn: parsed.jdn,
                raw_numeric: false,
                is_utc: parsed.is_utc,
                n_floor: parsed.n_floor,
                unmodified_ymd: parsed.ymd,
                unmodified_hms: parsed.hms,
            })
        }
    }
}

struct ParsedDateTimeArgs {
    jdn: f64,
    subsec: bool,
    unmodified_ymd: Option<(i64, i64, i64)>,
    unmodified_hms: Option<UnmodifiedHms>,
}

/// Parse the common `(time-value, modifier...)` argument layout.
fn parse_args(args: &[SqliteValue]) -> Option<ParsedDateTimeArgs> {
    let first_position_subsec = args.first().is_some_and(|value| {
        sqlite_value_is_keyword(value, "subsec") || sqlite_value_is_keyword(value, "subsecond")
    });
    let parsed = match args.first() {
        None => ParsedTimeValue {
            jdn: current_time_jdn(),
            raw_numeric: false,
            is_utc: true,
            n_floor: 0,
            unmodified_ymd: None,
            unmodified_hms: None,
        },
        Some(value) => parse_time_value(value)?,
    };

    let mut modifiers = Vec::with_capacity(args.len().saturating_sub(1));
    for modifier in args.get(1..).unwrap_or_default() {
        modifiers.push(sqlite_value_datetime_text(modifier)?.into_owned());
    }

    // C SQLite rejects a numeric argument outside the representable
    // Julian-day range (date.c validJulianDay), returning NULL rather than
    // formatting a saturated garbage date — unless the first modifier
    // reinterprets the raw number ('unixepoch', 'julianday', 'auto').
    if parsed.raw_numeric {
        let first = modifiers
            .first()
            .map(|modifier| modifier.to_ascii_lowercase());
        let reinterprets_raw = matches!(first.as_deref(), Some("unixepoch" | "julianday" | "auto"));
        if !reinterprets_raw && !(0.0..=AUTO_JDN_MAX).contains(&parsed.jdn) {
            return None;
        }
    }

    let ModifiedTime {
        jdn,
        subsec: modifier_subsec,
        fields_kept,
    } = apply_modifiers(&parsed, &modifiers)?;
    // C SQLite's date functions return NULL when the *computed* result (after
    // every modifier) leaves the representable Julian-day range — not only when
    // the raw numeric input does (checked above). Mirror date.c's validJulianDay
    // on the final value so a modifier that overflows past 9999-12-31 23:59:59
    // (or below Julian day 0) yields NULL rather than a malformed 5-digit-year
    // string or an out-of-range julianday()/unixepoch() number. Verified against
    // SQLite 3.53: e.g. datetime('9999-12-31 23:59:59','+1 second') is NULL,
    // while datetime('9999-12-31 23:59:59.999','subsec') is still valid.
    if !is_valid_julian_day(jdn) {
        return None;
    }
    // date.c isDate: with no modifier at all, a day of month past 28 is
    // normalized (`2023-02-31` -> `2023-03-03`); otherwise fields that no
    // modifier recomputed are printed as written (`2024-01-01 24:00`).
    let unmodified_ymd = parsed
        .unmodified_ymd
        .filter(|&(_, _, d)| fields_kept && (!modifiers.is_empty() || d <= 28));
    Some(ParsedDateTimeArgs {
        jdn,
        subsec: first_position_subsec || modifier_subsec,
        unmodified_ymd,
        unmodified_hms: parsed.unmodified_hms.filter(|_| fields_kept),
    })
}

// ── date() ────────────────────────────────────────────────────────────────

pub struct DateFunc;

impl ScalarFunction for DateFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            Some(parsed) => Ok(SqliteValue::Text(format_date(
                parsed.jdn,
                parsed.unmodified_ymd,
            ))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "date"
    }
}

// ── time() ────────────────────────────────────────────────────────────────

pub struct TimeFunc;

impl ScalarFunction for TimeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            Some(parsed) => Ok(SqliteValue::Text(format_time(
                parsed.jdn,
                parsed.subsec,
                parsed.unmodified_hms,
            ))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "time"
    }
}

// ── datetime() ────────────────────────────────────────────────────────────

pub struct DateTimeFunc;

impl ScalarFunction for DateTimeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            Some(parsed) => Ok(SqliteValue::Text(format_datetime(
                parsed.jdn,
                parsed.subsec,
                parsed.unmodified_ymd,
                parsed.unmodified_hms,
            ))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "datetime"
    }
}

// ── julianday() ───────────────────────────────────────────────────────────

pub struct JuliandayFunc;

impl ScalarFunction for JuliandayFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            Some(parsed) => Ok(SqliteValue::Float(parsed.jdn)),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "julianday"
    }
}

// ── unixepoch() ───────────────────────────────────────────────────────────

pub struct UnixepochFunc;

impl ScalarFunction for UnixepochFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            // bd-855l7: the 'subsec'/'subsecond' modifier makes unixepoch return
            // a floating-point value carrying the fractional seconds.
            Some(parsed) if parsed.subsec => Ok(SqliteValue::Float(jdn_to_unix_subsec(parsed.jdn))),
            Some(parsed) => Ok(SqliteValue::Integer(jdn_to_unix(parsed.jdn))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "unixepoch"
    }
}

// ── strftime() ────────────────────────────────────────────────────────────

pub struct StrftimeFunc;

impl ScalarFunction for StrftimeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        let Some(format_value) = args.first() else {
            return Ok(SqliteValue::Null);
        };
        let Some(fmt) = sqlite_value_datetime_text(format_value) else {
            return Ok(SqliteValue::Null);
        };
        let rest = &args[1..];
        match parse_args(rest) {
            Some(parsed) => Ok(
                match format_strftime(
                    fmt.as_ref(),
                    parsed.jdn,
                    parsed.subsec,
                    parsed.unmodified_ymd,
                    parsed.unmodified_hms,
                ) {
                    // An unknown specifier in the format yields NULL (bd-13mqo).
                    Some(text) => SqliteValue::Text(text.into()),
                    None => SqliteValue::Null,
                },
            ),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "strftime"
    }
}

// ── timediff() ────────────────────────────────────────────────────────────

pub struct TimediffFunc;

impl ScalarFunction for TimediffFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 2 || args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }

        let jdn1 = parse_time_value(&args[0])
            .map(|parsed| parsed.jdn)
            .filter(|jdn| (0.0..=AUTO_JDN_MAX).contains(jdn));
        let jdn2 = parse_time_value(&args[1])
            .map(|parsed| parsed.jdn)
            .filter(|jdn| (0.0..=AUTO_JDN_MAX).contains(jdn));

        match (jdn1, jdn2) {
            (Some(j1), Some(j2)) => Ok(SqliteValue::Text(timediff_impl(j1, j2).into())),
            _ => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn is_deterministic(&self) -> bool {
        false
    }

    fn name(&self) -> &str {
        "timediff"
    }
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all §13.3 date/time functions.
pub fn register_datetime_builtins(registry: &mut FunctionRegistry) {
    registry.register_conditionally_deterministic_scalar(DateFunc);
    registry.register_conditionally_deterministic_scalar(TimeFunc);
    registry.register_conditionally_deterministic_scalar(DateTimeFunc);
    registry.register_conditionally_deterministic_scalar(JuliandayFunc);
    registry.register_conditionally_deterministic_scalar(UnixepochFunc);
    registry.register_conditionally_deterministic_scalar(StrftimeFunc);
    registry.register_conditionally_deterministic_scalar(TimediffFunc);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // bd-tl6ly: the `utc` modifier must resolve DST-transition ambiguity to the
    // pre-transition offset exactly as stock sqlite3 does. This is TZ-dependent,
    // so it is gated on TZ=America/New_York (set_var is unsafe / forbidden here,
    // so the harness/CI must run the process with that TZ); it skips otherwise
    // so it stays portable. Expected values were captured from sqlite3 3.46.1.
    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn utc_modifier_dst_transition_matches_stock_bd_tl6ly() {
        if std::env::var("TZ").ok().as_deref() != Some("America/New_York") {
            eprintln!(
                "SKIP utc_modifier_dst_transition_matches_stock_bd_tl6ly: needs TZ=America/New_York"
            );
            return;
        }
        let utc_of = |y, mo, d, h, mi, s| -> (i64, i64, i64, i64, i64, i64) {
            let jdn = ymdhms_to_jdn(y, mo, d, h, mi, s, 0.0);
            let out = apply_modifier(jdn, "utc", None).expect("utc modifier");
            let (yy, mm, dd) = jdn_to_ymd(out);
            let (hh, nn, ss, _f) = jdn_to_hms(out);
            (yy, mm, dd, hh, nn, ss)
        };
        // normal winter EST(-5): 2024-01-15 12:00 local -> 17:00 UTC
        assert_eq!(utc_of(2024, 1, 15, 12, 0, 0), (2024, 1, 15, 17, 0, 0));
        // normal summer EDT(-4): 2024-07-15 12:00 local -> 16:00 UTC
        assert_eq!(utc_of(2024, 7, 15, 12, 0, 0), (2024, 7, 15, 16, 0, 0));
        // near-transition, unambiguous EDT: 2024-03-10 06:30 -> 10:30 (-4)
        assert_eq!(utc_of(2024, 3, 10, 6, 30, 0), (2024, 3, 10, 10, 30, 0));
        // spring-forward GAP (02:30 is nonexistent) -> pre-transition EST(-5) -> 07:30
        assert_eq!(utc_of(2024, 3, 10, 2, 30, 0), (2024, 3, 10, 7, 30, 0));
        // fall-back FOLD (01:30 occurs twice) -> pre-transition EDT(-4) -> 05:30
        assert_eq!(utc_of(2024, 11, 3, 1, 30, 0), (2024, 11, 3, 5, 30, 0));
    }

    fn text(s: &str) -> SqliteValue {
        SqliteValue::Text(s.into())
    }

    fn int(v: i64) -> SqliteValue {
        SqliteValue::Integer(v)
    }

    fn float(v: f64) -> SqliteValue {
        SqliteValue::Float(v)
    }

    fn null() -> SqliteValue {
        SqliteValue::Null
    }

    fn assert_text(result: &SqliteValue, expected: &str) {
        match result {
            SqliteValue::Text(s) => assert_eq!(s.as_ref(), expected, "text mismatch"),
            other => panic!("expected Text(\"{expected}\"), got {other:?}"),
        }
    }

    // ── Schema-expression safety ─────────────────────────────────────────

    fn blob(s: &str) -> SqliteValue {
        SqliteValue::Blob(s.as_bytes().into())
    }

    #[test]
    fn test_schema_safety_common_datetime_argument_layout() {
        for name in ["date", "time", "datetime", "julianday", "unixepoch"] {
            assert!(
                !is_datetime_invocation_safe_for_schema(name, &[]),
                "{name}() implicitly reads the current time"
            );
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15 12:34:56")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(name, &[int(0)]));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[float(2_460_384.5)]
            ));
            assert!(is_datetime_invocation_safe_for_schema(name, &[null()]));

            for current_time in ["now", "NOW", "subsec", "SUBSECOND"] {
                assert!(
                    !is_datetime_invocation_safe_for_schema(name, &[text(current_time)]),
                    "{name}({current_time:?}) must be conditional"
                );
            }
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[blob("NOW")]
            ));
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[text("now\0ignored")]
            ));
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[SqliteValue::Blob(b"subsec\0\xff".as_slice().into())]
            ));
            for invalid_padded in [" now ", "subsec ", " subsecond"] {
                assert!(is_datetime_invocation_safe_for_schema(
                    name,
                    &[text(invalid_padded)]
                ));
            }
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[blob(" NoW ")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("nowhere")]
            ));

            // These words only have conditional meaning in modifier position.
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("localtime")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(name, &[text("utc")]));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), text("subsec")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), text("subsecond")]
            ));

            for modifier in ["localtime", "LOCALTIME", "utc"] {
                assert!(
                    !is_datetime_invocation_safe_for_schema(
                        name,
                        &[text("2024-03-15"), text(modifier)]
                    ),
                    "{name} modifier {modifier:?} depends on host-local state"
                );
            }
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), blob("UTC")]
            ));
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), text("localtime\0ignored")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), text(" utc ")]
            ));

            // A prior NULL returns NULL without reaching later modifiers.
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), null(), text("localtime")]
            ));
            assert!(!is_datetime_invocation_safe_for_schema(
                name,
                &[text("2024-03-15"), text("localtime"), null()]
            ));

            // Invalid input or an invalid modifier stops evaluation before a
            // later conditional token is reached.
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("bogus"), text("localtime")]
            ));
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("2000-01-01"), text("bogus"), text("localtime")]
            ));
        }
    }

    #[test]
    fn test_schema_safety_strftime_uses_shifted_time_arguments() {
        assert!(is_datetime_invocation_safe_for_schema("strftime", &[]));
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[null()]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[null(), text("now")]
        ));

        // A format without a time value implicitly formats the current time.
        assert!(!is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%Y")]
        ));
        assert!(!is_datetime_invocation_safe_for_schema(
            "STRFTIME",
            &[text("%Y"), text("now")]
        ));
        assert!(!is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%s"), text("subsecond")]
        ));

        // Keywords in the format slot are ordinary fixed format strings.
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("now localtime utc"), text("2024-03-15")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%Y"), int(0), text("unixepoch")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%f"), text("2024-03-15"), text("subsec")]
        ));
        assert!(!is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%Y"), text("2024-03-15"), text("localtime")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "strftime",
            &[text("%Y"), text("2024-03-15"), null(), text("localtime")]
        ));
    }

    #[test]
    fn test_schema_safety_timediff_treats_both_inputs_as_time_values() {
        assert!(is_datetime_invocation_safe_for_schema("timediff", &[]));
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[text("2024-03-15")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[text("2024-03-15"), text("2024-03-14")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[int(2_460_384), float(2_460_383.5)]
        ));

        for current_time in ["now", "subsec", "subsecond"] {
            assert!(!is_datetime_invocation_safe_for_schema(
                "timediff",
                &[text(current_time), text("2024-03-14")]
            ));
            assert!(!is_datetime_invocation_safe_for_schema(
                "timediff",
                &[text("2024-03-15"), text(current_time)]
            ));
        }

        // timediff has no modifier positions, so these are fixed (invalid)
        // time strings rather than requests for host-local conversion.
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[text("localtime"), text("utc")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[null(), text("now")]
        ));
        assert!(is_datetime_invocation_safe_for_schema(
            "timediff",
            &[text("bogus"), text("now")]
        ));
        assert!(!is_datetime_invocation_safe_for_schema(
            "timediff",
            &[blob("NOW"), null()]
        ));
    }

    #[test]
    fn test_schema_safety_ignores_non_datetime_function_names() {
        for name in ["", "my_date", "current_date", "date ", "random"] {
            assert!(is_datetime_invocation_safe_for_schema(
                name,
                &[text("now"), text("localtime")]
            ));
        }
    }

    // ── Basic functions ───────────────────────────────────────────────

    #[test]
    fn test_omitted_time_value_uses_current_time() {
        let date = DateFunc.invoke(&[]).unwrap();
        let time = TimeFunc.invoke(&[]).unwrap();
        let datetime = DateTimeFunc.invoke(&[]).unwrap();
        let julianday = JuliandayFunc.invoke(&[]).unwrap();
        let unixepoch = UnixepochFunc.invoke(&[]).unwrap();
        let year = StrftimeFunc.invoke(&[text("%Y")]).unwrap();

        assert!(matches!(&date, SqliteValue::Text(value) if value.len() == 10));
        assert!(matches!(&time, SqliteValue::Text(value) if value.len() == 8));
        assert!(matches!(&datetime, SqliteValue::Text(value) if value.len() == 19));
        assert!(matches!(julianday, SqliteValue::Float(_)));
        assert!(matches!(unixepoch, SqliteValue::Integer(_)));
        assert!(matches!(&year, SqliteValue::Text(value)
            if value.len() == 4 && value.as_bytes().iter().all(u8::is_ascii_digit)));

        assert_eq!(StrftimeFunc.invoke(&[]).unwrap(), SqliteValue::Null);
        assert_eq!(StrftimeFunc.invoke(&[null()]).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_datetime_signed_offset_modifiers_and_bce_dates() {
        // Oracle: sqlite3 3.54.0 and the 3.52.0 date.c parseModifier /
        // parseYyyyMmDd. Some(text) for TEXT, None for NULL.
        let date = |args: &[SqliteValue]| -> Option<String> {
            match DateFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("date -> {other:?}"),
            }
        };
        let dtime = |args: &[SqliteValue]| -> Option<String> {
            match DateTimeFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("datetime -> {other:?}"),
            }
        };
        let s = |x: &str| Some(x.to_owned());
        let base = text("2024-01-01 10:00");

        // [+|-]HH:MM[:SS[.FFF]] time shifts, modulo one day.
        assert_eq!(
            dtime(&[base.clone(), text("+01:30")]),
            s("2024-01-01 11:30:00")
        );
        assert_eq!(
            dtime(&[base.clone(), text("01:30")]),
            s("2024-01-01 11:30:00")
        );
        assert_eq!(
            dtime(&[base.clone(), text("-01:30:15")]),
            s("2024-01-01 08:29:45")
        );
        assert_eq!(
            dtime(&[base.clone(), text("+24:00")]),
            s("2024-01-01 10:00:00")
        );
        assert_eq!(
            dtime(&[text("2024-01-01"), text("+01:30:15.5")]),
            s("2024-01-01 01:30:15")
        );
        assert_eq!(dtime(&[base.clone(), text("+1:30")]), None);
        assert_eq!(dtime(&[base.clone(), text("+01:60")]), None);
        assert_eq!(dtime(&[base.clone(), text("+01:30 junk")]), None);

        // (+|-)YYYY-MM-DD[ HH:MM] calendar shifts, with floor support.
        assert_eq!(
            dtime(&[text("2024-01-01 12:00"), text("+0001-02-03 04:05:06")]),
            s("2025-03-04 16:05:06")
        );
        assert_eq!(
            dtime(&[text("2024-03-31"), text("+0000-01-00")]),
            s("2024-05-01 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2024-03-31"), text("+0000-01-00"), text("floor")]),
            s("2024-04-30 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2024-01-01"), text("-0000-00-01")]),
            s("2023-12-31 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2024-01-01"), text("+00000-01-00")]),
            s("2024-02-01 00:00:00")
        );
        assert_eq!(dtime(&[text("2024-01-01"), text("+0000-12-00")]), None);
        assert_eq!(dtime(&[text("2024-01-01"), text("+0000-00-31")]), None);
        assert_eq!(dtime(&[text("2024-01-01"), text("0000-01-00")]), None);

        // A leading '-' is a BCE year.
        assert_eq!(date(&[text("-0001-01-01")]), s("-0001-01-01"));
        assert_eq!(
            date(&[text("-0001-03-01"), text("-1 day")]),
            s("-0001-02-28")
        );
    }

    #[test]
    fn test_datetime_oracle_edges_2026_08() {
        // Oracle: sqlite3 3.46.1. Deterministic (TZ-independent) date/time edge
        // semantics — month/year overflow, floor, weekday, invalid dates, ISO
        // 8601 week/year boundaries, and the newer strftime codes. Some(text)
        // for a TEXT result, None for SQL NULL.
        let date = |args: &[SqliteValue]| -> Option<String> {
            match DateFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("date -> {other:?}"),
            }
        };
        let dtime = |args: &[SqliteValue]| -> Option<String> {
            match DateTimeFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("datetime -> {other:?}"),
            }
        };
        let strf = |args: &[SqliteValue]| -> Option<String> {
            match StrftimeFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("strftime -> {other:?}"),
            }
        };
        let s = |x: &str| Some(x.to_owned());

        // Month/year overflow normalizes forward (ceiling is the default).
        assert_eq!(
            date(&[text("2020-01-31"), text("+1 month")]),
            s("2020-03-02")
        );
        assert_eq!(
            date(&[text("2020-01-31"), text("+1 month"), text("+1 month")]),
            s("2020-04-02")
        );
        assert_eq!(date(&[text("2021-02-28"), text("+1 day")]), s("2021-03-01"));
        assert_eq!(
            date(&[text("2020-02-29"), text("+1 year")]),
            s("2021-03-01")
        );
        // A literal date validates day<=31 (NOT days-in-month), then JDN math
        // normalizes: Feb 30 -> Mar 1.
        assert_eq!(date(&[text("2020-02-30")]), s("2020-03-01"));
        // `floor` rolls a month/year overflow back to the last valid day.
        assert_eq!(
            date(&[text("2020-01-31"), text("+1 month"), text("floor")]),
            s("2020-02-29")
        );
        assert_eq!(
            date(&[text("2020-02-29"), text("+1.0 year"), text("floor")]),
            s("2021-02-28")
        );
        // start-of / weekday (0..=6; 7 is out of range -> NULL).
        assert_eq!(
            date(&[text("2020-01-31"), text("start of month")]),
            s("2020-01-01")
        );
        assert_eq!(
            date(&[text("2020-06-15"), text("weekday 0")]),
            s("2020-06-21")
        );
        assert_eq!(
            date(&[text("2020-06-15"), text("weekday 6")]),
            s("2020-06-20")
        );
        assert_eq!(date(&[text("2020-06-15"), text("weekday 7")]), None);
        // Fractional month/year (bd-datetime-fractional-month-year-ag5fo): the integer part recomposes the
        // calendar; the remainder adds FLAT days (30/month, 365/year) AFTER,
        // signed like the modifier — NOT `value * average-month-length`.
        assert_eq!(
            date(&[text("2020-01-15"), text("+1.5 months")]),
            s("2020-03-01")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+0.5 months")]),
            s("2020-01-30 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+1.5 months")]),
            s("2020-03-01 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+2.5 months")]),
            s("2020-03-30 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+1.25 months")]),
            s("2020-02-22 12:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+1.75 months")]),
            s("2020-03-08 12:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("-0.5 months")]),
            s("2019-12-31 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("-1.5 months")]),
            s("2019-11-30 00:00:00")
        );
        // Integer recomposition (ceiling) happens BEFORE the fractional days.
        assert_eq!(
            dtime(&[text("2020-01-31 00:00:00"), text("+1.5 months")]),
            s("2020-03-17 00:00:00")
        );
        // Fractional years use 365 flat days for the remainder.
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+1.5 years")]),
            s("2021-07-16 12:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-15 00:00:00"), text("+0.5 years")]),
            s("2020-07-15 12:00:00")
        );
        // Invalid month / unparseable -> NULL.
        assert_eq!(date(&[text("2020-13-01")]), None);
        assert_eq!(date(&[text("2020-00-10")]), None);
        assert_eq!(date(&[text("not-a-date")]), None);

        // datetime rendering.
        assert_eq!(
            dtime(&[text("2020-06-15 12:34:56"), text("start of day")]),
            s("2020-06-15 00:00:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-01"), text("+90 minutes")]),
            s("2020-01-01 01:30:00")
        );
        assert_eq!(
            dtime(&[text("2020-01-01"), text("+1.5 days")]),
            s("2020-01-02 12:00:00")
        );
        assert_eq!(
            dtime(&[int(0), text("unixepoch")]),
            s("1970-01-01 00:00:00")
        );

        // strftime format codes.
        assert_eq!(
            strf(&[text("%f"), text("2020-01-01 00:00:01.25")]),
            s("01.250")
        );
        assert_eq!(strf(&[text("%j"), text("2020-03-01")]), s("061"));
        assert_eq!(strf(&[text("%w"), text("2020-06-15")]), s("1"));
        assert_eq!(strf(&[text("%W"), text("2020-01-01")]), s("00"));
        assert_eq!(
            strf(&[text("%s"), text("2020-01-01 00:00:00")]),
            s("1577836800")
        );
        assert_eq!(strf(&[text("%e"), text("2020-06-05")]), s(" 5"));
        assert_eq!(strf(&[text("%I"), text("2020-06-15 13:00:00")]), s("01"));
        assert_eq!(strf(&[text("%p"), text("2020-06-15 13:00:00")]), s("PM"));
        assert_eq!(strf(&[text("%P"), text("2020-06-15 13:00:00")]), s("pm"));
        assert_eq!(strf(&[text("%R"), text("2020-06-15 13:05:00")]), s("13:05"));
        assert_eq!(
            strf(&[text("%T"), text("2020-06-15 13:05:07")]),
            s("13:05:07")
        );
        // ISO 8601 week/year boundaries (the trickiest: a January date whose ISO
        // year is the PRIOR calendar year, and week 53).
        assert_eq!(strf(&[text("%G"), text("2020-12-31")]), s("2020"));
        assert_eq!(strf(&[text("%V"), text("2020-12-31")]), s("53"));
        assert_eq!(strf(&[text("%u"), text("2020-06-15")]), s("1"));
        assert_eq!(strf(&[text("%g"), text("2021-01-01")]), s("20"));
        assert_eq!(strf(&[text("%V"), text("2021-01-01")]), s("53"));
        assert_eq!(
            strf(&[text("%G-%V-%u"), text("2021-01-01")]),
            s("2020-53-5")
        );
        assert_eq!(strf(&[text("%V"), text("2016-01-01")]), s("53"));
        assert_eq!(strf(&[text("%V"), text("2018-12-31")]), s("01"));

        // julianday is exact for a round value.
        assert_eq!(
            JuliandayFunc
                .invoke(&[text("2000-01-01 12:00:00")])
                .unwrap(),
            SqliteValue::Float(2451545.0)
        );
    }

    #[test]
    fn test_datetime_more_edges_2026_08() {
        // Oracle: sqlite3 3.46.1. timediff format, %J/%k/%l/%f/%s, subsec, and
        // out-of-range (past year 9999) -> NULL.
        let dtime = |args: &[SqliteValue]| -> Option<String> {
            match DateTimeFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => Some(t.as_str().to_owned()),
                SqliteValue::Null => None,
                other => panic!("datetime -> {other:?}"),
            }
        };
        let strf = |args: &[SqliteValue]| -> String {
            match StrftimeFunc.invoke(args).unwrap() {
                SqliteValue::Text(t) => t.as_str().to_owned(),
                other => panic!("strftime -> {other:?}"),
            }
        };
        let s = |x: &str| Some(x.to_owned());
        let txt = |x: &str| SqliteValue::Text(SmallText::from_string(x));

        // timediff(A, B) = A - B, formatted +YYYY-MM-DD HH:MM:SS.SSS.
        assert_eq!(
            TimediffFunc
                .invoke(&[text("2020-03-01"), text("2020-01-15")])
                .unwrap(),
            txt("+0000-01-15 00:00:00.000")
        );
        assert_eq!(
            TimediffFunc
                .invoke(&[text("2020-01-15 12:00:00"), text("2020-01-15 10:30:00")])
                .unwrap(),
            txt("+0000-00-00 01:30:00.000")
        );

        // strftime codes: %J full julian day, %k/%l space-padded hours, %f, %s.
        assert_eq!(
            strf(&[text("%J"), text("2000-01-01 18:00:00")]),
            "2451545.25"
        );
        assert_eq!(strf(&[text("%k"), text("2020-06-15 09:00:00")]), " 9");
        assert_eq!(strf(&[text("%l"), text("2020-06-15 13:00:00")]), " 1");
        assert_eq!(
            strf(&[text("%f"), text("2020-01-01 00:00:00.999")]),
            "00.999"
        );
        assert_eq!(
            strf(&[text("%s"), text("2020-01-01 00:00:00.5")]),
            "1577836800"
        );

        // subsec / unixepoch-of-float / out-of-range.
        assert_eq!(
            dtime(&[text("2020-01-01 00:00:00"), text("subsec")]),
            s("2020-01-01 00:00:00.000")
        );
        assert_eq!(
            dtime(&[float(1_577_836_800.5), text("unixepoch")]),
            s("2020-01-01 00:00:00")
        );
        assert_eq!(dtime(&[text("2020-01-01"), text("+100000000 days")]), None);

        // julianday / unixepoch subsec (exact float).
        assert_eq!(
            JuliandayFunc.invoke(&[text("1970-01-01")]).unwrap(),
            SqliteValue::Float(2440587.5)
        );
        assert_eq!(
            UnixepochFunc
                .invoke(&[text("2020-01-01"), text("subsec")])
                .unwrap(),
            SqliteValue::Float(1_577_836_800.0)
        );
    }

    #[test]
    fn test_first_position_subsec_aliases_use_current_time() {
        for alias in ["subsec", "subsecond"] {
            assert!(matches!(
                DateFunc.invoke(&[text(alias)]).unwrap(),
                SqliteValue::Text(value) if value.len() == 10
            ));
            assert!(matches!(
                TimeFunc.invoke(&[text(alias)]).unwrap(),
                SqliteValue::Text(value)
                    if value.len() == 12 && value.as_bytes_direct()[8] == b'.'
            ));
            assert!(matches!(
                DateTimeFunc.invoke(&[text(alias)]).unwrap(),
                SqliteValue::Text(value)
                    if value.len() == 23 && value.as_bytes_direct()[19] == b'.'
            ));
            assert!(matches!(
                JuliandayFunc.invoke(&[text(alias)]).unwrap(),
                SqliteValue::Float(_)
            ));
            assert!(matches!(
                UnixepochFunc.invoke(&[text(alias)]).unwrap(),
                SqliteValue::Float(_)
            ));
            assert!(matches!(
                StrftimeFunc.invoke(&[text("%s"), text(alias)]).unwrap(),
                SqliteValue::Text(value)
                    if value.rsplit_once('.').is_some_and(|(_, fraction)| fraction.len() == 3)
            ));
        }
    }

    #[test]
    fn test_padded_and_nul_terminated_special_values() {
        for invalid in [" now ", "subsec ", " subsecond"] {
            assert_eq!(
                DateFunc.invoke(&[text(invalid)]).unwrap(),
                SqliteValue::Null
            );
        }
        assert_eq!(
            DateFunc
                .invoke(&[text("2000-01-01"), text(" localtime ")])
                .unwrap(),
            SqliteValue::Null
        );
        assert!(matches!(
            DateFunc.invoke(&[text("now\0ignored")]).unwrap(),
            SqliteValue::Text(value) if value.len() == 10
        ));
        assert!(matches!(
            TimeFunc
                .invoke(&[SqliteValue::Blob(b"subsec\0\xff".as_slice().into())])
                .unwrap(),
            SqliteValue::Text(value) if value.len() == 12
        ));
    }

    #[test]
    fn test_date_basic() {
        let r = DateFunc.invoke(&[text("2024-03-15 14:30:00")]).unwrap();
        assert_text(&r, "2024-03-15");
    }

    #[test]
    fn test_time_basic() {
        let r = TimeFunc.invoke(&[text("2024-03-15 14:30:45")]).unwrap();
        assert_text(&r, "14:30:45");
    }

    #[test]
    fn test_datetime_basic() {
        let r = DateTimeFunc.invoke(&[text("2024-03-15 14:30:00")]).unwrap();
        assert_text(&r, "2024-03-15 14:30:00");
    }

    #[test]
    fn test_julianday_basic() {
        let r = JuliandayFunc.invoke(&[text("2024-03-15")]).unwrap();
        match r {
            SqliteValue::Float(jdn) => {
                // JDN for 2024-03-15 should be approximately 2460384.5
                assert!((jdn - 2_460_384.5).abs() < 0.01, "unexpected JDN: {jdn}");
            }
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn test_computed_result_out_of_range_returns_null() {
        // C SQLite returns NULL when a modifier pushes the *computed* time
        // outside the representable Julian-day range — past 9999-12-31 23:59:59
        // or below Julian day 0 — for every date function, rather than emitting
        // a malformed 5-digit-year string or an out-of-range julianday()/
        // unixepoch() number. Verified against SQLite 3.53. Guards the
        // post-modifier range check in `parse_args`.
        let over_datetime = &[text("9999-12-31 23:59:59"), text("+1 second")];
        assert_eq!(
            DateTimeFunc.invoke(over_datetime).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(TimeFunc.invoke(over_datetime).unwrap(), SqliteValue::Null);
        assert_eq!(
            JuliandayFunc.invoke(over_datetime).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            UnixepochFunc.invoke(over_datetime).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            DateFunc
                .invoke(&[text("9999-12-31"), text("+1 day")])
                .unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            DateTimeFunc
                .invoke(&[text("9999-12-31"), text("+1 month")])
                .unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            StrftimeFunc
                .invoke(&[
                    text("%Y-%m-%d"),
                    text("9999-12-31 23:59:59"),
                    text("+1 second")
                ])
                .unwrap(),
            SqliteValue::Null
        );
        // Lower bound: a result below Julian day 0 (before ~4714 BC) is NULL.
        assert_eq!(
            DateTimeFunc
                .invoke(&[text("-4714-11-24 12:00:00"), text("-1 day")])
                .unwrap(),
            SqliteValue::Null
        );

        // Valid boundary / in-range cases must still succeed (no over-rejection):
        assert_text(
            &DateTimeFunc.invoke(&[text("9999-12-31 23:59:59")]).unwrap(),
            "9999-12-31 23:59:59",
        );
        // A negative *year* whose Julian day is still >= 0 stays valid (rendered,
        // not NULL) — SQLite formats such instants (e.g. -0001-12-31 ...).
        assert!(matches!(
            DateTimeFunc
                .invoke(&[text("0000-01-01"), text("-1 second")])
                .unwrap(),
            SqliteValue::Text(_)
        ));
    }

    #[test]
    fn test_negative_year_padding_matches_sqlite() {
        // C SQLite's date()/datetime() render a negative (proleptic BC) year with
        // the sign OUTSIDE a 4-digit zero-padded magnitude: -1 -> "-0001", not the
        // "-001" that a naive `{:04}` (sign inside the width) produces. Verified
        // against SQLite 3.53.
        assert_text(
            &DateTimeFunc
                .invoke(&[text("0000-01-01"), text("-1 second")])
                .unwrap(),
            "-0001-12-31 23:59:59",
        );
        assert_text(
            &DateFunc
                .invoke(&[text("0000-01-01"), text("-1 day")])
                .unwrap(),
            "-0001-12-31",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("0000-01-01"), text("-100 years")])
                .unwrap(),
            "-0100-01-01 00:00:00",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("0000-01-01"), text("-4000 years")])
                .unwrap(),
            "-4000-01-01 00:00:00",
        );
        // Non-negative years are unchanged (regression guard).
        assert_text(
            &DateTimeFunc.invoke(&[text("2024-03-15 14:30:00")]).unwrap(),
            "2024-03-15 14:30:00",
        );
        assert_text(
            &DateFunc.invoke(&[text("0000-01-01")]).unwrap(),
            "0000-01-01",
        );
        // strftime('%Y') keeps the sign INSIDE a 4-wide field ("-001"), matching
        // SQLite's separate strftime path — this fix must not touch it.
        assert_text(
            &StrftimeFunc
                .invoke(&[text("%Y"), text("0000-01-01"), text("-1 day")])
                .unwrap(),
            "-001",
        );
    }

    // ── RFC3339 / ISO-8601 timezone suffix parsing ────────────────────
    //
    // Regression coverage for issue #64: julianday() must accept
    // Z / ±HH:MM / ±HHMM / ±HH timezone-bearing timestamps and convert
    // them to UTC before computing the Julian day.  The expected JDN
    // values below match the C SQLite reference implementation.

    fn julianday_float(input: &str) -> f64 {
        match JuliandayFunc.invoke(&[text(input)]).unwrap() {
            SqliteValue::Float(v) => v,
            other => panic!("expected Float, got {other:?} for input {input:?}"),
        }
    }

    fn assert_jdn_close(actual: f64, expected: f64, ctx: &str) {
        // 1 µs precision (86400e6 µs / day) is well within float epsilon.
        assert!(
            (actual - expected).abs() < 1e-6,
            "JDN mismatch for {ctx}: got {actual}, expected {expected}"
        );
    }

    #[test]
    fn test_julianday_rfc3339_z_suffix() {
        // Zulu (UTC) — should match the equivalent naive form exactly.
        let naive = julianday_float("2026-04-07 16:00:00");
        assert_jdn_close(julianday_float("2026-04-07T16:00:00Z"), naive, "T...Z");
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00z"),
            naive,
            "lowercase z",
        );
    }

    #[test]
    fn test_julianday_rfc3339_zero_offset() {
        let naive = julianday_float("2026-04-07 16:00:00");
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00+00:00"),
            naive,
            "+00:00",
        );
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00-00:00"),
            naive,
            "-00:00",
        );
    }

    #[test]
    fn test_julianday_rfc3339_positive_offset() {
        // 16:00 +01:00 = 15:00 UTC → JDN is 1 hour (1/24) earlier.
        let base = julianday_float("2026-04-07 16:00:00");
        let expected = base - 1.0 / 24.0;
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00+01:00"),
            expected,
            "+01:00",
        );
    }

    #[test]
    fn test_julianday_rfc3339_negative_offset() {
        // 16:00 -05:00 = 21:00 UTC → JDN is 5 hours later.
        let base = julianday_float("2026-04-07 16:00:00");
        let expected = base + 5.0 / 24.0;
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00-05:00"),
            expected,
            "-05:00",
        );
    }

    #[test]
    fn test_julianday_rfc3339_half_hour_offset() {
        // India Standard Time is UTC+05:30.
        let base = julianday_float("2026-04-07 16:00:00");
        let expected = base - 5.5 / 24.0;
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00+05:30"),
            expected,
            "+05:30",
        );
    }

    #[test]
    fn test_julianday_compact_offsets_return_null() {
        // date.c parseTimezone takes only `±HH:MM` (hours 0..14): the compact
        // ISO-8601 forms are not SQLite time strings.
        for bad in &[
            "2026-04-07T16:00:00+0100",
            "2026-04-07T16:00:00-0530",
            "2026-04-07T16:00:00+09",
            "2026-04-07T16:00:00+15:00",
        ] {
            let result = JuliandayFunc.invoke(&[text(bad)]).unwrap();
            assert_eq!(result, SqliteValue::Null, "{bad:?}");
        }
    }

    #[test]
    fn test_time_string_grammar_matches_date_c() {
        // Each expectation is SQLite 3.53.2 output.
        let dtime = |value: &str| match DateTimeFunc.invoke(&[text(value)]).unwrap() {
            SqliteValue::Text(t) => Some(t.to_string()),
            _ => None,
        };
        let cases: &[(&str, Option<&str>)] = &[
            ("2024-01-01 12:00 +01:00", Some("2024-01-01 11:00:00")),
            ("2024-01-01 12:00   -01:30   ", Some("2024-01-01 13:30:00")),
            ("2024-01-01 12:00 Z", Some("2024-01-01 12:00:00")),
            ("2024-01-01 12:00+14:59", Some("2023-12-31 21:01:00")),
            ("2024-01-01  12:00", Some("2024-01-01 12:00:00")),
            ("2024-01-01 T 12:00", Some("2024-01-01 12:00:00")),
            ("2024-01-0112:00", Some("2024-01-01 12:00:00")),
            ("2024-01-01T", Some("2024-01-01 00:00:00")),
            ("2024-01-01\u{b}", Some("2024-01-01 00:00:00")),
            ("12:00 -05:30", Some("2000-01-01 17:30:00")),
            // Hour 24 is accepted, and fields no modifier touched print as written.
            ("2024-01-01 24:00", Some("2024-01-01 24:00:00")),
            ("2024-01-31 24:00", Some("2024-02-01 24:00:00")),
            ("24:00", Some("2000-01-02 24:00:00")),
            ("2024-01-01 24:00+01:00", Some("2024-01-01 23:00:00")),
            ("2024-01-01 25:00", None),
            ("2024-01-01 12:00+01:0", None),
            ("2024-01-01 12:00+", None),
            ("2024-01-01 12:00ZZ", None),
            ("2024-01-01 12:00 x", None),
            ("2024-01-01 12:00:00.", None),
            ("2024-01-01Z", None),
            ("+123-01-01", None),
            ("2024-+1-01", None),
            ("2024-01-+1", None),
        ];
        for &(input, expected) in cases {
            assert_eq!(dtime(input).as_deref(), expected, "datetime({input:?})");
        }
        let with = |args: &[&str]| {
            let args: Vec<SqliteValue> = args.iter().map(|a| text(a)).collect();
            match DateTimeFunc.invoke(&args).unwrap() {
                SqliteValue::Text(t) => Some(t.to_string()),
                _ => None,
            }
        };
        assert_eq!(
            with(&["2024-02-30", "subsec"]).as_deref(),
            Some("2024-02-30 00:00:00.000")
        );
        assert_eq!(
            with(&["2024-02-30"]).as_deref(),
            Some("2024-03-01 00:00:00")
        );
    }

    #[test]
    fn test_utc_is_a_no_op_on_values_already_in_utc() {
        // date.c isUtc: an explicit `Z`/`±HH:MM` makes the value UTC already.
        for (input, expected) in [
            ("2024-06-15 12:00Z", "2024-06-15 12:00:00"),
            ("2024-06-15 12:00+00:00", "2024-06-15 12:00:00"),
            ("2024-06-15 12:00-00:00", "2024-06-15 12:00:00"),
            ("2024-06-15 12:00+02:00", "2024-06-15 10:00:00"),
            ("2024-01-01 24:00Z", "2024-01-01 24:00:00"),
        ] {
            assert_text(
                &DateTimeFunc
                    .invoke(&[text(input), text("utc"), text("UTC")])
                    .unwrap(),
                expected,
            );
        }
    }

    #[test]
    fn test_localtime_outside_1970_2037_uses_an_equivalent_year() {
        // date.c toLocaltime asks localtime_r() about year 2000 + Y%4, so the
        // offset matches that year's whatever the process timezone is.
        for (y, mapped) in [(1800, 2000), (1945, 2001), (-100, 2000), (2050, 2002)] {
            for (mo, d) in [(1, 15), (7, 1)] {
                assert_eq!(
                    utc_offset_for_utc_jdn(ymdhms_to_jdn(y, mo, d, 12, 0, 0, 0.0)),
                    utc_offset_for_utc_jdn(ymdhms_to_jdn(mapped, mo, d, 12, 0, 0, 0.0)),
                    "{y}-{mo:02}-{d:02}"
                );
            }
        }
    }

    #[test]
    fn test_floor_state_follows_date_c() {
        // nFloor starts from the date as written, survives the named
        // modifiers, is cleared by `NNN units`, and 'floor' leaves it pending.
        for (args, expected) in [
            (&["2023-02-30", "floor"][..], "2023-02-28"),
            (&["2024-02-30", "floor"][..], "2024-02-29"),
            (&["2023-02-30", "ceiling"][..], "2023-03-02"),
            (&["2023-02-30 12:00+01:00", "floor"][..], "2023-02-28"),
            (
                &["2024-01-31", "+1 month", "start of day", "floor"][..],
                "2024-02-29",
            ),
            (
                &["2024-01-31", "+1 month", "+01:00", "floor"][..],
                "2024-02-29",
            ),
            (
                &["2024-01-31", "+1 month", "+1 day", "floor"][..],
                "2024-03-03",
            ),
            (
                &["2024-01-31", "+1 month", "floor", "floor"][..],
                "2024-02-27",
            ),
        ] {
            let values: Vec<SqliteValue> = args.iter().map(|a| text(a)).collect();
            assert_text(&DateFunc.invoke(&values).unwrap(), expected);
        }
    }

    #[test]
    fn test_julianday_rfc3339_fractional_seconds_with_tz() {
        // Fractional seconds must play nicely with the TZ suffix split.
        let base = julianday_float("2026-04-07 16:00:00.500");
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00.500Z"),
            base,
            "fractional + Z",
        );
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00.500+01:00"),
            base - 1.0 / 24.0,
            "fractional + +01:00",
        );
    }

    #[test]
    fn test_date_and_time_rfc3339_round_trip() {
        // date()/time()/datetime() all flow through parse_timestring, so
        // they should agree with the timezone conversion above.
        assert_text(
            &DateFunc
                .invoke(&[text("2026-04-07T16:00:00+05:00")])
                .unwrap(),
            // 16:00 +05:00 = 11:00 UTC on the same date.
            "2026-04-07",
        );
        assert_text(
            &TimeFunc
                .invoke(&[text("2026-04-07T16:00:00+05:00")])
                .unwrap(),
            "11:00:00",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2026-04-07T16:00:00+05:00")])
                .unwrap(),
            "2026-04-07 11:00:00",
        );
    }

    #[test]
    fn test_julianday_rfc3339_invalid_offsets_return_null() {
        // Malformed offsets fall through and return NULL (invalid input).
        for bad in &[
            "2026-04-07T16:00:00+25:00", // hour out of range
            "2026-04-07T16:00:00+01:99", // minute out of range
            "2026-04-07T16:00:00+1",     // too short
            "2026-04-07T16:00:00+123",   // wrong width
        ] {
            let result = JuliandayFunc.invoke(&[text(bad)]).unwrap();
            assert_eq!(
                result,
                SqliteValue::Null,
                "expected NULL for malformed offset {bad:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_julianday_rejects_malformed_time_fields() {
        // C SQLite's computeHMS requires exactly 2 bare decimal digits
        // for each HH, MM, SS field.  Inputs with wrong digit counts,
        // leading signs, or non-digit characters must return NULL.
        for bad in &[
            "+01:00",        // leading + on hour
            "-05:30",        // leading - on hour
            "+12:30:00",     // leading + on hour (with seconds)
            "12:+30:00",     // leading + on minute
            "12:30:+45",     // leading + on seconds (integer)
            "12:30:+45.123", // leading + on seconds (fractional)
            "0:00:00",       // 1-digit hour
            "12:0:00",       // 1-digit minute
            "12:30:0",       // 1-digit second
            "123:00:00",     // 3-digit hour
            "12:345:00",     // 3-digit minute
        ] {
            let result = JuliandayFunc.invoke(&[text(bad)]).unwrap();
            assert_eq!(
                result,
                SqliteValue::Null,
                "expected NULL for signed time field {bad:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_unixepoch_basic() {
        let r = UnixepochFunc
            .invoke(&[text("1970-01-01 00:00:00")])
            .unwrap();
        assert_eq!(r, int(0));
    }

    #[test]
    fn test_unixepoch_known_date() {
        let r = UnixepochFunc
            .invoke(&[text("2024-01-01 00:00:00")])
            .unwrap();
        // 2024-01-01 00:00:00 UTC = 1704067200
        assert_eq!(r, int(1_704_067_200));
    }

    // ── Modifiers ─────────────────────────────────────────────────────

    #[test]
    fn test_modifier_days() {
        let r = DateFunc
            .invoke(&[text("2024-01-15"), text("+10 days")])
            .unwrap();
        assert_text(&r, "2024-01-25");
    }

    #[test]
    fn test_modifier_months() {
        // 2024-01-31 + 1 month: C SQLite lets day=31 overflow via JDN
        // arithmetic → Feb 31 wraps to Mar 2 (2024 is a leap year).
        let r = DateFunc
            .invoke(&[text("2024-01-31"), text("+1 months")])
            .unwrap();
        assert_text(&r, "2024-03-02");
    }

    #[test]
    fn test_modifier_years() {
        // 2024-02-29 + 1 year: 2025 is not a leap year, day=29 overflows
        // via JDN arithmetic → Mar 1.
        let r = DateFunc
            .invoke(&[text("2024-02-29"), text("+1 years")])
            .unwrap();
        assert_text(&r, "2025-03-01");
    }

    #[test]
    fn test_modifier_hours() {
        let r = DateTimeFunc
            .invoke(&[text("2024-01-01 23:00:00"), text("+2 hours")])
            .unwrap();
        assert_text(&r, "2024-01-02 01:00:00");
    }

    #[test]
    fn test_modifier_unsigned_is_positive_bd_t8g1e() {
        // bd-t8g1e: SQLite's date grammar makes the modifier sign optional — a
        // bare `NNN units` is positive, identical to a `+`-prefixed modifier.
        // Frank previously required a sign and returned NULL for the unsigned
        // form (probe: date('2024-01-01','30 days') -> NULL vs sqlite
        // '2024-01-31'). Oracle: sqlite3 3.46.1.
        assert_text(
            &DateFunc
                .invoke(&[text("2024-01-15"), text("10 days")])
                .unwrap(),
            "2024-01-25",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2024-01-01"), text("5 hours")])
                .unwrap(),
            "2024-01-01 05:00:00",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2024-01-01"), text("90 minutes")])
                .unwrap(),
            "2024-01-01 01:30:00",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2024-01-01"), text("86400 seconds")])
                .unwrap(),
            "2024-01-02 00:00:00",
        );
        // Month/year use exact YMD math (not the approximate day-delta) even
        // unsigned: + 2 months = 2024-03-01, + 1 year = 2025-01-01.
        assert_text(
            &DateFunc
                .invoke(&[text("2024-01-01"), text("2 months")])
                .unwrap(),
            "2024-03-01",
        );
        assert_text(
            &DateFunc
                .invoke(&[text("2024-01-01"), text("1 year")])
                .unwrap(),
            "2025-01-01",
        );
        // Fractional unsigned modifier.
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2024-01-01 12:00"), text("1.5 hours")])
                .unwrap(),
            "2024-01-01 13:30:00",
        );
        // Regression guard: named modifiers containing "month"/"year" still work
        // (they reach apply_month_year_exact, fail its numeric parse, and fall
        // through to apply_modifier unchanged).
        assert_text(
            &DateFunc
                .invoke(&[text("2024-03-15"), text("start of month")])
                .unwrap(),
            "2024-03-01",
        );
        assert_text(
            &DateFunc
                .invoke(&[text("2024-06-15"), text("start of year")])
                .unwrap(),
            "2024-01-01",
        );
        // Signed forms unchanged.
        assert_text(
            &DateFunc
                .invoke(&[text("2024-01-15"), text("-10 days")])
                .unwrap(),
            "2024-01-05",
        );
    }

    #[test]
    fn test_modifier_start_of_month() {
        let r = DateFunc
            .invoke(&[text("2024-03-15"), text("start of month")])
            .unwrap();
        assert_text(&r, "2024-03-01");
    }

    #[test]
    fn test_modifier_start_of_year() {
        let r = DateFunc
            .invoke(&[text("2024-06-15"), text("start of year")])
            .unwrap();
        assert_text(&r, "2024-01-01");
    }

    #[test]
    fn test_modifier_start_of_day() {
        let r = DateTimeFunc
            .invoke(&[text("2024-03-15 14:30:00"), text("start of day")])
            .unwrap();
        assert_text(&r, "2024-03-15 00:00:00");
    }

    #[test]
    fn test_modifier_unixepoch() {
        let r = DateTimeFunc.invoke(&[int(0), text("unixepoch")]).unwrap();
        assert_text(&r, "1970-01-01 00:00:00");
    }

    #[test]
    fn test_modifier_weekday() {
        // 2024-03-15 is Friday. `weekday 0` advances to the next Sunday.
        let r = DateFunc
            .invoke(&[text("2024-03-15"), text("weekday 0")])
            .unwrap();
        assert_text(&r, "2024-03-17");
    }

    #[test]
    fn test_modifier_auto_unixepoch() {
        let ts = int(1_710_531_045);
        let r = DateTimeFunc.invoke(&[ts.clone(), text("auto")]).unwrap();
        let expected = DateTimeFunc.invoke(&[ts, text("unixepoch")]).unwrap();
        assert_eq!(
            r, expected,
            "auto and unixepoch should agree for unix-like values"
        );
    }

    #[test]
    fn test_modifier_auto_julian_day() {
        let r = DateFunc
            .invoke(&[float(2_460_384.5), text("auto")])
            .unwrap();
        assert_text(&r, "2024-03-15");
    }

    #[test]
    fn test_modifier_localtime_utc_roundtrip() {
        // localtime→utc should roundtrip back to the original value.
        let r = DateTimeFunc
            .invoke(&[text("2024-03-15 14:30:45"), text("localtime"), text("utc")])
            .unwrap();
        assert_text(&r, "2024-03-15 14:30:45");
    }

    #[test]
    fn test_modifier_localtime_shifts_value() {
        // When system offset != 0, 'localtime' should actually shift the value.
        let offset = utc_offset_for_utc_jdn(ymdhms_to_jdn(2024, 3, 15, 12, 0, 0, 0.0));
        if offset != 0 {
            let r = DateTimeFunc
                .invoke(&[text("2024-03-15 12:00:00"), text("localtime")])
                .unwrap();
            // The shifted value should differ from the input.
            let shifted = match &r {
                SqliteValue::Text(s) => s.clone(),
                _ => panic!("expected text"),
            };
            assert_ne!(&*shifted, "2024-03-15 12:00:00");
        }
    }

    #[test]
    fn test_modifier_auto_out_of_range_returns_null() {
        let r = DateTimeFunc.invoke(&[float(1.0e20), text("auto")]).unwrap();
        assert_eq!(r, SqliteValue::Null);
    }

    #[test]
    fn test_modifier_order_matters() {
        // 'start of month' then '+1 day' = March 2nd.
        let r1 = DateFunc
            .invoke(&[text("2024-03-15"), text("start of month"), text("+1 days")])
            .unwrap();
        assert_text(&r1, "2024-03-02");

        // '+1 day' then 'start of month' = March 1st.
        let r2 = DateFunc
            .invoke(&[text("2024-03-15"), text("+1 days"), text("start of month")])
            .unwrap();
        assert_text(&r2, "2024-03-01");
    }

    #[test]
    fn test_modifier_weekday_same_day_is_noop() {
        // 2024-03-17 is Sunday; SQLite semantics: already on target weekday, no-op.
        let r = DateFunc
            .invoke(&[text("2024-03-17"), text("weekday 0")])
            .unwrap();
        assert_text(&r, "2024-03-17");
    }

    // ── Input formats ─────────────────────────────────────────────────

    #[test]
    fn test_bare_time_defaults() {
        let r = DateFunc.invoke(&[text("12:30:00")]).unwrap();
        assert_text(&r, "2000-01-01");
    }

    #[test]
    fn test_t_separator() {
        let r = DateTimeFunc.invoke(&[text("2024-03-15T14:30:00")]).unwrap();
        assert_text(&r, "2024-03-15 14:30:00");
    }

    #[test]
    fn test_julian_day_input() {
        // 2460384.5 is 2024-03-15.
        let r = DateFunc.invoke(&[float(2_460_384.5)]).unwrap();
        assert_text(&r, "2024-03-15");
    }

    #[test]
    fn test_null_input() {
        assert_eq!(DateFunc.invoke(&[null()]).unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_invalid_input() {
        assert_eq!(
            DateFunc.invoke(&[text("not-a-date")]).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_negative_time_component_invalid() {
        let r = TimeFunc.invoke(&[text("-01:00")]).unwrap();
        assert_eq!(r, SqliteValue::Null);
    }

    // ── Leap year ─────────────────────────────────────────────────────

    #[test]
    fn test_leap_year() {
        let r = DateFunc
            .invoke(&[text("2024-02-28"), text("+1 days")])
            .unwrap();
        assert_text(&r, "2024-02-29");
    }

    #[test]
    fn test_non_leap_year() {
        let r = DateFunc
            .invoke(&[text("2023-02-28"), text("+1 days")])
            .unwrap();
        assert_text(&r, "2023-03-01");
    }

    // ── strftime ──────────────────────────────────────────────────────

    #[test]
    fn test_strftime_basic() {
        let r = StrftimeFunc
            .invoke(&[text("%Y-%m-%d"), text("2024-03-15")])
            .unwrap();
        assert_text(&r, "2024-03-15");
    }

    #[test]
    fn test_strftime_time_specifiers() {
        let r = StrftimeFunc
            .invoke(&[text("%H:%M:%S"), text("2024-03-15 14:30:45")])
            .unwrap();
        assert_text(&r, "14:30:45");
    }

    #[test]
    fn test_strftime_unix_seconds() {
        let r = StrftimeFunc
            .invoke(&[text("%s"), text("1970-01-01 00:00:00")])
            .unwrap();
        assert_text(&r, "0");
    }

    #[test]
    fn test_strftime_day_of_year() {
        let r = StrftimeFunc
            .invoke(&[text("%j"), text("2024-03-15")])
            .unwrap();
        // 2024-03-15: Jan(31) + Feb(29) + 15 = 75
        assert_text(&r, "075");
    }

    #[test]
    fn test_strftime_day_of_week() {
        // 2024-03-15 is a Friday → w=5 (0=Sunday), u=5 (1=Monday)
        let r = StrftimeFunc
            .invoke(&[text("%w"), text("2024-03-15")])
            .unwrap();
        assert_text(&r, "5");

        let r = StrftimeFunc
            .invoke(&[text("%u"), text("2024-03-15")])
            .unwrap();
        assert_text(&r, "5");
    }

    #[test]
    fn test_strftime_12hour() {
        let r = StrftimeFunc
            .invoke(&[text("%I %p"), text("2024-03-15 14:30:00")])
            .unwrap();
        assert_text(&r, "02 PM");

        let r = StrftimeFunc
            .invoke(&[text("%I %P"), text("2024-03-15 09:30:00")])
            .unwrap();
        assert_text(&r, "09 am");
    }

    #[test]
    fn test_strftime_all_specifiers_presence() {
        let fmt = "%d|%e|%f|%H|%I|%j|%J|%k|%l|%m|%M|%p|%P|%R|%s|%S|%T|%u|%w|%W|%G|%g|%V|%Y|%%";
        let r = StrftimeFunc
            .invoke(&[text(fmt), text("2024-03-15 14:30:45.123")])
            .unwrap();

        let s = match r {
            SqliteValue::Text(v) => v,
            other => panic!("expected Text, got {other:?}"),
        };
        let parts: Vec<&str> = s.split('|').collect();
        assert_eq!(parts.len(), 25, "unexpected specifier output: {s}");
        assert_eq!(parts[0], "15"); // %d
        assert_eq!(parts[1], "15"); // %e
        assert_eq!(parts[2], "45.123"); // %f
        assert_eq!(parts[3], "14"); // %H
        assert_eq!(parts[4], "02"); // %I
        assert_eq!(parts[5], "075"); // %j
        assert!(
            parts[6].parse::<f64>().is_ok(),
            "expected numeric %J output, got {}",
            parts[6]
        );
        assert_eq!(parts[7], "14"); // %k
        assert_eq!(parts[8], " 2"); // %l
        assert_eq!(parts[9], "03"); // %m
        assert_eq!(parts[10], "30"); // %M
        assert_eq!(parts[11], "PM"); // %p
        assert_eq!(parts[12], "pm"); // %P
        assert_eq!(parts[13], "14:30"); // %R
        assert!(
            parts[14].parse::<i64>().is_ok(),
            "expected numeric %s output, got {}",
            parts[14]
        );
        assert_eq!(parts[15], "45"); // %S
        assert_eq!(parts[16], "14:30:45"); // %T
        assert_eq!(parts[17], "5"); // %u
        assert_eq!(parts[18], "5"); // %w
        assert_eq!(parts[19], "11"); // %W
        assert_eq!(parts[20], "2024"); // %G
        assert_eq!(parts[21], "24"); // %g
        assert_eq!(parts[22], "11"); // %V
        assert_eq!(parts[23], "2024"); // %Y
        assert_eq!(parts[24], "%"); // %%
    }

    #[test]
    fn test_strftime_null() {
        assert_eq!(
            StrftimeFunc.invoke(&[null(), text("2024-01-01")]).unwrap(),
            SqliteValue::Null
        );
        assert_eq!(
            StrftimeFunc.invoke(&[text("%Y"), null()]).unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    #[ignore = "perf-only benchmark"]
    fn perf_strftime_timestamp_rows() {
        use std::hint::black_box;
        use std::time::Instant;

        const ROWS: usize = 200_000;
        const REPEATS: usize = 5;
        const FORMAT: &str = "%Y-%m-%d %H:%M:%S";
        const INPUT: &str = "2024-03-15 14:30:45";

        let func = StrftimeFunc;
        let fmt = text(FORMAT);
        let input = text(INPUT);
        let mut best_ns = u128::MAX;
        let mut output_len = 0usize;

        for _ in 0..REPEATS {
            let started = Instant::now();
            for _ in 0..ROWS {
                let result = black_box(
                    func.invoke(black_box(&[fmt.clone(), input.clone()]))
                        .expect("strftime benchmark invocation must succeed"),
                );
                output_len = match result {
                    SqliteValue::Text(text) => text.len(),
                    SqliteValue::Null
                    | SqliteValue::Integer(_)
                    | SqliteValue::Float(_)
                    | SqliteValue::Blob(_) => 0,
                };
            }
            let elapsed_ns = started.elapsed().as_nanos();
            if elapsed_ns < best_ns {
                best_ns = elapsed_ns;
            }
        }

        println!(
            "strftime_timestamp_rows rows={ROWS} repeats={REPEATS} best_ns={best_ns} output_len={output_len}"
        );
    }

    // ── timediff ──────────────────────────────────────────────────────

    #[test]
    fn test_timediff_basic() {
        let r = TimediffFunc
            .invoke(&[text("2024-03-15"), text("2024-03-10")])
            .unwrap();
        assert_text(&r, "+0000-00-05 00:00:00.000");
    }

    #[test]
    fn test_timediff_negative() {
        let r = TimediffFunc
            .invoke(&[text("2024-03-10"), text("2024-03-15")])
            .unwrap();
        assert_text(&r, "-0000-00-05 00:00:00.000");
    }

    #[test]
    fn test_timediff_year_boundary() {
        let r = TimediffFunc
            .invoke(&[text("2024-01-01 01:00:00"), text("2023-12-31 23:00:00")])
            .unwrap();
        assert_text(&r, "+0000-00-00 02:00:00.000");
    }

    // ── Subsec modifier ───────────────────────────────────────────────

    #[test]
    fn test_modifier_subsec() {
        assert_text(
            &TimeFunc
                .invoke(&[text("2024-01-01 12:00:00"), text("subsec")])
                .unwrap(),
            "12:00:00.000",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("2024-01-01 12:00:00"), text("subsecond")])
                .unwrap(),
            "2024-01-01 12:00:00.000",
        );
        assert_text(
            &TimeFunc
                .invoke(&[text("2024-01-01 12:00:00.123"), text("subsec")])
                .unwrap(),
            "12:00:00.123",
        );
    }

    #[test]
    fn test_subsec_unix_seconds_and_integer_flooring() {
        assert_text(
            &StrftimeFunc
                .invoke(&[text("%s"), text("1970-01-01 00:00:00.125"), text("subsec")])
                .unwrap(),
            "0.125",
        );
        assert_eq!(
            UnixepochFunc
                .invoke(&[text("1970-01-01 00:00:00.125"), text("subsec")])
                .unwrap(),
            float(0.125)
        );

        for input in ["1970-01-01 00:00:00.500", "1970-01-01 00:00:00.999"] {
            assert_eq!(UnixepochFunc.invoke(&[text(input)]).unwrap(), int(0));
            assert_text(
                &StrftimeFunc.invoke(&[text("%s"), text(input)]).unwrap(),
                "0",
            );
        }
        assert_eq!(
            UnixepochFunc
                .invoke(&[text("1969-12-31 23:59:59.999")])
                .unwrap(),
            int(-1)
        );
        assert_text(
            &StrftimeFunc
                .invoke(&[text("%s"), text("1969-12-31 23:59:59.999")])
                .unwrap(),
            "-1",
        );
    }

    #[test]
    fn test_last_representable_millisecond_is_valid() {
        // date.c validJulianDay admits iJD up to 9999-12-31 23:59:59.999.
        assert_text(
            &DateTimeFunc
                .invoke(&[text("9999-12-31 23:59:59.999"), text("subsec")])
                .unwrap(),
            "9999-12-31 23:59:59.999",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[
                    text("9999-12-31 23:59:59"),
                    text("+0.999 seconds"),
                    text("subsec"),
                ])
                .unwrap(),
            "9999-12-31 23:59:59.999",
        );
        assert_eq!(
            DateTimeFunc
                .invoke(&[text("9999-12-31 23:59:59"), text("+1.0 seconds")])
                .unwrap(),
            SqliteValue::Null
        );
    }

    #[test]
    fn test_subsec_fraction_truncates_at_999() {
        // SQLite 3.53 parseHhMmSs truncates fractional seconds past .999
        // (forum post 766a2c9231), so these never round up to second 60.
        assert_text(
            &TimeFunc
                .invoke(&[text("12:34:59.9995"), text("subsec")])
                .unwrap(),
            "12:34:59.999",
        );
        assert_text(
            &DateTimeFunc
                .invoke(&[text("1970-01-01 23:59:59.9995"), text("subsec")])
                .unwrap(),
            "1970-01-01 23:59:59.999",
        );
        assert_text(
            &StrftimeFunc
                .invoke(&[
                    text("%H:%M:%f|%s"),
                    text("1970-01-01 23:59:59.9995"),
                    text("subsec"),
                ])
                .unwrap(),
            "23:59:59.999|86399.999",
        );
    }

    // ── Registration ──────────────────────────────────────────────────

    #[test]
    fn test_register_datetime_builtins_all_present() {
        let mut reg = FunctionRegistry::new();
        register_datetime_builtins(&mut reg);

        let expected = [
            "date",
            "time",
            "datetime",
            "julianday",
            "unixepoch",
            "strftime",
            "timediff",
        ];

        for name in expected {
            assert!(
                reg.find_scalar(name, 1).is_some() || reg.find_scalar(name, 2).is_some(),
                "datetime function '{name}' not registered"
            );
        }
    }

    #[test]
    fn test_public_datetime_function_metadata_fails_closed_for_direct_registration() {
        assert!(!DateFunc.is_deterministic());
        assert!(!TimeFunc.is_deterministic());
        assert!(!DateTimeFunc.is_deterministic());
        assert!(!JuliandayFunc.is_deterministic());
        assert!(!UnixepochFunc.is_deterministic());
        assert!(!StrftimeFunc.is_deterministic());
        assert!(!TimediffFunc.is_deterministic());

        let mut registry = FunctionRegistry::new();
        registry.register_scalar(DateFunc);
        registry.register_scalar(TimeFunc);
        registry.register_scalar(DateTimeFunc);
        registry.register_scalar(JuliandayFunc);
        registry.register_scalar(UnixepochFunc);
        registry.register_scalar(StrftimeFunc);
        registry.register_scalar(TimediffFunc);
        for (name, num_args) in [
            ("date", 0),
            ("time", 0),
            ("datetime", 0),
            ("julianday", 0),
            ("unixepoch", 0),
            ("strftime", 1),
            ("timediff", 2),
        ] {
            assert_eq!(
                registry.scalar_schema_safety(name, num_args),
                Some(crate::ScalarSchemaSafety::Never),
                "generic registration of {name}/{num_args} must fail closed"
            );
        }
    }

    // ── JDN roundtrip ─────────────────────────────────────────────────

    #[test]
    fn test_modifier_year_overflow() {
        // "+9223372036854775807 years" causes i64 overflow in year calculation.
        // Should return NULL, not panic.
        let huge = i64::MAX;
        let modifier = format!("+{huge} years");
        let r = DateFunc.invoke(&[text("2000-01-01"), text(&modifier)]);
        // The implementation should catch overflow and return Ok(Null), or at least not panic.
        // If it panics, the test harness catches it (but we want to prevent panics).
        assert_eq!(r.unwrap(), SqliteValue::Null);
    }

    #[test]
    fn test_jdn_roundtrip() {
        // Test that ymd → jdn → ymd roundtrips correctly.
        let dates = [
            (2024, 3, 15),
            (2000, 1, 1),
            (1970, 1, 1),
            (2024, 2, 29),
            (1900, 1, 1),
            (2099, 12, 31),
        ];
        for (y, m, d) in dates {
            let jdn = ymd_to_jdn(y, m, d);
            let (y2, m2, d2) = jdn_to_ymd(jdn);
            assert_eq!(
                (y, m, d),
                (y2, m2, d2),
                "roundtrip failed for {y}-{m}-{d} (JDN={jdn})"
            );
        }
    }

    #[test]
    fn test_unix_epoch_roundtrip() {
        let jdn = ymd_to_jdn(1970, 1, 1);
        let unix = jdn_to_unix(jdn);
        assert_eq!(unix, 0, "Unix epoch should be 0");

        let jdn2 = unix_to_jdn(0.0);
        assert!((jdn2 - UNIX_EPOCH_JDN).abs() < 1e-10, "roundtrip failed");
    }

    #[test]
    fn test_hour_24_start_of_uses_written_ymd_until_recomputed_bd_a20bp() {
        let cases = [
            (&["2024-01-31 24:00", "start of day"][..], "2024-01-31"),
            (&["2024-01-31 24:00", "start of month"][..], "2024-01-01"),
            (&["2024-01-31 24:00", "start of year"][..], "2024-01-01"),
            (&["2024-12-31 24:00", "start of day"][..], "2024-12-31"),
            (&["2024-12-31 24:00", "start of month"][..], "2024-12-01"),
            (&["2024-12-31 24:00", "start of year"][..], "2024-01-01"),
            (
                &["2024-01-31 24:00", "subsec", "start of month"][..],
                "2024-01-01",
            ),
            (
                &["2024-01-31 24:00Z", "utc", "start of month"][..],
                "2024-01-01",
            ),
            (
                &["2024-01-31 24:00", "+0 days", "start of day"][..],
                "2024-02-01",
            ),
            (
                &["2024-01-31 24:00", "+0 days", "start of month"][..],
                "2024-02-01",
            ),
            (
                &["2024-01-31 24:00", "+1 days", "start of day"][..],
                "2024-02-02",
            ),
            (
                &["2024-01-31 24:00", "+1 days", "start of month"][..],
                "2024-02-01",
            ),
            (
                &["2024-12-31 24:00", "+0 days", "start of year"][..],
                "2025-01-01",
            ),
            (
                &["2024-12-31 24:00", "+1 days", "start of year"][..],
                "2025-01-01",
            ),
        ];

        for (raw_args, expected) in cases {
            let args: Vec<SqliteValue> = raw_args.iter().copied().map(text).collect();
            let got = DateFunc.invoke(&args).expect("date() should succeed");
            assert_eq!(got, text(expected), "failed for args={raw_args:?}");
        }
    }
}
