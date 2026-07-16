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

/// Return the UTC offset in seconds, interpreting the components as **local** time.
///
/// Used by the `utc` modifier (local → UTC): the input JDN is local time,
/// so we ask chrono "what UTC offset applies at this local time?"
#[cfg(not(target_arch = "wasm32"))]
fn utc_offset_for_local_datetime(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
    use chrono::{Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
    let date = NaiveDate::from_ymd_opt(y, mo, d).unwrap_or_default();
    let time = NaiveTime::from_hms_opt(h, mi, s).unwrap_or_default();
    let naive = NaiveDateTime::new(date, time);
    match Local.from_local_datetime(&naive).earliest() {
        Some(dt) => dt.offset().local_minus_utc() as i64,
        None => 0, // ambiguous or nonexistent time (DST gap)
    }
}

#[cfg(target_arch = "wasm32")]
fn utc_offset_for_local_datetime(_y: i32, _mo: u32, _d: u32, _h: u32, _mi: u32, _s: u32) -> i64 {
    0
}

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

/// Compute the UTC offset for the `localtime` modifier (UTC → local).
fn utc_offset_for_utc_jdn(jdn: f64) -> i64 {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, _frac) = jdn_to_hms(jdn);
    utc_offset_for_utc_datetime(y as i32, mo as u32, d as u32, h as u32, mi as u32, s as u32)
}

/// Compute the UTC offset for the `utc` modifier (local → UTC).
fn utc_offset_for_local_jdn(jdn: f64) -> i64 {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, _frac) = jdn_to_hms(jdn);
    utc_offset_for_local_datetime(y as i32, mo as u32, d as u32, h as u32, mi as u32, s as u32)
}

// ── Julian Day Number Conversions ─────────────────────────────────────────
//
// Algorithms from Meeus, "Astronomical Algorithms" (1991).

/// Gregorian (y, m, d, h, min, sec, frac_sec) → Julian Day Number.
fn ymd_to_jdn(y: i64, m: i64, d: i64) -> f64 {
    let (y, m) = if m <= 2 {
        (y.saturating_sub(1), m.saturating_add(12))
    } else {
        (y, m)
    };
    let a = y / 100;
    let b = 2_i64.saturating_sub(a).saturating_add(a / 4);
    (365.25 * y.saturating_add(4716) as f64).floor()
        + (30.6001 * m.saturating_add(1) as f64).floor()
        + d as f64
        + b as f64
        - 1524.5
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
/// Unix timestamp bounds used by SQLite's `auto` modifier.
const AUTO_UNIX_MIN: f64 = -210_866_760_000.0;
const AUTO_UNIX_MAX: f64 = 253_402_300_799.0;

fn jdn_to_unix(jdn: f64) -> i64 {
    ((jdn - UNIX_EPOCH_JDN) * 86400.0).round() as i64
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

/// Parse a SQLite time string into a JDN.
fn parse_timestring(s: &str) -> Option<f64> {
    let s = s.trim();

    // Special value: 'now' — return the current UTC wall-clock time as JDN.
    // C SQLite (date.c:451) captures time once per sqlite3_step() via the VFS
    // and caches it; we use SystemTime::now() which gives per-call resolution.
    // A future refinement (Track: Cx time source) could freeze time at
    // statement start for full C SQLite compatibility.
    if s.eq_ignore_ascii_case("now") {
        use std::time::{SystemTime, UNIX_EPOCH};
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        // Unix epoch (1970-01-01 00:00:00) = JDN 2_440_587.5
        return Some(2_440_587.5 + secs / 86_400.0);
    }

    // Try as a Julian Day Number (bare float).
    // Reject non-finite values (NaN/Inf) — sqlite3AtoF doesn't recognize them.
    if let Ok(jdn) = s.parse::<f64>() {
        if jdn >= 0.0 && jdn.is_finite() {
            return Some(jdn);
        }
    }

    // ISO-8601 variants.
    parse_iso8601(s)
}

fn parse_iso8601(s: &str) -> Option<f64> {
    // YYYY-MM-DD HH:MM:SS.SSS[Z|±HH:MM]  or  YYYY-MM-DDTHH:MM:SS.SSS[Z|±HH:MM]
    // YYYY-MM-DD HH:MM:SS[Z|±HH:MM]  or  YYYY-MM-DD HH:MM[Z|±HH:MM]
    // YYYY-MM-DD
    // HH:MM:SS.SSS[Z|±HH:MM]  (bare time → 2000-01-01)
    // HH:MM:SS[Z|±HH:MM]
    // HH:MM[Z|±HH:MM]

    let bytes = s.as_bytes();
    let len = bytes.len();

    // Try date-only or date+time.
    if len >= 10 && bytes[4] == b'-' && bytes[7] == b'-' {
        let y = s[0..4].parse::<i64>().ok()?;
        let m = s[5..7].parse::<i64>().ok()?;
        let d = s[8..10].parse::<i64>().ok()?;

        if m < 1 || m > 12 || d < 1 || d > 31 {
            return None;
        }

        if len == 10 {
            return Some(ymd_to_jdn(y, m, d));
        }

        // Separator: space or 'T'.
        if len > 10 && (bytes[10] == b' ' || bytes[10] == b'T') {
            let time_part = &s[11..];
            let (h, mi, sec, frac, tz_offset_min) = parse_time_part_with_tz(time_part)?;
            let jdn = ymdhms_to_jdn(y, m, d, h, mi, sec, frac);
            // Apply TZ offset: subtract the offset to convert local → UTC.
            // For "+01:00", local = UTC + 1h, so UTC = local - 1h.
            return Some(jdn - (tz_offset_min as f64) / 1440.0);
        }
        return None;
    }

    // Bare time: HH:MM:SS or HH:MM:SS.SSS or HH:MM, optionally with TZ suffix.
    if len >= 5 && bytes[2] == b':' {
        let (h, mi, sec, frac, tz_offset_min) = parse_time_part_with_tz(s)?;
        let jdn = ymdhms_to_jdn(2000, 1, 1, h, mi, sec, frac);
        return Some(jdn - (tz_offset_min as f64) / 1440.0);
    }

    None
}

/// Split an optional trailing ISO-8601 timezone suffix (`Z`, `+HH:MM`, `-HH:MM`,
/// or the compact `±HHMM` / `±HH` forms) from a time string.
///
/// Returns the time string without the suffix and the offset in minutes east
/// of UTC.  `+01:00` returns `60`, `-05:30` returns `-330`, `Z` returns `0`.
///
/// If no recognised suffix is present, returns `(s, 0)`.
fn split_tz_suffix(s: &str) -> Option<(&str, i64)> {
    // Trailing 'Z' or 'z' → UTC.
    if let Some(stripped) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        return Some((stripped, 0));
    }

    // Find the last '+' or '-' that could plausibly start a tz offset.
    // The sign must appear AFTER the seconds (or minutes) portion — i.e.
    // after the last ':' or after a '.' fractional seconds block — so we
    // never confuse a negative fractional value with a tz sign.
    //
    // Scan from the right: the first '+' or '-' we hit *before* any
    // alphanumeric mismatch is the tz sign, provided the remainder is a
    // well-formed HH[:MM] / HHMM offset.
    let bytes = s.as_bytes();
    // Look at the trailing 6 chars ("+HH:MM"), 5 chars ("+HHMM"), or 3 chars ("+HH").
    for width in [6usize, 5, 3] {
        if bytes.len() < width + 1 {
            continue;
        }
        let split_at = bytes.len() - width;
        let sign_byte = bytes[split_at];
        if sign_byte != b'+' && sign_byte != b'-' {
            continue;
        }
        let tz_part = &s[split_at..];
        if let Some(offset) = parse_tz_offset(tz_part) {
            return Some((&s[..split_at], offset));
        }
    }

    // No recognised TZ suffix.
    Some((s, 0))
}

/// Parse an ISO-8601 timezone offset like `+01:00`, `-05:30`, `+0100`, or `+05`.
/// Returns the offset in minutes east of UTC, or None if not a valid offset.
fn parse_tz_offset(tz: &str) -> Option<i64> {
    let bytes = tz.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let sign: i64 = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let rest = &tz[1..];
    let (hours, minutes) = match rest.len() {
        // ±HH:MM
        5 if rest.as_bytes()[2] == b':' => (
            rest[0..2].parse::<i64>().ok()?,
            rest[3..5].parse::<i64>().ok()?,
        ),
        // ±HHMM
        4 => (
            rest[0..2].parse::<i64>().ok()?,
            rest[2..4].parse::<i64>().ok()?,
        ),
        // ±HH
        2 => (rest.parse::<i64>().ok()?, 0),
        _ => return None,
    };
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    Some(sign * (hours * 60 + minutes))
}

/// Parse "HH:MM:SS.SSS" or "HH:MM:SS" or "HH:MM", optionally followed by a
/// timezone suffix.  Returns `(h, mi, sec, frac, tz_offset_minutes)`.
fn parse_time_part_with_tz(s: &str) -> Option<(i64, i64, i64, f64, i64)> {
    let (time_only, tz_offset_min) = split_tz_suffix(s)?;
    let (h, mi, sec, frac) = parse_time_part(time_only)?;
    Some((h, mi, sec, frac, tz_offset_min))
}

/// Parse "HH:MM:SS.SSS" or "HH:MM:SS" or "HH:MM".
fn parse_time_part(s: &str) -> Option<(i64, i64, i64, f64)> {
    let [h_tens, h_ones, b':', mi_tens, mi_ones, rest @ ..] = s.as_bytes() else {
        return None;
    };
    // C SQLite's computeHMS uses getDigits with a "20" field width,
    // meaning exactly 2 bare decimal digits per time component.  Reject
    // fields that don't match that shape: wrong length, leading signs
    // (`+01`), or non-digit characters.
    let h = parse_two_ascii_digits(*h_tens, *h_ones)?;
    let mi = parse_two_ascii_digits(*mi_tens, *mi_ones)?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&mi) {
        return None;
    }

    // Third part may have fractional seconds: "SS" or "SS.SSS".
    // Apply the same 2-digit bare-digits constraint as hours/minutes:
    // C SQLite's computeHMS requires the seconds integer to be exactly
    // 2 decimal digits.
    match rest {
        [] => Some((h, mi, 0, 0.0)),
        [b':', sec_tens, sec_ones] => {
            let sec = parse_two_ascii_digits(*sec_tens, *sec_ones)?;
            if !(0..=59).contains(&sec) {
                return None;
            }
            Some((h, mi, sec, 0.0))
        }
        [b':', sec_tens, sec_ones, b'.', ..] => {
            let sec = parse_two_ascii_digits(*sec_tens, *sec_ones)?;
            if !(0..=59).contains(&sec) {
                return None;
            }
            let frac = s.get(8..)?.parse::<f64>().ok()?;
            Some((h, mi, sec, frac))
        }
        _ => None,
    }
}

#[inline]
fn parse_two_ascii_digits(tens: u8, ones: u8) -> Option<i64> {
    if tens.is_ascii_digit() && ones.is_ascii_digit() {
        Some(i64::from((tens - b'0') * 10 + (ones - b'0')))
    } else {
        None
    }
}

// ── Modifier Pipeline ─────────────────────────────────────────────────────

/// Apply a single modifier string to a JDN.  Returns None if invalid.
fn apply_modifier(jdn: f64, modifier: &str) -> Option<f64> {
    let m = modifier.trim().to_ascii_lowercase();

    // 'start of month' / 'start of year' / 'start of day'
    if m == "start of month" {
        let (y, mo, _d) = jdn_to_ymd(jdn);
        return Some(ymd_to_jdn(y, mo, 1));
    }
    if m == "start of year" {
        let (y, _mo, _d) = jdn_to_ymd(jdn);
        return Some(ymd_to_jdn(y, 1, 1));
    }
    if m == "start of day" {
        let (y, mo, d) = jdn_to_ymd(jdn);
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
    // 'utc' — convert local time to UTC.  The input JDN is local time, so
    // we compute the offset by interpreting the datetime as local.
    if m == "utc" {
        let offset = utc_offset_for_local_jdn(jdn);
        return Some(jdn - offset as f64 / 86400.0);
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

/// Parse "+NNN unit" / "-NNN unit" and return the JDN delta.
fn parse_arithmetic_modifier(m: &str) -> Option<f64> {
    let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
        (1.0, r.trim())
    } else {
        let r = m.strip_prefix('-')?;
        (-1.0, r.trim())
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

/// Apply a sequence of modifiers, also tracking the 'subsec' flag.
fn apply_modifiers(jdn: f64, modifiers: &[String]) -> Option<(f64, bool)> {
    let mut j = jdn;
    let mut subsec = false;
    // Pending day-of-month overflow from the most recent +N month/year shift,
    // consumed by a later 'floor' modifier (mirrors SQLite's DateTime.nFloor).
    let mut n_floor: i64 = 0;
    for m in modifiers {
        let m_lower = m.trim().to_ascii_lowercase();
        if m_lower == "subsec" || m_lower == "subsecond" {
            subsec = true;
            continue;
        }
        // 'ceiling' is the default day-of-month-overflow behavior (roll forward
        // into the next month); it merely clears any pending floor adjustment.
        // 'floor' rolls the date back to the last day of the prior month by
        // subtracting the overflow days that the preceding month/year shift left.
        if m_lower == "ceiling" {
            n_floor = 0;
            continue;
        }
        if m_lower == "floor" {
            j -= n_floor as f64;
            n_floor = 0;
            continue;
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
        // Any other date-changing modifier clears the pending floor.
        n_floor = 0;
        j = apply_modifier(j, m)?;
    }
    Some((j, subsec))
}

fn is_month_year_modifier(m: &str) -> bool {
    (m.contains("month") || m.contains("year")) && (m.starts_with('+') || m.starts_with('-'))
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
        return Err(());
    };

    let mut parts = rest.splitn(2, ' ');
    let num_str = parts.next().ok_or(())?;
    let unit = parts.next().ok_or(())?.trim();

    // SQLite uses exact math only if the value is an integer.
    let num = if let Ok(n) = num_str.parse::<i64>() {
        n
    } else if let Ok(f) = num_str.parse::<f64>() {
        if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            f as i64
        } else {
            return Err(());
        }
    } else {
        return Err(());
    };

    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);

    let total_months = match unit.trim_end_matches('s') {
        "month" => {
            if let Some(val) = num.checked_mul(sign) {
                val
            } else {
                return Ok(None);
            }
        }
        "year" => {
            if let Some(val) = num.checked_mul(sign).and_then(|v| v.checked_mul(12)) {
                val
            } else {
                return Ok(None);
            }
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
        ymdhms_to_jdn(new_y, new_mo, d, h, mi, s, frac),
        n_floor,
    )))
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

fn format_date(jdn: f64) -> SmallText {
    let (y, m, d) = jdn_to_ymd(jdn);
    build_small_text(move |w| write!(w, "{y:04}-{m:02}-{d:02}"))
}

fn format_time(jdn: f64, subsec: bool) -> SmallText {
    let (h, m, s, frac) = jdn_to_hms(jdn);
    if subsec && frac > 1e-9 {
        let ms = (frac * 1000.0).round() as i64;
        build_small_text(move |w| write!(w, "{h:02}:{m:02}:{s:02}.{ms:03}"))
    } else {
        build_small_text(move |w| write!(w, "{h:02}:{m:02}:{s:02}"))
    }
}

fn format_datetime(jdn: f64, subsec: bool) -> SmallText {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);
    if subsec && frac > 1e-9 {
        let ms = (frac * 1000.0).round() as i64;
        build_small_text(move |w| write!(w, "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{ms:03}"))
    } else {
        build_small_text(move |w| write!(w, "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}"))
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
fn format_strftime(fmt: &str, jdn: f64) -> String {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);
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
                let total = s as f64 + frac;
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
                // C SQLite uses %.15g which strips trailing zeros.
                push_format(&mut result, format_args!("{jdn:.15}"));
                while result.as_bytes().last() == Some(&b'0') {
                    result.pop();
                }
                if result.as_bytes().last() == Some(&b'.') {
                    result.pop();
                }
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
                let unix = jdn_to_unix(jdn);
                push_format(&mut result, format_args!("{unix}"));
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
            other => {
                result.push('%');
                result.push(other);
            }
        }
    }

    if literal_start < fmt.len() {
        result.push_str(&fmt[literal_start..]);
    }

    result
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

/// Parse args: first arg is time string, rest are modifiers.
fn parse_args(args: &[SqliteValue]) -> Option<(f64, bool)> {
    if args.is_empty() || args[0].is_null() {
        return None;
    }

    let numeric_input = matches!(&args[0], SqliteValue::Integer(_) | SqliteValue::Float(_));
    let input = match &args[0] {
        SqliteValue::Text(s) => parse_timestring(s)?,
        SqliteValue::Integer(i) => *i as f64,
        SqliteValue::Float(f) => *f,
        _ => return None,
    };

    // C SQLite: a NULL modifier causes the entire function to return NULL
    // (date.c:1127). Previously, NULL modifiers were silently skipped.
    if args[1..].iter().any(SqliteValue::is_null) {
        return None;
    }
    let modifiers: Vec<String> = args[1..].iter().map(SqliteValue::to_text).collect();

    // C SQLite rejects a numeric argument outside the representable
    // Julian-day range (date.c validJulianDay), returning NULL rather than
    // formatting a saturated garbage date — unless the first modifier
    // reinterprets the raw number ('unixepoch', 'julianday', 'auto').
    if numeric_input {
        let first = modifiers
            .first()
            .map(|modifier| modifier.trim().to_ascii_lowercase());
        let reinterprets_raw = matches!(first.as_deref(), Some("unixepoch" | "julianday" | "auto"));
        if !reinterprets_raw && !(0.0..=AUTO_JDN_MAX).contains(&input) {
            return None;
        }
    }

    apply_modifiers(input, &modifiers)
}

// ── date() ────────────────────────────────────────────────────────────────

pub struct DateFunc;

impl ScalarFunction for DateFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        match parse_args(args) {
            Some((jdn, _)) => Ok(SqliteValue::Text(format_date(jdn))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
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
            Some((jdn, subsec)) => Ok(SqliteValue::Text(format_time(jdn, subsec))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
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
            Some((jdn, subsec)) => Ok(SqliteValue::Text(format_datetime(jdn, subsec))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
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
            Some((jdn, _)) => Ok(SqliteValue::Float(jdn)),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
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
            Some((jdn, true)) => {
                let secs = (jdn - UNIX_EPOCH_JDN) * 86400.0;
                let rounded = (secs * 1000.0).round() / 1000.0;
                Ok(SqliteValue::Float(rounded))
            }
            Some((jdn, false)) => Ok(SqliteValue::Integer(jdn_to_unix(jdn))),
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
    }

    fn name(&self) -> &str {
        "unixepoch"
    }
}

// ── strftime() ────────────────────────────────────────────────────────────

pub struct StrftimeFunc;

impl ScalarFunction for StrftimeFunc {
    fn invoke(&self, args: &[SqliteValue]) -> Result<SqliteValue> {
        if args.len() < 2 || args[0].is_null() || args[1].is_null() {
            return Ok(SqliteValue::Null);
        }
        let rest = &args[1..];
        match parse_args(rest) {
            Some((jdn, _)) => {
                let fmt = match args[0].as_text_str() {
                    Some(text) => Cow::Borrowed(text),
                    None => Cow::Owned(args[0].to_text()),
                };
                Ok(SqliteValue::Text(format_strftime(fmt.as_ref(), jdn).into()))
            }
            None => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        -1
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

        let jdn1 = match &args[0] {
            SqliteValue::Text(s) => parse_timestring(s),
            SqliteValue::Integer(i) => Some(*i as f64),
            SqliteValue::Float(f) => Some(*f),
            _ => None,
        };
        let jdn2 = match &args[1] {
            SqliteValue::Text(s) => parse_timestring(s),
            SqliteValue::Integer(i) => Some(*i as f64),
            SqliteValue::Float(f) => Some(*f),
            _ => None,
        };

        match (jdn1, jdn2) {
            (Some(j1), Some(j2)) => Ok(SqliteValue::Text(timediff_impl(j1, j2).into())),
            _ => Ok(SqliteValue::Null),
        }
    }

    fn num_args(&self) -> i32 {
        2
    }

    fn name(&self) -> &str {
        "timediff"
    }
}

// ── Registration ──────────────────────────────────────────────────────────

/// Register all §13.3 date/time functions.
pub fn register_datetime_builtins(registry: &mut FunctionRegistry) {
    registry.register_scalar(DateFunc);
    registry.register_scalar(TimeFunc);
    registry.register_scalar(DateTimeFunc);
    registry.register_scalar(JuliandayFunc);
    registry.register_scalar(UnixepochFunc);
    registry.register_scalar(StrftimeFunc);
    registry.register_scalar(TimediffFunc);
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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

    // ── Basic functions ───────────────────────────────────────────────

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
    fn test_julianday_rfc3339_compact_offsets() {
        // Compact ISO-8601 offsets: ±HHMM and ±HH.
        let base = julianday_float("2026-04-07 16:00:00");
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00+0100"),
            base - 1.0 / 24.0,
            "+0100",
        );
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00-0530"),
            base + 5.5 / 24.0,
            "-0530",
        );
        assert_jdn_close(
            julianday_float("2026-04-07T16:00:00+09"),
            base - 9.0 / 24.0,
            "+09",
        );
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
        let r = TimeFunc
            .invoke(&[text("2024-01-01 12:00:00.123"), text("subsec")])
            .unwrap();
        match &r {
            SqliteValue::Text(s) => assert!(
                s.contains('.'),
                "expected fractional seconds with subsec: {s}"
            ),
            other => panic!("expected Text, got {other:?}"),
        }
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
}
