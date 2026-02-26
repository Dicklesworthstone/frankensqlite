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

use fsqlite_error::Result;
use fsqlite_types::SqliteValue;

use crate::{FunctionRegistry, ScalarFunction};

// ── Julian Day Number Conversions ─────────────────────────────────────────
//
// Algorithms from Meeus, "Astronomical Algorithms" (1991).

/// Gregorian (y, m, d, h, min, sec, frac_sec) → Julian Day Number.
fn ymd_to_jdn(y: i64, m: i64, d: i64) -> f64 {
    let (y, m) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
    let a = y / 100;
    let b = 2 - a + a / 4;
    (365.25 * (y + 4716) as f64).floor() + (30.6001 * (m + 1) as f64).floor() + d as f64 + b as f64
        - 1524.5
}

/// Julian Day Number → Gregorian (year, month, day).
///
/// Uses saturating/wrapping-safe arithmetic so that extreme JDN values
/// (from overflowed modifier chains) produce deterministic garbage rather
/// than panicking.  Callers that care about validity should bounds-check
/// the JDN before calling.
fn jdn_to_ymd(jdn: f64) -> (i64, i64, i64) {
    let z = (jdn + 0.5).floor() as i64;
    let a = if z < 2_299_161 {
        z
    } else {
        let alpha = ((z as f64 - 1_867_216.25) / 36524.25).floor() as i64;
        z.saturating_add(1)
            .saturating_add(alpha)
            .saturating_sub(alpha / 4)
    };
    let b = a.saturating_add(1524);
    let c = ((b as f64 - 122.1) / 365.25).floor() as i64;
    let d = (365.25 * c as f64).floor() as i64;
    let e = ((b.saturating_sub(d)) as f64 / 30.6001).floor() as i64;

    let day = b
        .saturating_sub(d)
        .saturating_sub((30.6001 * e as f64).floor() as i64);
    let month = if e < 14 { e - 1 } else { e - 13 };
    let year = if month > 2 { c - 4716 } else { c - 4715 };
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
        doy += days_in_month(y, mo);
    }
    doy
}

// ── Time String Parsing ───────────────────────────────────────────────────

/// Parse a SQLite time string into a JDN.
fn parse_timestring(s: &str) -> Option<f64> {
    let s = s.trim();

    // Special value: 'now' — we use a fixed time for determinism in tests.
    // Real 'now' will come from the Cx connection time source later.
    if s.eq_ignore_ascii_case("now") {
        // Return a placeholder JDN for 2000-01-01 00:00:00.
        return Some(ymd_to_jdn(2000, 1, 1));
    }

    // Try as a Julian Day Number (bare float).
    if let Ok(jdn) = s.parse::<f64>() {
        if jdn >= 0.0 {
            return Some(jdn);
        }
    }

    // ISO-8601 variants.
    parse_iso8601(s)
}

fn parse_iso8601(s: &str) -> Option<f64> {
    // YYYY-MM-DD HH:MM:SS.SSS  or  YYYY-MM-DDTHH:MM:SS.SSS
    // YYYY-MM-DD HH:MM:SS  or  YYYY-MM-DD HH:MM
    // YYYY-MM-DD
    // HH:MM:SS.SSS  (bare time → 2000-01-01)
    // HH:MM:SS
    // HH:MM

    let bytes = s.as_bytes();
    let len = bytes.len();

    // Try date-only or date+time.
    if len >= 10 && bytes[4] == b'-' && bytes[7] == b'-' {
        let y = s[0..4].parse::<i64>().ok()?;
        let m = s[5..7].parse::<i64>().ok()?;
        let d = s[8..10].parse::<i64>().ok()?;

        if m < 1 || m > 12 || d < 1 || d > days_in_month(y, m) {
            return None;
        }

        if len == 10 {
            return Some(ymd_to_jdn(y, m, d));
        }

        // Separator: space or 'T'.
        if len > 10 && (bytes[10] == b' ' || bytes[10] == b'T') {
            let time_part = &s[11..];
            let (h, mi, sec, frac) = parse_time_part(time_part)?;
            return Some(ymdhms_to_jdn(y, m, d, h, mi, sec, frac));
        }
        return None;
    }

    // Bare time: HH:MM:SS or HH:MM:SS.SSS or HH:MM
    if len >= 5 && bytes[2] == b':' {
        let (h, mi, sec, frac) = parse_time_part(s)?;
        return Some(ymdhms_to_jdn(2000, 1, 1, h, mi, sec, frac));
    }

    None
}

/// Parse "HH:MM:SS.SSS" or "HH:MM:SS" or "HH:MM".
fn parse_time_part(s: &str) -> Option<(i64, i64, i64, f64)> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() < 2 {
        return None;
    }
    let h = parts[0].parse::<i64>().ok()?;
    let mi = parts[1].parse::<i64>().ok()?;
    if !(0..=23).contains(&h) || !(0..=59).contains(&mi) {
        return None;
    }

    if parts.len() == 2 {
        return Some((h, mi, 0, 0.0));
    }

    // Third part may have fractional seconds: "SS" or "SS.SSS".
    let sec_str = parts[2];
    if let Some(dot_pos) = sec_str.find('.') {
        let sec = sec_str[..dot_pos].parse::<i64>().ok()?;
        let frac_str = &sec_str[dot_pos..]; // ".SSS"
        let frac = frac_str.parse::<f64>().ok()?;
        if !(0..=59).contains(&sec) {
            return None;
        }
        Some((h, mi, sec, frac))
    } else {
        let sec = sec_str.parse::<i64>().ok()?;
        if !(0..=59).contains(&sec) {
            return None;
        }
        Some((h, mi, sec, 0.0))
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

    // 'localtime' / 'utc' — timezone conversion stubs.
    // Full implementation requires system timezone access; stub for now.
    if m == "localtime" || m == "utc" {
        return Some(jdn);
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
        if diff <= 0 {
            diff += 7;
        }
        // If already the target weekday, advance 7 days (SQLite behavior).
        return Some(jdn + diff as f64);
    }

    // Arithmetic: '+NNN days', '-NNN hours', etc.
    parse_arithmetic_modifier(&m).map(|delta| jdn + delta)
}

/// Parse "+NNN unit" / "-NNN unit" and return the JDN delta.
fn parse_arithmetic_modifier(m: &str) -> Option<f64> {
    let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
        (1.0, r.trim())
    } else if let Some(r) = m.strip_prefix('-') {
        (-1.0, r.trim())
    } else {
        return None;
    };

    let mut parts = rest.splitn(2, ' ');
    let num_str = parts.next()?;
    let unit = parts.next()?.trim();

    let num = num_str.parse::<f64>().ok()?;
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

/// Apply a sequence of modifiers, also tracking the 'subsec' flag.
fn apply_modifiers(jdn: f64, modifiers: &[String]) -> Option<(f64, bool)> {
    let mut j = jdn;
    let mut subsec = false;
    for m in modifiers {
        let m_lower = m.trim().to_ascii_lowercase();
        if m_lower == "subsec" || m_lower == "subsecond" {
            subsec = true;
        }
        // Month/year modifiers need special handling for exact date math.
        // If exact arithmetic overflows (returns None), the modifier is
        // out of representable range — return NULL rather than falling
        // through to the approximate path which would produce overflow
        // panics in jdn_to_ymd.
        if is_month_year_modifier(&m_lower) {
            j = apply_month_year_exact(j, &m_lower)?;
            continue;
        }
        j = apply_modifier(j, m)?;
    }
    Some((j, subsec))
}

fn is_month_year_modifier(m: &str) -> bool {
    (m.contains("month") || m.contains("year")) && (m.starts_with('+') || m.starts_with('-'))
}

/// Exact month/year arithmetic by decomposing to YMD.
fn apply_month_year_exact(jdn: f64, m: &str) -> Option<f64> {
    let (sign, rest) = if let Some(r) = m.strip_prefix('+') {
        (1_i64, r.trim())
    } else if let Some(r) = m.strip_prefix('-') {
        (-1_i64, r.trim())
    } else {
        return None;
    };

    let mut parts = rest.splitn(2, ' ');
    let num_str = parts.next()?;
    let unit = parts.next()?.trim();
    let num = num_str.parse::<i64>().ok()?;

    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);

    let total_months = match unit.trim_end_matches('s') {
        "month" => num.checked_mul(sign)?,
        "year" => num.checked_mul(sign)?.checked_mul(12)?,
        _ => return None,
    };

    // (y * 12 + (mo - 1)) + total_months
    let current_months = y.checked_mul(12)?.checked_add(mo - 1)?;
    let new_total = current_months.checked_add(total_months)?;

    let new_y = new_total.div_euclid(12);
    let new_mo = new_total.rem_euclid(12) + 1;
    let new_d = d.min(days_in_month(new_y, new_mo));

    Some(ymdhms_to_jdn(new_y, new_mo, new_d, h, mi, s, frac))
}

// ── Output Formatters ─────────────────────────────────────────────────────

fn format_date(jdn: f64) -> String {
    let (y, m, d) = jdn_to_ymd(jdn);
    format!("{y:04}-{m:02}-{d:02}")
}

fn format_time(jdn: f64, subsec: bool) -> String {
    let (h, m, s, frac) = jdn_to_hms(jdn);
    if subsec && frac > 1e-9 {
        format!("{h:02}:{m:02}:{s:02}.{:03}", (frac * 1000.0).round() as i64)
    } else {
        format!("{h:02}:{m:02}:{s:02}")
    }
}

fn format_datetime(jdn: f64, subsec: bool) -> String {
    format!("{} {}", format_date(jdn), format_time(jdn, subsec))
}

/// strftime format engine.
fn format_strftime(fmt: &str, jdn: f64) -> String {
    let (y, mo, d) = jdn_to_ymd(jdn);
    let (h, mi, s, frac) = jdn_to_hms(jdn);
    let doy = day_of_year(y, mo, d);
    // Day of week: 0=Sunday.
    let jdn_int = (jdn + 0.5).floor() as i64;
    let dow = (jdn_int + 1) % 7; // 0=Sunday, 6=Saturday

    let mut result = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '%' && i + 1 < chars.len() {
            i += 1;
            match chars[i] {
                'd' => result.push_str(&format!("{d:02}")),
                'e' => result.push_str(&format!("{d:>2}")), // space-padded day
                'f' => {
                    // Seconds with fractional part.
                    let total = s as f64 + frac;
                    result.push_str(&format!("{total:06.3}"));
                }
                'H' => result.push_str(&format!("{h:02}")),
                'I' => {
                    // 12-hour clock.
                    let h12 = if h == 0 {
                        12
                    } else if h > 12 {
                        h - 12
                    } else {
                        h
                    };
                    result.push_str(&format!("{h12:02}"));
                }
                'j' => result.push_str(&format!("{doy:03}")),
                'J' => result.push_str(&format!("{jdn:.6}")),
                'k' => {
                    // Space-padded 24-hour.
                    result.push_str(&format!("{h:>2}"));
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
                    result.push_str(&format!("{h12:>2}"));
                }
                'm' => result.push_str(&format!("{mo:02}")),
                'M' => result.push_str(&format!("{mi:02}")),
                'p' => {
                    result.push_str(if h < 12 { "AM" } else { "PM" });
                }
                'P' => {
                    result.push_str(if h < 12 { "am" } else { "pm" });
                }
                'R' => result.push_str(&format!("{h:02}:{mi:02}")),
                's' => {
                    let unix = jdn_to_unix(jdn);
                    result.push_str(&unix.to_string());
                }
                'S' => result.push_str(&format!("{s:02}")),
                'T' => result.push_str(&format!("{h:02}:{mi:02}:{s:02}")),
                'u' => {
                    // ISO 8601 day of week: 1=Monday, 7=Sunday.
                    let u = if dow == 0 { 7 } else { dow };
                    result.push_str(&u.to_string());
                }
                'w' => result.push_str(&dow.to_string()),
                'W' => {
                    // Week of year (Monday as first day of week, 00-53).
                    let w = (doy + 6 - ((dow + 6) % 7)) / 7;
                    result.push_str(&format!("{w:02}"));
                }
                'Y' => result.push_str(&format!("{y:04}")),
                'G' | 'g' | 'V' => {
                    // ISO 8601 week-based year/week.
                    let (iso_y, iso_w) = iso_week(y, mo, d);
                    match chars[i] {
                        'G' => result.push_str(&format!("{iso_y:04}")),
                        'g' => result.push_str(&format!("{:02}", iso_y % 100)),
                        'V' => result.push_str(&format!("{iso_w:02}")),
                        _ => unreachable!(),
                    }
                }
                '%' => result.push('%'),
                other => {
                    result.push('%');
                    result.push(other);
                }
            }
        } else {
            result.push(chars[i]);
        }
        i += 1;
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
    let (start_h, start_mi, start_s, start_frac) = jdn_to_hms(start_jdn);
    let mut start_ms = (start_frac * 1000.0).round() as i64;
    if start_ms >= 1000 {
        start_ms = 999;
    }

    let (end_y, end_mo, end_d) = jdn_to_ymd(end_jdn);
    let (end_h, end_mi, end_s, end_frac) = jdn_to_hms(end_jdn);
    let mut end_ms = (end_frac * 1000.0).round() as i64;
    if end_ms >= 1000 {
        end_ms = 999;
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

    let input = match &args[0] {
        SqliteValue::Text(s) => parse_timestring(s)?,
        SqliteValue::Integer(i) => *i as f64,
        SqliteValue::Float(f) => *f,
        _ => return None,
    };

    let modifiers: Vec<String> = args[1..]
        .iter()
        .filter_map(|a| if a.is_null() { None } else { Some(a.to_text()) })
        .collect();

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
            Some((jdn, _)) => Ok(SqliteValue::Integer(jdn_to_unix(jdn))),
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
        let fmt = args[0].to_text();
        let rest = &args[1..];
        match parse_args(rest) {
            Some((jdn, _)) => Ok(SqliteValue::Text(format_strftime(&fmt, jdn))),
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
            (Some(j1), Some(j2)) => Ok(SqliteValue::Text(timediff_impl(j1, j2))),
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
        SqliteValue::Text(s.to_owned())
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
            SqliteValue::Text(s) => assert_eq!(s, expected, "text mismatch"),
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
        // 2024-01-31 + 1 month: Feb only has 29 days in 2024 (leap year).
        let r = DateFunc
            .invoke(&[text("2024-01-31"), text("+1 months")])
            .unwrap();
        assert_text(&r, "2024-02-29");
    }

    #[test]
    fn test_modifier_years() {
        // 2024-02-29 + 1 year: 2025 is not a leap year.
        let r = DateFunc
            .invoke(&[text("2024-02-29"), text("+1 years")])
            .unwrap();
        assert_text(&r, "2025-02-28");
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
        let r = DateTimeFunc
            .invoke(&[
                text("2024-03-15 14:30:45"),
                text("utc"),
                text("localtime"),
                text("utc"),
            ])
            .unwrap();
        assert_text(&r, "2024-03-15 14:30:45");
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
    fn test_modifier_weekday_same_day_advances_next_week() {
        // 2024-03-17 is Sunday; SQLite semantics advance to the NEXT Sunday.
        let r = DateFunc
            .invoke(&[text("2024-03-17"), text("weekday 0")])
            .unwrap();
        assert_text(&r, "2024-03-24");
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
