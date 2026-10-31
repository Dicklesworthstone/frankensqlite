#!/usr/bin/env bash
# Star matrices, the acceptance list for bd-star-hidden-reland-nxl8a. Every statement of shapes.sql and
# shapes_consumers.sql runs once per table function on the stock sqlite3 shell, a candidate fsqlite CLI and a baseline
# fsqlite CLI. A row is a whole statement's output.
#
# Two errors count as equal whatever their text. Stock's json `id` is a byte offset, not a counter, so that difference is
# normalised, and rows sort, because neither engine promises join order without ORDER BY. `key` becomes `value` for
# generate_series. {f} is the table function and {s} the extra tables.
#
# usage: compare.sh <candidate-cli> <baseline-cli> [<stock-sqlite3>]
# Prints every row that misses stock or differs from the baseline, then a total per file. Exits non-zero if any row is
# worse than the baseline.
set -u
here=$(cd "$(dirname "$0")" && pwd)
candidate="$1"
baseline="$2"
stock="${3:-sqlite3}"
statement=$(mktemp)
setup="CREATE TABLE t(key INTEGER, note TEXT); INSERT INTO t VALUES (0,'zero'),(1,'one'),(9,'nine');"
tables="CREATE TABLE ta(key INTEGER, n INTEGER); INSERT INTO ta VALUES (0,5); CREATE TABLE tc(key INTEGER, note TEXT COLLATE NOCASE); INSERT INTO tc VALUES (0,'ZERO');"

run() {
  "$1" :memory: < "$statement" 2>&1 | sed -E 's/[[:space:]]+$//; s/\|4\|(0?)\|\$\[1\]/|2|\1|$[1]/g' | LC_ALL=C sort
}
is_error() {
  printf '%s' "$1" | grep -qiE '^(parse error|runtime error|error|sql error|[^|]*error:|.*near line)'
}
same() {
  [ "$1" = "$2" ] && return 0
  is_error "$1" && is_error "$2"
}

worse_total=0
for shapes in "$here/shapes.sql" "$here/shapes_consumers.sql"; do
  total=0 both=0 neither=0 better=0 worse=0
  for source in "json_each('[10,20]')" "json_tree('[10,20]')" "generate_series(0,1)"; do
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      sql="$setup ${line//\{f\}/$source}"
      sql=${sql//\{s\}/$tables}
      case "$source" in
        generate_series*)
          sql=${sql//key/value}
          sql=${sql//json_each(d.doc)/generate_series(0,1)}
          ;;
      esac
      printf '%s\n' "$sql" | tee "$statement" > /dev/null
      s=$(run "$stock")
      c=$(run "$candidate")
      b=$(run "$baseline")
      total=$((total + 1))
      if same "$s" "$c"; then candidate_row=match; else candidate_row=miss; fi
      if same "$s" "$b"; then baseline_row=match; else baseline_row=miss; fi
      case "$baseline_row/$candidate_row" in
        match/match) both=$((both + 1)); continue ;;
        miss/miss) neither=$((neither + 1)); tag=neither ;;
        miss/match) better=$((better + 1)); tag=better ;;
        match/miss) worse=$((worse + 1)); tag=WORSE ;;
      esac
      printf '%s [%s] %s\n' "$tag" "$source" "$line"
      printf '  stock:     %s\n  candidate: %s\n  baseline:  %s\n' \
        "$(printf '%s' "$s" | head -2 | tr '\n' '~')" \
        "$(printf '%s' "$c" | head -2 | tr '\n' '~')" \
        "$(printf '%s' "$b" | head -2 | tr '\n' '~')"
    done < "$shapes"
  done
  echo "$(basename "$shapes"): total=$total both=$both neither=$neither better=$better worse=$worse"
  worse_total=$((worse_total + worse))
done
[ "$worse_total" = 0 ]
