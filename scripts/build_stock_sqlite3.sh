#!/usr/bin/env bash
# Build the scorecard's stock incumbent: a sqlite3 shell from an official SQLite amalgamation, built the way Ubuntu
# builds /usr/bin/sqlite3 (bd-31oat):
# - a shared libsqlite3.so.0 (-fPIC, -Wl,-Bsymbolic-functions) with the system build's SQLITE_* compile options
#   (those of sqlite3 3.46.1-9ubuntu0.3, so `PRAGMA compile_options` lists the same entries, plus the new version's
#   own defaults);
# - the shell linked against it by rpath, with libz, and loading libreadline and libtinfo as the system shell does
#   (readline's headers are not needed: the scorecard's stdin is never a terminal, so no shell calls readline);
# - this host's Ubuntu build flags: -O2, frame pointers, LTO, stack protector, CET, _FORTIFY_SOURCE=3.
# Known difference from the system shell: SQLITE_ENABLE_UPDATE_DELETE_LIMIT is listed but the amalgamation's parser
# does not accept DELETE ... LIMIT (that needs a parser regenerated from the full source tree); no scorecard row uses it.
#
#   scripts/build_stock_sqlite3.sh <out-dir> [version-code sha3-256]
# Default version 3540100 (3.54.1, SHA3-256 published on sqlite.org/download.html). The zip is downloaded into
# <out-dir>/dl with an honest User-Agent and checked against the SHA3-256 before anything is built; then
# <out-dir>/sqlite3 and <out-dir>/libsqlite3.so.0 are built and <out-dir>/compile-options.txt is written.
# Refuses an <out-dir> that already holds a shell. Deletes nothing.
set -euo pipefail
OUT=${1:?out dir}
VERSION=${2:-3540100}
SHA3=${3:-92281c86e2957f9b9305890f9ac04204caa7b677cc868dc326d942689df28e30}
YEAR=${YEAR:-2026}
UA="frankensqlite-agent (+https://github.com/Dicklesworthstone/frankensqlite)"
[ -e "$OUT/sqlite3" ] && { echo "refuse: $OUT/sqlite3 exists" >&2; exit 3; }
mkdir -p "$OUT/dl"
ZIP=$OUT/dl/sqlite-amalgamation-$VERSION.zip
[ -f "$ZIP" ] || curl -fsS -A "$UA" -o "$ZIP" "https://sqlite.org/$YEAR/sqlite-amalgamation-$VERSION.zip"
GOT=$(python3 -I -c 'import hashlib,sys; print(hashlib.sha3_256(open(sys.argv[1],"rb").read()).hexdigest())' "$ZIP")
[ "$GOT" = "$SHA3" ] || { echo "SHA3-256 mismatch: got $GOT, expected $SHA3" >&2; exit 4; }
(cd "$OUT/dl" && unzip -q -o "$ZIP")
SRC=$OUT/dl/sqlite-amalgamation-$VERSION
CFLAGS="-g -O2 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer -flto=auto -ffat-lto-objects -fstack-protector-strong -fstack-clash-protection -fcf-protection"
CPPFLAGS="-D_FORTIFY_SOURCE=3"
LDFLAGS="-Wl,-Bsymbolic-functions -flto=auto -ffat-lto-objects -Wl,-z,relro"
DEFS="-DSQLITE_ALLOW_ROWID_IN_VIEW -DSQLITE_DEFAULT_AUTOVACUUM=0 -DSQLITE_DEFAULT_CACHE_SIZE=-2000 -DSQLITE_DEFAULT_FILE_FORMAT=4 -DSQLITE_DEFAULT_JOURNAL_SIZE_LIMIT=-1 -DSQLITE_DEFAULT_MMAP_SIZE=0 -DSQLITE_DEFAULT_PAGE_SIZE=4096 -DSQLITE_DEFAULT_PCACHE_INITSZ=20 -DSQLITE_DEFAULT_RECURSIVE_TRIGGERS=1 -DSQLITE_DEFAULT_SECTOR_SIZE=4096 -DSQLITE_DEFAULT_SYNCHRONOUS=2 -DSQLITE_DEFAULT_WAL_AUTOCHECKPOINT=1000 -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=2 -DSQLITE_DEFAULT_WORKER_THREADS=0 -DSQLITE_DIRECT_OVERFLOW_READ -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_ENABLE_DBPAGE_VTAB -DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_LOAD_EXTENSION -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_STMTVTAB -DSQLITE_ENABLE_UNLOCK_NOTIFY -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_HAVE_ISNAN -DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_MALLOC_SOFT_LIMIT=1024 -DSQLITE_MAX_ATTACHED=10 -DSQLITE_MAX_COLUMN=2000 -DSQLITE_MAX_COMPOUND_SELECT=500 -DSQLITE_MAX_DEFAULT_PAGE_SIZE=32768 -DSQLITE_MAX_EXPR_DEPTH=1000 -DSQLITE_MAX_FUNCTION_ARG=127 -DSQLITE_MAX_LENGTH=1000000000 -DSQLITE_MAX_LIKE_PATTERN_LENGTH=50000 -DSQLITE_MAX_MMAP_SIZE=0x7fff0000 -DSQLITE_MAX_PAGE_COUNT=0xfffffffe -DSQLITE_MAX_PAGE_SIZE=65536 -DSQLITE_MAX_SCHEMA_RETRY=25 -DSQLITE_MAX_SQL_LENGTH=1000000000 -DSQLITE_MAX_TRIGGER_DEPTH=1000 -DSQLITE_MAX_VARIABLE_NUMBER=250000 -DSQLITE_MAX_VDBE_OP=250000000 -DSQLITE_MAX_WORKER_THREADS=8 -DSQLITE_SECURE_DELETE -DSQLITE_SOUNDEX -DSQLITE_TEMP_STORE=1 -DSQLITE_THREADSAFE=1 -DSQLITE_USE_URI"
# shellcheck disable=SC2086
gcc $CFLAGS $CPPFLAGS $DEFS -fPIC -shared -Wl,-soname,libsqlite3.so.0 $LDFLAGS \
  -o "$OUT/libsqlite3.so.0" "$SRC/sqlite3.c" -lm -lpthread -ldl
# shellcheck disable=SC2086
gcc $CFLAGS $CPPFLAGS $DEFS -DSQLITE_HAVE_ZLIB=1 -I"$SRC" $LDFLAGS -o "$OUT/sqlite3" "$SRC/shell.c" \
  -L"$OUT" -l:libsqlite3.so.0 -Wl,-rpath,"$(cd "$OUT" && pwd)" -lz \
  -Wl,--no-as-needed -l:libreadline.so.8 -l:libtinfo.so.6 -Wl,--as-needed -lm
printf 'PRAGMA compile_options;\n' | "$OUT/sqlite3" :memory: > "$OUT/compile-options.txt"
"$OUT/sqlite3" --version
