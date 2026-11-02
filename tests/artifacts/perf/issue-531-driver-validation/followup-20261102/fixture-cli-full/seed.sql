PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL UNIQUE);
CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE likes (user_id INTEGER NOT NULL, post_id INTEGER NOT NULL, PRIMARY KEY (user_id, post_id));
CREATE INDEX posts_created_at_id_idx ON posts (created_at DESC, id DESC);
CREATE INDEX likes_post_id_idx ON likes (post_id);
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 50000)
INSERT INTO users SELECT i, 'user' || i FROM n;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 500000)
INSERT INTO posts SELECT i, i * 48271 % 50000 + 1, 'post body ' || i, printf('2025-%02d-%02dT%02d:%02d:%02d.%03dZ', i % 12 + 1, i % 28 + 1, i % 24, i % 60, i / 7 % 60, i % 1000) FROM n;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 2000000)
INSERT OR IGNORE INTO likes SELECT i * 48271 % 50000 + 1, i * 16807 % 500000 + 1 FROM n;
ANALYZE;
VACUUM;
PRAGMA journal_mode=WAL;
