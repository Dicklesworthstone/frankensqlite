SELECT (SELECT count(*) FROM users),(SELECT count(*) FROM posts),(SELECT count(*) FROM likes);
PRAGMA integrity_check;
