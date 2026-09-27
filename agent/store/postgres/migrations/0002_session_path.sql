-- 0002_session_path: a root stores the path of spans it reads, and each
-- segment stores the right endpoints of the live sessions that cover it.
-- Covers name a segment that must exist; they do not name the root, because
-- a fork writes them before the root row is inserted.

ALTER TABLE session_roots ADD COLUMN path TEXT NOT NULL DEFAULT '[]';

CREATE TABLE session_covers (
	segment TEXT NOT NULL REFERENCES session_segments (id),
	session TEXT NOT NULL,
	open    BOOLEAN NOT NULL,
	through BIGINT NOT NULL,
	PRIMARY KEY (segment, session)
);
