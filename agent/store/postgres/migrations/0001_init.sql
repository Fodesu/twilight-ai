-- 0001_init: every shared store of the reference agent in one schema.
-- Bodies are JSON as TEXT, so the wire form of each ledger is the one the
-- kernel writes; sequences and epochs are BIGINT holding the kernel's uint64.

CREATE TABLE executions (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL
);
CREATE TABLE execution_commits (
	key       TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);
CREATE TABLE execution_leases (
	key         TEXT PRIMARY KEY,
	owner       TEXT NOT NULL,
	epoch       BIGINT NOT NULL,
	lease_until BIGINT NOT NULL
);
CREATE INDEX execution_leases_by_owner ON execution_leases (owner, key);

CREATE TABLE processes (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL,
	epoch          BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE process_commits (
	key       TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);

CREATE TABLE checkpoints (
	consumer TEXT NOT NULL,
	ledger   TEXT NOT NULL,
	next     BIGINT NOT NULL,
	PRIMARY KEY (consumer, ledger)
);

CREATE TABLE bindings (
	id      TEXT PRIMARY KEY,
	digest  TEXT NOT NULL,
	binding TEXT NOT NULL
);
CREATE TABLE claims (
	id              TEXT PRIMARY KEY,
	owner_kind      TEXT NOT NULL,
	owner_authority TEXT NOT NULL,
	owner_identity  TEXT NOT NULL,
	state           TEXT NOT NULL,
	claim           TEXT NOT NULL
);
CREATE INDEX claims_by_owner ON claims (owner_kind, owner_authority, id);

CREATE TABLE inbox (
	session     TEXT NOT NULL,
	seq         BIGINT NOT NULL,
	command_id  TEXT NOT NULL,
	kind        TEXT NOT NULL,
	payload     TEXT NOT NULL DEFAULT '',
	enqueued_at BIGINT NOT NULL,
	status      TEXT NOT NULL DEFAULT '',
	reason      TEXT NOT NULL DEFAULT '',
	resolved_at BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (session, seq),
	UNIQUE (session, command_id)
);
CREATE INDEX inbox_pending ON inbox (status, session, seq);

CREATE TABLE workspaces (
	id     TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
CREATE TABLE workspace_snapshots (
	ref       TEXT PRIMARY KEY,
	workspace TEXT NOT NULL,
	record    TEXT NOT NULL
);

CREATE TABLE session_segments (
	id     TEXT PRIMARY KEY,
	header TEXT NOT NULL
);
CREATE TABLE session_commits (
	segment   TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (segment, seq),
	UNIQUE (segment, commit_id)
);
CREATE TABLE session_roots (
	id          TEXT PRIMARY KEY,
	tip         TEXT NOT NULL,
	created_at  BIGINT NOT NULL,
	epoch       BIGINT NOT NULL DEFAULT 0,
	owned       BOOLEAN NOT NULL DEFAULT FALSE,
	owner       TEXT NOT NULL DEFAULT '',
	lease_until BIGINT NOT NULL DEFAULT 0,
	failed      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX session_roots_owned ON session_roots (owned);

CREATE TABLE content (
	authority  TEXT NOT NULL,
	key        TEXT NOT NULL,
	data       BYTEA NOT NULL,
	media_type TEXT NOT NULL,
	durability TEXT NOT NULL,
	PRIMARY KEY (authority, key)
);
