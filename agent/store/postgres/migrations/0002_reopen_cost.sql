-- Reopen cost (APP-ACT, SES-REP-5): the CommitIndex columns beside each
-- commit, so Index reads no body; a durable projection cache, so a Session
-- reopened on another replica folds only the tail; the lease expiry index
-- the activation scan reads.
ALTER TABLE session_commits ADD COLUMN streams TEXT NOT NULL DEFAULT '';

CREATE TABLE projection_cache (
	session    TEXT   NOT NULL,
	projection TEXT   NOT NULL,
	version    BIGINT NOT NULL,
	state      TEXT   NOT NULL,
	through    BIGINT NOT NULL,
	PRIMARY KEY (session, projection, version)
);

CREATE INDEX session_roots_expiry ON session_roots (lease_until) WHERE owned;
