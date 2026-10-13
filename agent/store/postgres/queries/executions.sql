-- name: ExecutionCommitsFrom :many
SELECT seq, body FROM execution_commits WHERE key = $1 AND seq >= $2 ORDER BY seq;

-- name: ExecutionHead :one
SELECT COALESCE(MAX(seq), -1)::bigint AS last_seq FROM execution_commits WHERE key = $1;

-- name: ExecutionCommitSeq :one
SELECT seq FROM execution_commits WHERE key = $1 AND commit_id = $2;

-- name: ExecutionLease :one
SELECT owner, epoch, lease_until FROM execution_leases WHERE key = $1;

-- name: ExecutionExists :one
SELECT EXISTS(SELECT 1 FROM executions WHERE key = $1)::boolean AS present;

-- name: InsertExecution :exec
INSERT INTO executions (key, assignment_key) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING;

-- name: InsertExecutionCommit :exec
INSERT INTO execution_commits (key, seq, commit_id, body) VALUES ($1, $2, $3, $4);

-- name: UpsertExecutionLease :exec
INSERT INTO execution_leases (key, owner, epoch, lease_until) VALUES ($1, $2, $3, $4)
ON CONFLICT (key) DO UPDATE SET owner = EXCLUDED.owner, epoch = EXCLUDED.epoch, lease_until = EXCLUDED.lease_until;

-- name: InsertExecutionLease :exec
INSERT INTO execution_leases (key, owner, epoch, lease_until) VALUES ($1, $2, $3, $4);

-- name: RenewExecutionLease :exec
UPDATE execution_leases SET lease_until = $1 WHERE key = $2;

-- name: ExecutionsOwnedBy :many
SELECT e.assignment_key FROM execution_leases l JOIN executions e ON e.key = l.key WHERE l.owner = $1 ORDER BY l.key;

-- name: InsertExecutionSettlement :exec
INSERT INTO execution_settlement_feed (execution_key, assignment_key)
VALUES ($1, $2) ON CONFLICT (execution_key) DO NOTHING;

-- name: NotifyExecutionSettlement :exec
SELECT pg_notify('twilight_execution_settlements', '');

-- name: ExecutionSettlementHead :one
SELECT COALESCE(MAX(sequence), 0)::bigint AS head FROM execution_settlement_feed;

-- name: ExecutionSettlementsAfter :many
SELECT sequence, assignment_key
FROM execution_settlement_feed
WHERE sequence > $1
ORDER BY sequence
LIMIT $2;
