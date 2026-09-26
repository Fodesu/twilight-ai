-- Stream heads without the whole index (SES-REP-3): one row per commit per
-- stream, written with the commit, so StreamHead is a sum over one stream's
-- rows and Index derives its entries from these rows instead of a JSON
-- column. The streams column of session_commits is superseded.
CREATE TABLE session_commit_streams (
	segment   TEXT   NOT NULL,
	seq       BIGINT NOT NULL,
	domain    TEXT   NOT NULL,
	stream_id TEXT   NOT NULL,
	events    BIGINT NOT NULL,
	PRIMARY KEY (segment, seq, domain, stream_id)
);
CREATE INDEX session_commit_streams_by_stream ON session_commit_streams (segment, domain, stream_id, seq);

INSERT INTO session_commit_streams (segment, seq, domain, stream_id, events)
SELECT c.segment, c.seq, s.domain, s.stream_id, s.events
FROM session_commits c
CROSS JOIN LATERAL (
	SELECT (e->'stream'->>'domain') AS domain, COALESCE(e->'stream'->>'id', '') AS stream_id, (e->>'events')::bigint AS events
	FROM jsonb_array_elements(c.streams::jsonb) AS e
) AS s
WHERE c.streams <> '' AND c.streams <> 'null';

ALTER TABLE session_commits DROP COLUMN streams;
