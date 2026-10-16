package session

import (
	"context"
	"errors"
	"sort"
)

// Directory is the ownership-free enumeration capability used to rebuild
// application work indexes. A page is ordered by immutable SessionID, after
// is exclusive, and concurrent creations before after are found on a later
// sweep. Enumeration never grants write authority.
type Directory interface {
	ListSessions(ctx context.Context, after SessionID, limit int) (SessionPage, error)
}

type SessionPage struct {
	Items []SessionRecord
	Next  SessionID
	More  bool
}

// ListSessions supplies the portable directory over the storage maintenance
// port. Database adapters may implement the same read using an indexed page.
func (l *Ledger) ListSessions(ctx context.Context, after SessionID, limit int) (SessionPage, error) {
	if err := ctx.Err(); err != nil {
		return SessionPage{}, err
	}
	if limit <= 0 {
		return SessionPage{}, errors.New("session: directory page size must be positive")
	}
	records, err := l.st.ListRecords(ctx)
	if err != nil {
		return SessionPage{}, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	page := SessionPage{}
	for _, record := range records {
		if record.ID <= after {
			continue
		}
		if len(page.Items) == limit {
			page.More = true
			break
		}
		page.Items = append(page.Items, record)
		page.Next = record.ID
	}
	return page, nil
}

var _ Directory = (*Ledger)(nil)
