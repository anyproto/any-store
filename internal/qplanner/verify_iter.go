package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/internal/btree"
)

// VerifyIter wraps an index-yielding iterator and verifies each candidate docId
// against a secondary index. For each docId from the source, it builds a
// verification key (prefix + docId) and checks if it exists in the verify namespace.
// This avoids document fetches for count queries where the chosen index doesn't
// cover all filter fields but single-field indexes exist for uncovered fields.
type VerifyIter struct {
	Source   Iterator
	Tx       *btree.ReadTx
	VerifyNs *btree.Namespace
	Prefix   []byte // encoded value prefix for the verification index
	keyBuf   []byte // reusable buffer for verification key construction
}

func (it *VerifyIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}
		// Build verification key: prefix + docId
		it.keyBuf = append(it.keyBuf[:0], it.Prefix...)
		it.keyBuf = append(it.keyBuf, docId...)
		_, gerr := it.Tx.Get(it.VerifyNs, it.keyBuf)
		if gerr == nil {
			return key, docId, nil // verified!
		}
		// Not found in verification index → skip this candidate
	}
}

func (it *VerifyIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *VerifyIter) String() string {
	return fmt.Sprintf("%s -> Verify", it.Source)
}
