package anystore

// LatestSchemaFormat is the on-disk format a database created by this
// release gets. It says what this build creates, not that an older file must
// change: a file keeps the format it was created with, and this release
// reads and writes every format it supports. CopyToLatestFormat copies a
// database into this format.
const LatestSchemaFormat = 6

// SchemaFormat returns the on-disk format of d's file: 5 for a file created
// before v2.5.0, 6 for one created since. A file a release before v2.5.0
// created whose header records no format reads as 5. The format of a file
// does not change while it is open or across reopens; a result below
// LatestSchemaFormat is a file CopyToLatestFormat can bring to it.
//
// A DB this package did not open returns ErrUnsupportedDB.
func SchemaFormat(d DB) (int, error) {
	dbi, ok := d.(*db)
	if !ok || dbi == nil {
		return 0, ErrUnsupportedDB
	}
	format := dbi.btreeDB.SchemaFormat()
	if format == 0 {
		// A header that never recorded a format: the btree reads it as 5.
		format = 5
	}
	return int(format), nil
}
