package driver

import (
	"fmt"
	"math"
	"unsafe"

	"modernc.org/libc"
	"modernc.org/libc/sys/types"
	sqlite3 "modernc.org/sqlite/lib"
)

func sqlitePreallocatePageCache(tls *libc.TLS, pageCacheSize int) error {
	if pageCacheSize > math.MaxInt32 {
		return fmt.Errorf("sqlite: page cache size is too large")
	}
	p := libc.Xmalloc(tls, types.Size_t(pageCacheSize))
	if p == 0 {
		return fmt.Errorf("sqlite: cannot allocate memory")
	}

	headerSizeMem := libc.Xmalloc(tls, 4)
	if headerSizeMem == 0 {
		return fmt.Errorf("sqlite: cannot allocate memory for header size")
	}
	defer libc.Xfree(tls, headerSizeMem)

	*(*int32)(unsafe.Pointer(headerSizeMem)) = 0

	// Create a va_list containing the pointer to headerSize.
	// Unlike SQLITE_CONFIG_SMALL_MALLOC (which takes an int value),
	// SQLITE_CONFIG_PCACHE_HDRSZ expects a pointer to an int.
	args := libc.NewVaList(headerSizeMem)
	if args == 0 {
		return fmt.Errorf("sqlite: get page cache header size: cannot allocate memory")
	}
	defer libc.Xfree(tls, args)

	// Call sqlite3_config with SQLITE_CONFIG_PCACHE_HDRSZ.
	rc := sqlite3.Xsqlite3_config(
		tls,
		sqlite3.SQLITE_CONFIG_PCACHE_HDRSZ,
		args,
	)
	if rc != sqlite3.SQLITE_OK {
		p := sqlite3.Xsqlite3_errstr(tls, rc)
		str := libc.GoString(p)
		return fmt.Errorf("sqlite: failed to get SQLITE_CONFIG_PCACHE_HDRSZ: %v", str)
	}

	headerSize := *(*int32)(unsafe.Pointer(headerSizeMem))
	var sqlitePageSize int32 = sqlite3.SQLITE_DEFAULT_PAGE_SIZE // or your chosen SQLite page size
	var sz = sqlitePageSize + headerSize                        // 4104 bytes
	var n int32 = int32(pageCacheSize) / sz                     // number of cache lines

	list := libc.NewVaList(p, sz, n)
	rc = sqlite3.Xsqlite3_config(
		tls,
		sqlite3.SQLITE_CONFIG_PAGECACHE,
		list,
	)
	if rc != sqlite3.SQLITE_OK {
		p := sqlite3.Xsqlite3_errstr(tls, rc)
		str := libc.GoString(p)
		return fmt.Errorf("sqlite: failed to configure SQLITE_CONFIG_PAGECACHE: %v", str)
	}
	return nil
}
