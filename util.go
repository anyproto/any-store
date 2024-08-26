package anystore

import (
	"slices"

	"github.com/valyala/fastjson"
	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/syncpool"
)

func readIndexInfo(buf *syncpool.DocBuffer, stmt *sqlite.Stmt) (result []IndexInfo, err error) {
	for {
		hasRow, stepErr := stmt.Step()
		if !hasRow {
			return
		}
		if stepErr != nil {
			return nil, stepErr
		}
		fields, err := jsonToStringArray(buf.Parser, stmt.ColumnText(1))
		if err != nil {
			return nil, err
		}
		result = append(result, IndexInfo{
			Name:   stmt.ColumnText(0),
			Fields: fields,
			Sparse: stmt.ColumnInt(2) != 0,
			Unique: stmt.ColumnInt(2) != 0,
		})
	}
}

func readBytes(stmt *sqlite.Stmt, buf []byte) []byte {
	l := stmt.ColumnLen(0)
	buf = slices.Grow(buf, l)[:l]
	stmt.ColumnBytes(0, buf)
	return buf
}

func stringArrayToJson(a *fastjson.Arena, array []string) string {
	jArr := a.NewArray()
	for i, s := range array {
		jArr.SetArrayItem(i, a.NewString(s))
	}
	return jArr.String()
}

func jsonToStringArray(p *fastjson.Parser, j string) ([]string, error) {
	jVal, err := p.Parse(j)
	if err != nil {
		return nil, err
	}
	jVals, err := jVal.Array()
	if err != nil {
		return nil, err
	}
	result := make([]string, len(jVals))
	for i, jArrV := range jVals {
		result[i] = string(jArrV.GetStringBytes())
	}
	return result, nil
}

func copyItem(buf *syncpool.DocBuffer, it item) item {
	res, _ := buf.Parser.ParseBytes(it.val.MarshalTo(buf.DocBuf[:0]))
	return item{val: res}
}
