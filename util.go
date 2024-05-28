package anystore

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/syncpool"
)

func readRowsString(rows driver.Rows) (result []string, err error) {
	var dest = make([]driver.Value, 1)
	for {
		if err = rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			return nil, err
		}
		result = append(result, driverValueToString(dest[0]))
	}
}

func driverValueToString(v driver.Value) string {
	if v == nil {
		return ""
	}
	return v.(string)
}

func readIndexInfo(buf *syncpool.DocBuffer, rows driver.Rows) (result []IndexInfo, err error) {
	var dest = make([]driver.Value, 5)
	for {
		rErr := rows.Next(dest)
		if rErr != nil {
			if errors.Is(rErr, io.EOF) {
				break
			}
			return nil, err
		}

		fields, err := jsonToStringArray(buf.Parser, dest[1].(string))
		if err != nil {
			return nil, err
		}
		result = append(result, IndexInfo{
			Name:   dest[0].(string),
			Fields: fields,
			Sparse: dest[2].(int64) != 0,
			Unique: dest[3].(int64) != 0,
		})
	}
	return
}

func readOneInt(rows driver.Rows) (i int, err error) {
	var dest = make([]driver.Value, 1)
	if err = rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, sql.ErrNoRows
		} else {
			return 0, err
		}
	}
	return int(dest[0].(int64)), nil
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
