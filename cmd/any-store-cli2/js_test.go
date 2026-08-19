package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Js(t *testing.T) {
	j, err := newJs()
	require.NoError(t, err)

	j.RegisterCollection("coll")

	var assertCmd = func(t *testing.T, line string, exp Cmd) {
		cmd, err := j.GetQuery(line)
		require.NoError(t, err)
		assert.Equal(t, exp, cmd)
	}

	t.Run("createCollection", func(t *testing.T) {
		assertCmd(t, `db.createCollection("collName")`, Cmd{
			Cmd:        "createCollection",
			Collection: "collName",
		})
	})
	t.Run("backup", func(t *testing.T) {
		assertCmd(t, `db.backup("backupPath")`, Cmd{
			Cmd:  "backup",
			Path: "backupPath",
		})
	})
	t.Run("count", func(t *testing.T) {
		assertCmd(t, `db.coll.count()`, Cmd{
			Cmd:        "count",
			Collection: "coll",
		})
	})
	t.Run("stats", func(t *testing.T) {
		assertCmd(t, `db.coll.stats()`, Cmd{
			Cmd:        "stats",
			Collection: "coll",
		})
	})
	t.Run("find", func(t *testing.T) {
		assertCmd(t, `db.coll.find()`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query: Query{
				Find: json.RawMessage(`{}`),
			},
		})
	})
	t.Run("find with limit offset sort", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).limit(1).offset(2).sort("a", "-b")`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Limit: 1, Offset: 2, Sort: []string{"a", "-b"}},
		})
	})
	t.Run("find count", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).count()`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Count: true},
		})
	})
	t.Run("find explain", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).explain()`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Explain: true},
		})
	})
	t.Run("find pretty", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).pretty()`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Pretty: true},
		})
	})
	t.Run("find project", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).project({a:1})`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Project: json.RawMessage(`{"a":1}`)},
		})
	})
	t.Run("find update", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).update({b:"c"})`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Update: json.RawMessage(`{"b":"c"}`)},
		})
	})
	t.Run("find delete", func(t *testing.T) {
		assertCmd(t, `db.coll.find({a:"b"}).delete()`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`), Delete: true},
		})
	})
	t.Run("ensure index", func(t *testing.T) {
		assertCmd(t, `db.coll.ensureIndex({fields:["a"]})`, Cmd{
			Cmd:        "ensureIndex",
			Collection: "coll",
			Index:      Index{Fields: []string{"a"}},
		})
	})
	t.Run("ensure fulltext index", func(t *testing.T) {
		assertCmd(t, `db.coll.ensureIndex({name:"fts", kind:"fulltext", fields:["title","body"]})`, Cmd{
			Cmd:        "ensureIndex",
			Collection: "coll",
			Index:      Index{Name: "fts", Kind: "fulltext", Fields: []string{"title", "body"}},
		})
	})
	t.Run("aggregate", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate([{$match:{a:"b"}},{$group:{_id:"$a"}}])`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline: json.RawMessage(`[{"$match":{"a":"b"}},{"$group":{"_id":"$a"}}]`),
			},
		})
	})
	t.Run("aggregate variadic stages", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate({$match:{a:"b"}}, {$count:"n"})`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline: json.RawMessage(`[{"$match":{"a":"b"}},{"$count":"n"}]`),
			},
		})
	})
	t.Run("aggregate empty", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate()`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline: json.RawMessage(`[]`),
			},
		})
	})
	t.Run("aggregate options", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate([{$group:{_id:"$a"}}]).groupLimit(100).accumArrayLimit(-1).memoryLimit(1024).pretty()`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline:        json.RawMessage(`[{"$group":{"_id":"$a"}}]`),
				GroupLimit:      100,
				AccumArrayLimit: -1,
				MemoryLimit:     1024,
				Pretty:          true,
			},
		})
	})
	t.Run("aggregate count", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate([{$match:{a:"b"}}]).count()`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline: json.RawMessage(`[{"$match":{"a":"b"}}]`),
				Count:    true,
			},
		})
	})
	t.Run("aggregate explain", func(t *testing.T) {
		assertCmd(t, `db.coll.aggregate([{$match:{a:"b"}}]).explain()`, Cmd{
			Cmd:        "aggregate",
			Collection: "coll",
			Query: Query{
				Pipeline: json.RawMessage(`[{"$match":{"a":"b"}}]`),
				Explain:  true,
			},
		})
	})
	t.Run("find after aggregate resets cmd", func(t *testing.T) {
		_, err := j.GetQuery(`db.coll.aggregate([{$match:{a:"b"}}])`)
		require.NoError(t, err)
		assertCmd(t, `db.coll.find({a:"b"})`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"a":"b"}`)},
		})
	})
	t.Run("drop index", func(t *testing.T) {
		assertCmd(t, `db.coll.dropIndex("indexName")`, Cmd{
			Cmd:        "dropIndex",
			Collection: "coll",
			Index:      Index{Name: "indexName"},
		})
	})
	t.Run("drop", func(t *testing.T) {
		assertCmd(t, `db.coll.drop()`, Cmd{
			Cmd:        "drop",
			Collection: "coll",
		})
	})
	t.Run("insert", func(t *testing.T) {
		assertCmd(t, `db.coll.insert({a:"b"}, {"c":"d"})`, Cmd{
			Cmd:        "insert",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`{"a":"b"}`), json.RawMessage(`{"c":"d"}`)},
		})
	})
	t.Run("upsert", func(t *testing.T) {
		assertCmd(t, `db.coll.upsert({a:"b"}, {"c":"d"})`, Cmd{
			Cmd:        "upsert",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`{"a":"b"}`), json.RawMessage(`{"c":"d"}`)},
		})
	})
	t.Run("update", func(t *testing.T) {
		assertCmd(t, `db.coll.update({a:"b"}, {"c":"d"})`, Cmd{
			Cmd:        "update",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`{"a":"b"}`), json.RawMessage(`{"c":"d"}`)},
		})
	})
	t.Run("deleteId", func(t *testing.T) {
		assertCmd(t, `db.coll.deleteId(1, 2)`, Cmd{
			Cmd:        "deleteId",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)},
		})
	})
	t.Run("findId", func(t *testing.T) {
		assertCmd(t, `db.coll.findId(1, 2)`, Cmd{
			Cmd:        "findId",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`2`)},
		})
	})
	t.Run("findId pretty", func(t *testing.T) {
		assertCmd(t, `db.coll.findId(1).pretty()`, Cmd{
			Cmd:        "findId",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`1`)},
			Query:      Query{Pretty: true},
		})
	})
	t.Run("quickCheck", func(t *testing.T) {
		assertCmd(t, `db.quickCheck()`, Cmd{
			Cmd: "quickCheck",
		})
	})
	t.Run("getIndexes", func(t *testing.T) {
		assertCmd(t, `db.coll.getIndexes()`, Cmd{
			Cmd:        "getIndexes",
			Collection: "coll",
		})
	})
	t.Run("rename", func(t *testing.T) {
		assertCmd(t, `db.coll.rename("newName")`, Cmd{
			Cmd:        "rename",
			Collection: "coll",
			Path:       "newName",
		})
	})
	t.Run("updateId", func(t *testing.T) {
		assertCmd(t, `db.coll.updateId(1, {$set:{a:1}})`, Cmd{
			Cmd:        "updateId",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`1`), json.RawMessage(`{"$set":{"a":1}}`)},
		})
	})
	t.Run("it", func(t *testing.T) {
		assertCmd(t, `it`, Cmd{
			Cmd: "it",
		})
	})
	t.Run("insert with ObjectId/BinData/Vector constructors", func(t *testing.T) {
		// One single-key document per constructor: otto's JSON.stringify reorders
		// multi-key objects, so keep each wrapper on its own document to assert
		// exact bytes. (Field order is not semantically meaningful anyway.)
		assertCmd(t, `db.coll.insert({id: ObjectId("0123456789abcdef01234567")}, {b: BinData("AQID")}, {e: Vector([1,2,3])})`, Cmd{
			Cmd:        "insert",
			Collection: "coll",
			Documents: []json.RawMessage{
				json.RawMessage(`{"id":{"$oid":"0123456789abcdef01234567"}}`),
				json.RawMessage(`{"b":{"$binary":"AQID"}}`),
				json.RawMessage(`{"e":{"$vector":[1,2,3]}}`),
			},
		})
	})
	t.Run("find with Knn constructor", func(t *testing.T) {
		assertCmd(t, `db.coll.find({emb: Knn([1,2], 10, {ef: 200})})`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			// Key order is otto's JSON.stringify ordering; not semantically meaningful.
			Query:      Query{Find: json.RawMessage(`{"emb":{"$knn":{"$ef":200,"$k":10,"$query":[1,2]}}}`)},
		})
	})
	t.Run("findId with ObjectId constructor", func(t *testing.T) {
		assertCmd(t, `db.coll.findId(ObjectId("0123456789abcdef01234567"))`, Cmd{
			Cmd:        "findId",
			Collection: "coll",
			Documents:  []json.RawMessage{json.RawMessage(`{"$oid":"0123456789abcdef01234567"}`)},
		})
	})
	t.Run("find by ObjectId field", func(t *testing.T) {
		assertCmd(t, `db.coll.find({id: ObjectId("0123456789abcdef01234567")})`, Cmd{
			Cmd:        "find",
			Collection: "coll",
			Query:      Query{Find: json.RawMessage(`{"id":{"$oid":"0123456789abcdef01234567"}}`)},
		})
	})
}
