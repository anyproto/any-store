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
	t.Run("count", func(t *testing.T) {
		assertCmd(t, `db.coll.count()`, Cmd{
			Cmd:        "count",
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
}
