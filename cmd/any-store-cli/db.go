package main

import (
	"encoding/json"
	"fmt"
	"strings"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
)

func openConn(path string) (err error) {
	db, err := anystore.Open(mainCtx.Ctx(), path, nil)
	if err != nil {
		return
	}
	j, err := newJs()
	if err != nil {
		return err
	}
	conn = &Conn{db: db, js: j}
	if err = conn.makeAutocomplete(); err != nil {
		_ = db.Close()
		return
	}
	return
}

var conn *Conn

type Conn struct {
	db                anystore.DB
	js                *js
	autocomplete      []string
	autocompleteDb    []string
	autocompleteColl  []string
	autocompleteQuery []string
}

func (c *Conn) makeAutocomplete() (err error) {
	c.autocomplete = append(c.autocomplete[:0], "show collections", "show stats", "db.")
	c.autocompleteDb = append(c.autocompleteDb[:0], "db.createCollection(")
	c.autocompleteQuery = append(c.autocompleteQuery[:0], "limit(", "offset(", "sort(", "hint(", "pretty()", "count()", "explain()", "delete()", "update(")
	collNames, err := c.db.GetCollectionNames(mainCtx.Ctx())
	if err != nil {
		return
	}
	c.autocompleteColl = c.autocompleteColl[:0]
	for _, collName := range collNames {
		c.autocompleteDb = append(c.autocompleteDb, "db."+collName+".")
		for _, cmd := range []string{"insert", "find", "findOne", "findId", "deleteId", "update", "upsert", "ensureIndex", "dropIndex", "drop", "count"} {
			c.autocompleteColl = append(c.autocompleteColl, "db."+collName+"."+cmd+"(")
		}
		c.js.RegisterCollection(collName)
	}
	return nil
}

func (c *Conn) Exec(cmdLine string) (result string, err error) {
	var cmd Cmd
	if strings.HasPrefix(cmdLine, "db.") {
		if cmd, err = c.js.GetQuery(cmdLine); err != nil {
			return
		}
	} else {
		cmd.Cmd = cmdLine
	}
	return c.ExecCmd(cmd)
}

func (c *Conn) ExecCmd(cmd Cmd) (result string, err error) {
	switch cmd.Cmd {
	case "show collections":
		return c.ShowCollections()
	case "show stats":
		return c.ShowStats()
	case "createCollection":
		return c.CreateCollection(cmd)
	case "insert":
		return c.Insert(cmd)
	case "update":
		return c.Update(cmd)
	case "upsert":
		return c.Upsert(cmd)
	case "count":
		return c.Count(cmd)
	case "find":
		return c.Find(cmd)
	case "findOne":
		return c.FindOne(cmd)
	case "findId":
		return c.FindId(cmd)
	case "deleteId":
		return c.DeleteId(cmd)
	case "ensureIndex":
		return c.EnsureIndex(cmd)
	case "dropIndex":
		return c.DropIndex(cmd)
	case "drop":
		return c.Drop(cmd)
	}
	return "", fmt.Errorf("unexpected command: %s", cmd.Cmd)
}

func (c *Conn) Complete(line string) (result []string) {
	lowerLine := strings.ToLower(line)
	if !strings.HasPrefix(lowerLine, "db.") {
		for _, cmd := range c.autocomplete {
			if strings.HasPrefix(cmd, lowerLine) {
				result = append(result, cmd)
			}
		}
		return
	}

	dotCount := strings.Count(lowerLine, ".")
	if dotCount == 1 {
		for _, cmd := range c.autocompleteDb {
			if strings.HasPrefix(cmd, lowerLine) {
				result = append(result, cmd)
			}
		}
		return
	}

	// find the last dot or parenthesis
	lastDot := strings.LastIndex(line, ".")
	lastParen := strings.LastIndex(line, ")")

	if lastDot > lastParen {
		// we are after a dot, suggest methods
		prefix := line[:lastDot+1]
		toComplete := strings.ToLower(line[lastDot+1:])

		// check if it's db.collection. or something else
		if strings.Count(line, ".") == 2 && !strings.Contains(line, "(") {
			for _, cmd := range c.autocompleteColl {
				if strings.HasPrefix(strings.ToLower(cmd), lowerLine) {
					result = append(result, cmd)
				}
			}
		} else {
			// suggest query methods
			for _, cmd := range c.autocompleteQuery {
				if strings.HasPrefix(cmd, toComplete) {
					result = append(result, prefix+cmd)
				}
			}
		}
	} else {
		// we might be in the middle of a command or at the start
		for _, cmd := range c.autocompleteColl {
			if strings.HasPrefix(strings.ToLower(cmd), lowerLine) {
				result = append(result, cmd)
			}
		}
	}

	return
}

func (c *Conn) ShowCollections() (result string, err error) {
	names, err := c.db.GetCollectionNames(mainCtx.Ctx())
	if err != nil {
		return "", err
	}
	return strings.Join(names, "\n"), nil
}

func (c *Conn) ShowStats() (result string, err error) {
	stats, err := c.db.Stats(mainCtx.Ctx())
	if err != nil {
		return "", err
	}
	var buf = &strings.Builder{}
	buf.WriteString(fmt.Sprintf("Collections:\t%d\n", stats.CollectionsCount))
	buf.WriteString(fmt.Sprintf("Indexes:\t%d\n", stats.IndexesCount))
	buf.WriteString(fmt.Sprintf("Data size:\t%d KiB\n", stats.DataSizeBytes/1024))
	buf.WriteString(fmt.Sprintf("Total size:\t%d KiB\n", stats.DataSizeBytes/1024))
	return buf.String(), nil
}

func (c *Conn) CreateCollection(cmd Cmd) (result string, err error) {
	_, err = c.db.CreateCollection(mainCtx.Ctx(), cmd.Collection)
	if err == nil {
		_ = c.makeAutocomplete()
	}
	return
}

func (c *Conn) Insert(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	var docs = make([]*anyenc.Value, len(cmd.Documents))
	for i, d := range cmd.Documents {
		if docs[i], err = anyenc.ParseJson(string(d)); err != nil {
			return
		}
	}
	if err = coll.Insert(mainCtx.Ctx(), docs...); err != nil {
		return
	}
	result = fmt.Sprintf("inserted %d documents", len(cmd.Documents))
	return
}

func (c *Conn) Upsert(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) == 0 {
		return "", fmt.Errorf(`expected document`)
	}
	var doc *anyenc.Value
	if doc, err = anyenc.ParseJson(string(cmd.Documents[0])); err != nil {
		return
	}
	if err = coll.UpsertOne(mainCtx.Ctx(), doc); err != nil {
		return
	}
	result = fmt.Sprintf("upserted")
	return
}

func (c *Conn) Update(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) == 0 {
		return "", fmt.Errorf(`expected document`)
	}
	var doc *anyenc.Value
	if doc, err = anyenc.ParseJson(string(cmd.Documents[0])); err != nil {
		return
	}
	if err = coll.UpdateOne(mainCtx.Ctx(), doc); err != nil {
		return
	}
	return
}

func (c *Conn) Count(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	count, err := coll.Count(mainCtx.Ctx())
	if err != nil {
		return
	}
	result = fmt.Sprintf("%d", count)
	return
}

func (c *Conn) EnsureIndex(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Index.Fields) == 0 {
		return "", fmt.Errorf("no index fields specified")
	}
	indexInfo := anystore.IndexInfo(cmd.Index)
	err = coll.EnsureIndex(mainCtx.Ctx(), indexInfo)
	if err != nil {
		return
	}
	return
}

func (c *Conn) DropIndex(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Index.Name) == 0 {
		return "", fmt.Errorf("no index name specified")
	}
	err = coll.DropIndex(mainCtx.Ctx(), cmd.Index.Name)
	if err != nil {
		return
	}
	return
}

func (c *Conn) FindId(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) != 1 {
		return "", fmt.Errorf("you can specify only one id; got %d", len(cmd.Documents))
	}
	var id any
	if err = json.Unmarshal(cmd.Documents[0], &id); err != nil {
		return
	}
	doc, err := coll.FindId(mainCtx.Ctx(), id)
	if err != nil {
		return
	}
	if cmd.Query.Pretty {
		result, err = prettyJson(doc.Value().String())
	} else {
		result = doc.Value().String()
	}
	return
}

func (c *Conn) DeleteId(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) != 1 {
		return "", fmt.Errorf("you can specify only one id; got %d", len(cmd.Documents))
	}
	var id any
	if err = json.Unmarshal(cmd.Documents[0], &id); err != nil {
		return
	}
	err = coll.DeleteId(mainCtx.Ctx(), id)
	if err != nil {
		return
	}
	return
}

func (c *Conn) Drop(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if err = coll.Drop(mainCtx.Ctx()); err != nil {
		return "", err
	}
	return
}

func (c *Conn) FindOne(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	q := coll.Find(cmd.Query.Find)
	q.Limit(1)

	iter, err := q.Iter(mainCtx.Ctx())
	if err != nil {
		return "", err
	}
	defer iter.Close()
	if iter.Next() {
		var doc anystore.Doc
		if doc, err = iter.Doc(); err != nil {
			return "", err
		}
		res, err := prettyJson(doc.Value().String())
		if err != nil {
			return "", err
		}
		fmt.Println(res)
	}
	err = iter.Err()
	return
}

func (c *Conn) Find(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	q := coll.Find(cmd.Query.Find)

	if cmd.Query.Sort != nil {
		q.Sort(toAnySlice(cmd.Query.Sort)...)
	}
	if cmd.Query.Limit > 0 {
		q.Limit(uint(cmd.Query.Limit))
	} else {
		q.Limit(50)
	}
	if cmd.Query.Offset > 0 {
		q.Offset(uint(cmd.Query.Offset))
	}
	if cmd.Query.Hint != nil {
		hints := []anystore.IndexHint{}
		for idxName, boost := range cmd.Query.Hint {
			hints = append(hints, anystore.IndexHint{
				IndexName: idxName,
				Boost:     boost,
			})
		}
		q.IndexHint(hints...)
	}

	if cmd.Query.Count {
		count, cErr := q.Count(mainCtx.Ctx())
		if cErr != nil {
			return "", cErr
		}
		result = fmt.Sprintf("%d", count)
		return
	}

	if cmd.Query.Explain {
		explain, cErr := q.Explain(mainCtx.Ctx())
		if cErr != nil {
			return "", cErr
		}
		result = fmt.Sprintf("Query:\n%s\n\nExplain:\n%s\n\n", explain.Sql, strings.Join(explain.SqliteExplain, "\n"))
		for _, idx := range explain.Indexes {
			result += fmt.Sprintf("index:\t%s; weight\t%d; used:\t%v\n", idx.Name, idx.Weight, idx.Used)
		}
		return
	}

	if cmd.Query.Update != nil {
		res, cErr := q.Update(mainCtx.Ctx(), cmd.Query.Update)
		if cErr != nil {
			return "", cErr
		}
		fmt.Printf("Matched:\t%d\nModified:\t%d\n", res.Matched, res.Modified)
		return
	}

	if cmd.Query.Delete {
		res, cErr := q.Delete(mainCtx.Ctx())
		if cErr != nil {
			return "", cErr
		}
		fmt.Printf("Deleted:\t%d\n", res.Modified)
		return
	}

	iter, err := q.Iter(mainCtx.Ctx())
	if err != nil {
		return "", err
	}
	defer iter.Close()
	var doc anystore.Doc
	for iter.Next() {
		if doc, err = iter.Doc(); err != nil {
			return "", err
		}
		if cmd.Query.Pretty {
			res, err := prettyJson(doc.Value().String())
			if err != nil {
				return "", err
			}
			fmt.Println(res)
		} else {
			fmt.Println(doc.Value().String())
		}
	}
	err = iter.Err()
	return
}

func prettyJson(s string) (string, error) {
	var anyVal any
	if err := json.Unmarshal([]byte(s), &anyVal); err != nil {
		return "", err
	}
	res, err := json.MarshalIndent(anyVal, "", "  ")
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func toAnySlice[T any](slice []T) []any {
	res := make([]any, len(slice))
	for i, v := range slice {
		res[i] = v
	}
	return res
}
