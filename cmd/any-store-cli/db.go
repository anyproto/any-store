package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	_ "net/http/pprof"
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

func init() {
	go http.ListenAndServe(":6060", nil)
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
	c.autocomplete = append(c.autocomplete[:0], "show collections", "show stats", "db.", "help")
	c.autocompleteDb = append(c.autocompleteDb[:0], "db.createCollection(", "db.backup(", "db.quickCheck()")
	c.autocompleteQuery = append(c.autocompleteQuery[:0], "limit(", "offset(", "sort(", "hint(", "project(", "pretty()", "count()", "explain()", "delete()", "update(")
	collNames, err := c.db.GetCollectionNames(mainCtx.Ctx())
	if err != nil {
		return
	}
	c.autocompleteColl = c.autocompleteColl[:0]
	for _, collName := range collNames {
		c.autocompleteDb = append(c.autocompleteDb, "db."+collName+".")
		for _, cmd := range []string{"insert", "find", "findOne", "findId", "deleteId", "update", "updateId", "upsert", "upsertId", "ensureIndex", "dropIndex", "getIndexes", "rename", "drop", "count"} {
			c.autocompleteColl = append(c.autocompleteColl, "db."+collName+"."+cmd+"(")
		}
		c.js.RegisterCollection(collName)
	}
	return nil
}

func (c *Conn) Exec(cmdLine string) (result string, err error) {
	var cmd Cmd
	if strings.HasPrefix(cmdLine, "help") {
		cmd.Cmd = "help"
		cmd.Path = strings.TrimSpace(strings.TrimPrefix(cmdLine, "help"))
		return c.ExecCmd(cmd)
	}
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
	case "quickCheck":
		return c.QuickCheck()
	case "createCollection":
		return c.CreateCollection(cmd)
	case "backup":
		return c.Backup(cmd)
	case "rename":
		return c.Rename(cmd)
	case "insert":
		return c.Insert(cmd)
	case "update":
		return c.Update(cmd)
	case "updateId":
		return c.UpdateId(cmd)
	case "upsert":
		return c.Upsert(cmd)
	case "upsertId":
		return c.UpsertId(cmd)
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
	case "getIndexes":
		return c.GetIndexes(cmd)
	case "drop":
		return c.Drop(cmd)
	case "help":
		return c.Help(cmd)
	}
	return "", fmt.Errorf("unexpected command: %s", cmd.Cmd)
}

var helpData = map[string]string{
	"show collections": "Description: Show all collections in the database\nExample: show collections",
	"show stats":       "Description: Show database statistics\nExample: show stats",
	"quickCheck":       "Description: Perform a quick check of the database integrity\nExample: db.quickCheck()",
	"createCollection": "Description: Create a new collection\nExample: db.createCollection(\"myCollection\")",
	"backup":           "Description: Backup the database to a file\nExample: db.backup(\"backup.db\")",
	"rename":           "Description: Rename the collection\nExample: db.collection.rename(\"newName\")",
	"insert":           "Description: Insert one or more documents into a collection\nExample: db.collection.insert({id: \"1\", name: \"test\"})",
	"update":           "Description: Update documents in a collection\nExample: db.collection.update({$set: {name: \"new name\"}})",
	"updateId":         "Description: Update a document by ID with a modifier\nExample: db.collection.updateId(\"1\", {$set: {name: \"new name\"}})",
	"upsert":           "Description: Upsert documents into a collection\nExample: db.collection.upsert({id: \"1\", name: \"test\"})",
	"upsertId":         "Description: Upsert a document by ID with a modifier\nExample: db.collection.upsertId(\"1\", {$set: {name: \"new name\"}})",
	"count":            "Description: Count documents in a collection\nExample: db.collection.count()",
	"find":             "Description: Find documents in a collection\nExample: db.collection.find({name: \"test\"}).limit(10)",
	"findOne":          "Description: Find one document in a collection\nExample: db.collection.findOne({id: \"1\"})",
	"findId":           "Description: Find documents by ID\nExample: db.collection.findId(\"1\", \"2\")",
	"deleteId":         "Description: Delete documents by ID\nExample: db.collection.deleteId(\"1\", \"2\")",
	"ensureIndex":      "Description: Ensure an index exists on a collection\nExample: db.collection.ensureIndex({name: \"indexName\", fields: [\"fieldName\"], unique: true})",
	"dropIndex":        "Description: Drop an index from a collection\nExample: db.collection.dropIndex(\"indexName\")",
	"getIndexes":       "Description: Get all indexes on a collection\nExample: db.collection.getIndexes()",
	"drop":             "Description: Drop a collection\nExample: db.collection.drop()",
	"limit":            "Description: Limit the number of documents returned by a query\nExample: db.collection.find({}).limit(10)",
	"offset":           "Description: Skip a number of documents in a query\nExample: db.collection.find({}).offset(20)",
	"sort":             "Description: Sort the documents returned by a query\nExample: db.collection.find({}).sort(\"name\", \"-age\")",
	"hint":             "Description: Force the use of a specific index\nExample: db.collection.find({}).hint({indexName: 1})",
	"project":          "Description: Specify the fields to return in a query\nExample: db.collection.find({}).project({name: 1, age: 1})",
	"pretty":           "Description: Pretty-print the JSON output\nExample: db.collection.find({}).pretty()",
	"explain":          "Description: Show the query execution plan\nExample: db.collection.find({}).explain()",
	"delete":           "Description: Delete the documents matched by a query\nExample: db.collection.find({status: \"old\"}).delete()",
}

func (c *Conn) Help(cmd Cmd) (string, error) {
	if cmd.Path == "" {
		var sb strings.Builder
		sb.WriteString("Available commands:\n")
		sb.WriteString("  show collections\n")
		sb.WriteString("  show stats\n")
		sb.WriteString("  db\n")
		sb.WriteString("    .createCollection(name)\n")
		sb.WriteString("    .backup(path)\n")
		sb.WriteString("    .quickCheck()\n")
		sb.WriteString("    .{collection}\n")
		sb.WriteString("      .insert(doc, ...)\n")
		sb.WriteString("      .upsert(doc, ...)\n")
		sb.WriteString("      .upsertId(id, mod)\n")
		sb.WriteString("      .find(query)\n")
		sb.WriteString("        .limit(n)\n")
		sb.WriteString("        .offset(n)\n")
		sb.WriteString("        .sort(field, ...)\n")
		sb.WriteString("        .project(spec)\n")
		sb.WriteString("        .hint(spec)\n")
		sb.WriteString("        .count()\n")
		sb.WriteString("        .explain()\n")
		sb.WriteString("        .pretty()\n")
		sb.WriteString("        .update(doc)\n")
		sb.WriteString("        .delete()\n")
		sb.WriteString("      .findOne(query)\n")
		sb.WriteString("      .findId(id, ...)\n")
		sb.WriteString("      .update(updateDoc)\n")
		sb.WriteString("      .updateId(id, mod)\n")
		sb.WriteString("      .deleteId(id, ...)\n")
		sb.WriteString("      .count()\n")
		sb.WriteString("      .ensureIndex(indexDef)\n")
		sb.WriteString("      .dropIndex(name)\n")
		sb.WriteString("      .getIndexes()\n")
		sb.WriteString("      .rename(newName)\n")
		sb.WriteString("      .drop()\n")
		sb.WriteString("\nUse \"help {command}\" for more information on a specific command.")
		return sb.String(), nil
	}

	if help, ok := helpData[cmd.Path]; ok {
		return help, nil
	}

	// Try to match without "db.collection." or "db." prefix if the user typed that
	cleanCmd := cmd.Path
	if strings.HasPrefix(cleanCmd, "db.") {
		parts := strings.Split(cleanCmd, ".")
		if len(parts) > 1 {
			cleanCmd = parts[len(parts)-1]
		}
	}
	if help, ok := helpData[cleanCmd]; ok {
		return help, nil
	}

	return "", fmt.Errorf("no help available for command: %s", cmd.Path)
}

func (c *Conn) Complete(line string) (result []string) {
	lowerLine := strings.ToLower(line)
	if strings.HasPrefix(lowerLine, "help ") {
		prefix := line[:5]
		toComplete := strings.ToLower(line[5:])
		for cmd := range helpData {
			if strings.HasPrefix(strings.ToLower(cmd), toComplete) {
				result = append(result, prefix+cmd)
			}
		}
		return
	}
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

func (c *Conn) QuickCheck() (result string, err error) {
	err = c.db.QuickCheck(mainCtx.Ctx())
	if err != nil {
		return "", err
	}
	return "ok", nil
}

func (c *Conn) CreateCollection(cmd Cmd) (result string, err error) {
	_, err = c.db.CreateCollection(mainCtx.Ctx(), cmd.Collection)
	if err == nil {
		_ = c.makeAutocomplete()
	}
	return
}

func (c *Conn) Backup(cmd Cmd) (result string, err error) {
	if cmd.Path == "" {
		return "", fmt.Errorf("backup path is required")
	}
	err = c.db.Backup(mainCtx.Ctx(), cmd.Path)
	if err != nil {
		return "", err
	}
	return "ok", nil
}

func (c *Conn) Rename(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if err = coll.Rename(mainCtx.Ctx(), cmd.Path); err != nil {
		return
	}
	_ = c.makeAutocomplete()
	return "ok", nil
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

func (c *Conn) UpdateId(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) < 2 {
		return "", fmt.Errorf(`expected id and modifier`)
	}
	id, err := anyenc.ParseJson(string(cmd.Documents[0]))
	if err != nil {
		return
	}
	modVal, err := anyenc.ParseJson(string(cmd.Documents[1]))
	if err != nil {
		return
	}
	mod, err := query.ParseModifier(modVal)
	if err != nil {
		return
	}
	res, err := coll.UpdateId(mainCtx.Ctx(), id, mod)
	if err != nil {
		return
	}
	return fmt.Sprintf("matched: %v, modified: %v", res.Matched, res.Modified), nil
}

func (c *Conn) UpsertId(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	if len(cmd.Documents) < 2 {
		return "", fmt.Errorf(`expected id and modifier`)
	}
	id, err := anyenc.ParseJson(string(cmd.Documents[0]))
	if err != nil {
		return
	}
	modVal, err := anyenc.ParseJson(string(cmd.Documents[1]))
	if err != nil {
		return
	}
	mod, err := query.ParseModifier(modVal)
	if err != nil {
		return
	}
	res, err := coll.UpsertId(mainCtx.Ctx(), id, mod)
	if err != nil {
		return
	}
	return fmt.Sprintf("matched: %v, modified: %v", res.Matched, res.Modified), nil
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

func (c *Conn) GetIndexes(cmd Cmd) (result string, err error) {
	coll, err := c.db.OpenCollection(mainCtx.Ctx(), cmd.Collection)
	if err != nil {
		return
	}
	indexes := coll.GetIndexes()
	infos := make([]anystore.IndexInfo, len(indexes))
	for i, idx := range indexes {
		infos[i] = idx.Info()
	}
	var b []byte
	if b, err = json.MarshalIndent(infos, "", "  "); err != nil {
		return
	}
	return string(b), nil
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

	val := doc.Value()
	if len(cmd.Query.Project) > 0 {
		if val, err = applyProjection(val, cmd.Query.Project); err != nil {
			return
		}
	}

	if cmd.Query.Pretty {
		result, err = prettyJson(val.String())
	} else {
		result = val.String()
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

		val := doc.Value()
		if len(cmd.Query.Project) > 0 {
			if val, err = applyProjection(val, cmd.Query.Project); err != nil {
				return "", err
			}
		}

		res, err := prettyJson(val.String())
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
		val := doc.Value()
		if len(cmd.Query.Project) > 0 {
			if val, err = applyProjection(val, cmd.Query.Project); err != nil {
				return "", err
			}
		}
		if cmd.Query.Pretty {
			res, err := prettyJson(val.String())
			if err != nil {
				return "", err
			}
			fmt.Println(res)
		} else {
			fmt.Println(val.String())
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

func applyProjection(val *anyenc.Value, projection json.RawMessage) (*anyenc.Value, error) {
	var projMap map[string]int
	if err := json.Unmarshal(projection, &projMap); err != nil {
		return nil, err
	}
	if len(projMap) == 0 {
		return val, nil
	}

	obj, err := val.Object()
	if err != nil {
		return val, nil // Not an object, can't project
	}

	// Check if it's inclusion or exclusion projection
	inclusion := false
	for _, v := range projMap {
		if v > 0 {
			inclusion = true
			break
		}
	}

	arena := &anyenc.Arena{}
	newVal := arena.NewObject()
	if inclusion {
		obj.Visit(func(k []byte, v *anyenc.Value) {
			key := string(k)
			if projMap[key] > 0 || key == "id" {
				newVal.Set(key, v)
			}
		})
	} else {
		obj.Visit(func(k []byte, v *anyenc.Value) {
			key := string(k)
			if projMap[key] == 0 {
				newVal.Set(key, v)
			}
		})
	}
	return newVal, nil
}
