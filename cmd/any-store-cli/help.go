package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/pflag"
)

// helpForAgentEnv is set to 1 by tools that hand a command's --help to a
// language model; the help then adds a section on scripting the CLI.
const helpForAgentEnv = "HELP_FOR_AGENT"

// printUsage is the short reminder printed on a usage error.
func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage:\n%s path/to/dbFile.db  [flags]\nFlags:\n", os.Args[0])
	pflag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "Run %s --help for the command reference.\n", os.Args[0])
}

// Filter and modifier operators the query parser accepts (help_test.go
// parses each one).
var (
	helpFilterOps   = []string{"$all", "$and", "$eq", "$exists", "$gt", "$gte", "$in", "$lt", "$lte", "$ne", "$nin", "$nor", "$not", "$or", "$regex", "$size", "$type"}
	helpModifierOps = []string{"$addToSet", "$inc", "$pop", "$pull", "$pullAll", "$push", "$rename", "$set", "$unset"}
)

// helpExample is a command shown in the help, with a line of the output it
// produces on the example data (help_test.go runs every one of them).
type helpExample struct {
	cmd, out string
}

var helpExamples = []helpExample{
	{`db.orders.find({"status":"paid","total":{"$gt":20}}).sort("-total").project({"total":1})`, `{"id":"o1","total":42.5}`},
	{`db.orders.find({"items":"pen"}).count()`, `4`},
	{`db.orders.find({"placed":{"$gte":"2026-09-01"}}).count()`, `4`},
	{`db.orders.find({"status":{"$regex":"(?i)^PA"}}).count()`, `2`},
	{`db.orders.find({"items":{"$all":["pen","ink"]}}).project({"id":1})`, `{"id":"o1"}`},
	{`db.orders.updateId("o2", {"$set":{"status":"paid"}})`, `matched: 1, modified: 1`},
	{`db.orders.find({"status":"cancelled"}).delete()`, `Deleted:	1`},
}

// helpGroupExample and helpJoinExample do in the shell what this version
// has no aggregation for: grouping, and combining collections.
const helpGroupExample = `{name} shop.db -e 'db.orders.find({}).project({"status":1,"total":1})' |
  sed 's/.*"status":"\([^"]*\)","total":\([0-9.]*\).*/\1 \2/' |
  awk '{n[$1]++; s[$1]+=$2} END {for (k in n) print k, n[k], s[k]}'`

const helpJoinExample = `{name} shop.db -e 'db.orders.find({"status":"open"}).project({"customerId":1,"total":1})' |
while read -r order; do
  cid=$(echo "$order" | sed 's/.*"customerId":"\([^"]*\)".*/\1/')
  name=$({name} shop.db -e "db.customers.findId(\"$cid\").project({\"name\":1})" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')
  echo "$name $order"
done`

const helpText = `{name} queries and edits an any-store database.

Usage:
  {name} <file.db>
      interactive shell: history, tab completion, "help <command>"; results come 30 at a time,
      "it" prints more
  {name} <file.db> -e '<command>'
      run one command, print all of its results, exit

Flags:
{flags}
Commands are JavaScript in a MongoDB-style shell; results print one JSON document per line.
There is no aggregation pipeline: group and combine results in the calling script.

Database
  show collections                          collection names, one per line
  show stats                                database statistics
  db.createCollection("name")               a collection must exist before inserting into it
  db.backup("copy.db")   db.quickCheck()

Collection: db.<collection>
  .insert(doc, ...)                         "id" is the primary key
  .update(doc)   .upsert(doc)               replace the document with doc's id; upsert also inserts
  .find(filter)                             matching documents; chain .sort("a", "-b"), .limit(n),
                                            .offset(n), .project({"a":1}), .count(),
                                            .update(modifier), .delete(), .pretty(), .explain()
  .findId("id", ...)                        documents by id
  .findOne(filter)                          first match, pretty-printed
  .updateId("id", modifier)   .upsertId("id", modifier)   .deleteId("id", ...)
  .count()                                  number of documents in the collection
  .ensureIndex({"fields":["a","-b"]})   .dropIndex("name")   .getIndexes()
  .rename("name")   .drop()

Filters
  {"field": value} matches equal values. Dotted paths ("a.b") reach nested fields; an array field
  matches when any element does. Strings compare bytewise, so "YYYY-MM-DD" dates work with $gt/$lt.
{filterOps}
  {"total":{"$gte":10,"$lt":50}}   {"$or":[{"status":"open"},{"vip":true}]}
  {"coupon":{"$exists":false}}   {"items":{"$all":["pen","ink"]}}   {"items":{"$size":1}}
  {"note":{"$regex":"(?i)^urgent"}} (RE2, flags inline)

Modifiers (for .update and .updateId)
{modifiers}
  {"$set":{"status":"shipped"}}   {"$inc":{"total":5}}   {"$unset":{"coupon":""}}
  {"$addToSet":{"items":"pen"}}

Differences from MongoDB
  - Documents use "id", not "_id".
  - .count(filter) ignores its argument: use .find(filter).count().
  - No update(filter, modifier), updateOne or updateMany: use .find(filter).update(modifier).
  - .sort takes field names, "-" for descending: .sort("-total", "name"), not .sort({"total":-1}).
  - No variables or cursor methods (forEach, toArray, distinct): a command is one expression.
  - Projections only include top-level fields: {"a":0} is ignored and prints the whole document,
    and {"author.name":1} prints no author; project {"author":1}.
  - No aggregate() and no joins.
  - $regex takes no $options: put flags in the pattern, "(?i)^urgent".
  - No $elemMatch.

Examples
  The data: orders {id, customerId, status, total, items[], placed "YYYY-MM-DD"},
  customers {id, name, city}.
{examples}`

const helpScripting = `
Scripting
  - Quote the command with single quotes; inside it, JSON keys and strings use double quotes.
  - Output: one JSON document per line, keys in alphabetical order; counts print a number.
    .find(filter).update() prints "Matched:" and "Modified:" lines, .updateId() prints
    "matched: N, modified: N", .delete() prints "Deleted: N". .findOne() spreads a document over
    several lines; .find(filter).limit(1) prints it on one.
  - Use the collection and field names the request gives, as in "collection=books, fields=year"
    or in sample documents; without them, take the names from the request's wording.
  - Ids are not titles or names: unless the request gives the id, select by fields,
    .find({"title":"Dune"}).update(modifier), not .updateId("Dune", modifier).
  - .find({}) matches every document: filter updates and deletes down to the intended documents.
  - Put every condition into a single .find(filter); .find(a).find(b) is an error. Collection
    methods such as .updateId() don't chain after .find().
  - Extract values from the output lines with sed, as below: POSIX awk's match() has no capture
    array.
  - To group, project the fields and aggregate the lines, e.g. count and total per status:
{group}
  - To combine collections, read one and look up the others line by line:
{join}
`

// printHelp writes the command reference; forAgent adds guidance for
// scripts that run the CLI.
func printHelp(w io.Writer, name string, forAgent bool) {
	var flags strings.Builder
	pflag.CommandLine.SetOutput(&flags)
	pflag.PrintDefaults()
	pflag.CommandLine.SetOutput(os.Stderr)

	var examples strings.Builder
	for _, e := range helpExamples {
		fmt.Fprintf(&examples, "  {name} shop.db -e '%s'\n      %s\n", e.cmd, e.out)
	}

	text := helpText
	if forAgent {
		text += helpScripting
	}
	text = strings.NewReplacer(
		"{flags}", flags.String(),
		"{filterOps}", wrapList("operators:", helpFilterOps),
		"{modifiers}", wrapList("operators:", helpModifierOps),
		"{examples}", examples.String(),
		"{group}", indent(helpGroupExample, "      "),
		"{join}", indent(helpJoinExample, "      "),
	).Replace(text)
	// {name} last: the examples carry it too.
	fmt.Fprint(w, strings.ReplaceAll(text, "{name}", name))
}

func helpName() string { return filepath.Base(os.Args[0]) }

// wrapList formats "  label a b c" wrapped at 100 columns, continuation lines
// aligned under the first item.
func wrapList(label string, items []string) string {
	const width = 100
	prefix := "  " + label + " "
	pad := strings.Repeat(" ", len(prefix))
	var b strings.Builder
	line := prefix
	for _, it := range items {
		if len(line)+len(it) > width && line != prefix && line != pad {
			b.WriteString(strings.TrimRight(line, " ") + "\n")
			line = pad
		}
		line += it + " "
	}
	b.WriteString(strings.TrimRight(line, " "))
	return b.String()
}

func indent(s, prefix string) string {
	return prefix + strings.ReplaceAll(s, "\n", "\n"+prefix)
}
