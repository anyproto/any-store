package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/query"
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

// helpExample is a command shown in the help, with a line of the output it
// produces on the example data (help_test.go runs every one of them).
type helpExample struct {
	cmd, out string
}

var helpExamples = []helpExample{
	{`db.orders.find({"status":"paid","total":{"$gt":20}}).sort("-total").project({"total":1})`, `{"id":"o1","total":42.5}`},
	{`db.orders.find({"items":"pen"}).count()`, `4`},
	{`db.orders.find({"placed":{"$gte":"2026-09-01"}}).count()`, `4`},
	{`db.orders.aggregate([{"$unwind":"$items"},{"$group":{"_id":"$items","n":{"$count":{}}}},{"$sort":{"n":-1}}])`, `{"id":"pen","n":4}`},
	{`db.orders.aggregate([{"$group":{"_id":"$status","total":{"$sum":"$total"}}},{"$project":{"id":1,"total":{"$round":["$total",1]}}}])`, `{"id":"open","total":111.9}`},
	{`db.orders.updateId("o2", {"$set":{"status":"paid"}})`, `matched: 1, modified: 1`},
	{`db.orders.find({"status":"cancelled"}).delete()`, `Deleted:	1`},
}

// helpJoinExample groups one collection and looks up another per line: the
// way to combine collections, which cannot be joined in a query.
const helpJoinExample = `{name} shop.db -e 'db.orders.aggregate([{"$group":{"_id":"$customerId","spent":{"$sum":"$total"}}}])' |
while read -r row; do
  cid=$(echo "$row" | sed 's/.*"id":"\([^"]*\)".*/\1/')
  name=$({name} shop.db -e "db.customers.findId(\"$cid\").project({\"name\":1})" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')
  echo "$name $row"   # Ada {"id":"c1","spent":142.4}
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
  .aggregate([stage, ...])                  pipeline; chain .count(), .pretty(), .explain()
  .ensureIndex({"fields":["a","-b"]})   .dropIndex("name")   .getIndexes()
  .stats()   .rename("name")   .drop()

Filters
  {"field": value} matches equal values. Dotted paths ("a.b") reach nested fields; an array field
  matches when any element does. Strings compare bytewise, so "YYYY-MM-DD" dates work with $gt/$lt.
{filterOps}
  {"total":{"$gte":10,"$lt":50}}   {"$or":[{"status":"open"},{"vip":true}]}
  {"coupon":{"$exists":false}}   {"items":{"$all":["pen","ink"]}}   {"items":{"$size":1}}
  {"note":{"$regex":"^urgent","$options":"i"}} (RE2)

Modifiers (for .update and .updateId)
{modifiers}
  {"$set":{"status":"shipped"}}   {"$inc":{"total":5}}   {"$unset":{"coupon":""}}
  {"$addToSet":{"items":"pen"}}

Aggregation
{stages}
{accumulators}
  expressions: "$field.path", {"$literal":v}, $add $subtract $multiply $divide $abs $round $concat
               $split $replaceOne $replaceAll $trim $strLenCP $size $cond $switch $ifNull $eq $ne
               $gt $gte $lt $lte $cmp $dateAdd $dateDiff $dateTrunc $year $week
  {"$match":{"$expr":{"$gt":["$paid","$total"]}}} compares two fields of one document.

Differences from MongoDB
  - Documents use "id", not "_id". $group takes "_id" but outputs the key as "id": later stages
    refer to it as "$id", and $project keeps it with "id":1.
  - .count(filter) ignores its argument: use .find(filter).count().
  - No update(filter, modifier), updateOne or updateMany: use .find(filter).update(modifier).
  - .sort takes field names, "-" for descending: .sort("-total", "name"), not .sort({"total":-1}).
    aggregate() has no .sort(): use a $sort stage.
  - No variables or cursor methods (forEach, toArray, distinct): a command is one expression.
  - Projections only include top-level fields: {"a":0}, {"id":0} and {"_id":0} are errors, and
    .project({"author.name":1}) prints no author. Project {"author":1}, or rename in a $project
    stage: {"name":"$author.name"}.
  - $lookup joins documents of the same collection by id; collections cannot be joined.
  - A $sort stage with several keys applies them in alphabetical key order. Sort by one key per
    stage, the secondary key first: [{"$sort":{"name":1}},{"$sort":{"total":-1}}].
  - $expr works in an aggregation $match only, not in find().
  - Numbers are 64-bit floats.

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
    methods such as .aggregate() and .updateId() don't chain after .find(): filter an aggregation
    with a $match stage.
  - Extract values from the output lines with sed, as below: POSIX awk's match() has no capture
    array.
  - To combine collections, read or group one and look up the others line by line; a $group row
    carries its key as "id":
{join}
`

// printHelp writes the command reference. The operator, stage and
// accumulator lists come from the library, so they match what the parsers
// accept. forAgent adds guidance for scripts that run the CLI.
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
		"{filterOps}", wrapList("operators:", query.Operators()),
		"{modifiers}", wrapList("operators:", query.ModifierOperators()),
		"{stages}", wrapList("stages:", anystore.AggregateStages()),
		"{accumulators}", wrapList("accumulators:", anystore.AggregateAccumulators()),
		"{examples}", examples.String(),
		"{join}", indent(helpJoinExample, "      "),
	).Replace(text)
	// {name} last: the examples and the join example carry it too.
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
