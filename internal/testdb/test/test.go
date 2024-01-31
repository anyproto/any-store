package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/valyala/fastjson"

	anystore "github.com/anyproto/any-store"
)

var (
	path       = flag.String("path", ".db", "path to database folder")
	collection = flag.String("c", "test", "collection name")

	insert = flag.String("insert", "", "insert document")

	insertR = flag.Int("insertR", 0, "num of elements to insertR")

	insertBatchR = flag.Int("insertBatchR", 0, "num of elements to insertR")

	count = flag.String("count", "", "count by query")

	explain = flag.Bool("e", true, "print query explain")

	find = flag.String("find", "", "query condition")

	limit  = flag.Uint("limit", 0, "limit")
	offset = flag.Uint("offset", 0, "offset")

	sort = flag.String("sort", "", "comma separated sort fields")

	hint = flag.String("hint", "", "hint index name")

	ensureIndex = flag.String("ensureIndex", "", "index field")
	dropIndex   = flag.String("dropIndex", "", "index name")
	indexList   = flag.Bool("indexList", false, "show index list")

	fields = flag.String("fields", "", "comma separated fieldNames to show")
)

var (
	arena = &fastjson.Arena{}
)

func main() {
	flag.Parse()
	db, err := anystore.Open(*path)
	if err != nil {
		log.Fatal(err)
	}

	coll, err := db.Collection(*collection)
	if err != nil {
		log.Fatal(err)
	}

	if *insert != "" {
		st := time.Now()
		res, err := coll.InsertOne(*insert)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("inserted element; id=%v; %s", res, time.Since(st))
		_ = db.Close()
		return
	}

	if *insertR > 0 {
		st := time.Now()
		for i := 0; i < *insertR; i++ {
			if _, err = coll.InsertOne(generateDoc(i)); err != nil {
				log.Fatal(err)
			}
		}
		log.Printf("inserted %d elemets; %s", *insertR, perSecString(*insertR, time.Since(st)))
		_ = db.Close()
		return
	}
	if *insertBatchR > 0 {
		var docs = make([]any, *insertBatchR)
		for i := range docs {
			docs[i] = generateDoc(i)
		}
		st := time.Now()
		if _, err = coll.InsertMany(docs...); err != nil {
			log.Fatal(err)
		}
		log.Printf("batch inserted %d elemets; %s", *insertBatchR, perSecString(*insertBatchR, time.Since(st)))
		_ = db.Close()
		return
	}

	if *count != "" {
		if len(*count) < 2 {
			st := time.Now()
			countRes, err := coll.Count()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("count is %d; %v", countRes, time.Since(st))
			_ = db.Close()
			return
		} else {
			st := time.Now()
			query := coll.Find().Cond(*count)
			countRes, err := query.Count()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("count is %d; %v", countRes, time.Since(st))
			if *explain {
				it, err := query.Iter()
				if err != nil {
					log.Fatal(err)
				}
				log.Printf("explain: %s", it.Explain())
				_ = it.Close()
			}
			_ = db.Close()
			return
		}
	}

	if *ensureIndex != "" {
		st := time.Now()
		if err = coll.EnsureIndex(anystore.Index{
			Fields: strings.Split(*ensureIndex, ","),
		}); err != nil {
			log.Fatal(err)
		}
		log.Printf("index ensured; %s", time.Since(st))
		_ = db.Close()
		return
	}

	if *dropIndex != "" {
		st := time.Now()
		if err = coll.DropIndex(*dropIndex); err != nil {
			log.Fatal(err)
		}
		log.Printf("index dropped; %s", time.Since(st))
		_ = db.Close()
		return
	}

	if *indexList {
		idxs, err := coll.Indexes()
		if err != nil {
			log.Fatal(err)
		}
		for _, idx := range idxs {
			fmt.Printf("indexName: %s\tsparse: %v\n", idx.Name(), idx.Sparse)
		}
		_ = db.Close()
		return
	}

	st := time.Now()
	query := coll.Find()
	if len(*find) > 0 {
		query.Cond(*find)
	}
	query.Limit(*limit).Offset(*offset).IndexHint(*hint)
	if len(*sort) > 0 {
		sorts := strings.Split(*sort, ",")
		sortArgs := make([]any, len(sorts))
		for i, s := range sorts {
			sortArgs[i] = s
		}
		query.Sort(sortArgs...)
	}
	iter, err := query.Iter()
	if err != nil {
		log.Fatal(err)
	}
	var printed int
	for iter.Next() {
		val := iter.Item().Value()
		if printed < 100 {
			printValue(val)
		}
		printed++
	}
	log.Print("find: ", time.Since(st))
	if *explain {
		log.Printf("explain: %s", iter.Explain())
	}
	if err = iter.Close(); err != nil {
		log.Fatal(err)
	}
	_ = db.Close()

}

func printValue(val *fastjson.Value) {
	if *fields == "" {
		fmt.Println(val.String())
		return
	}
	obj := arena.NewObject()
	for _, field := range strings.Split(*fields, ",") {
		obj.Set(field, val.Get(field))
	}
	fmt.Println(obj.String())
}

func perSecString(count int, dur time.Duration) string {
	var res = dur.String()
	if count > 0 {
		res += fmt.Sprintf(" %d p/s", int(float64(count)/dur.Seconds()))
	}
	return res
}

func generateDoc(n int) *fastjson.Value {
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(randId()))
	obj.Set("text", arena.NewString(randText(rand.Intn(10)+10)))
	obj.Set("num", arena.NewNumberInt(n))
	obj.Set("knownId", arena.NewString(randKnownIds[rand.Intn(len(randKnownIds))]))
	arr := arena.NewArray()
	for i := 0; i < rand.Intn(10); i++ {
		arr.SetArrayItem(i, arena.NewString(randKnownIds[rand.Intn(len(randKnownIds))]))
	}
	obj.Set("knownIds", arr)
	obj.Set("timeMicro", arena.NewNumberInt(int(time.Now().UnixMicro())))
	return obj
}

func randText(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	result := make([]byte, n)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

func randId() string {
	return "baef" + randText(30)
}

var randKnownIds = []string{
	randId(), randId(), randId(), randId(), randId(), randId(), randId(), randId(), randId(), randId(), randId(), randId(),
}
