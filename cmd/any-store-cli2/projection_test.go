package main

import (
	"encoding/json"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestApplyProjection_PrimaryKey(t *testing.T) {
	val := anyenc.MustParseJson(`{"uuid":"x","name":"n","extra":"e"}`)
	out, err := applyProjection(val, json.RawMessage(`{"name":1}`), "uuid")
	if err != nil {
		t.Fatal(err)
	}
	if out.Get("uuid") == nil {
		t.Fatalf("primary key field 'uuid' must be retained by an inclusion projection")
	}
	if out.Get("name") == nil {
		t.Fatalf("'name' must be included")
	}
	if out.Get("extra") != nil {
		t.Fatalf("'extra' must be excluded")
	}
}
