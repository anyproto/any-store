package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/robertkrimen/otto"
)

//go:embed commands.js
var commandsJS string

func newJs() (*js, error) {
	res := &js{
		otto: otto.New(),
	}
	if err := res.init(); err != nil {
		return nil, err
	}
	return res, nil
}

type js struct {
	otto *otto.Otto
}

func (j *js) init() (err error) {
	_, err = j.otto.Run(commandsJS)
	return
}

type Cmd struct {
	Cmd        string            `json:"cmd"`
	Collection string            `json:"collection"`
	Path       string            `json:"path"`
	Index      Index             `json:"index"`
	Query      Query             `json:"query"`
	Documents  []json.RawMessage `json:"documents"`
}

type Query struct {
	Find    json.RawMessage `json:"find"`
	Update  json.RawMessage `json:"update"`
	Project json.RawMessage `json:"project"`
	Limit   int             `json:"limit"`
	Offset  int             `json:"offset"`
	Count   bool            `json:"count"`
	Delete  bool            `json:"delete"`
	Explain bool            `json:"explain"`
	Pretty  bool            `json:"pretty"`
	Sort    []string        `json:"sort"`
	Hint    map[string]int  `json:"hint"`
}

type Index struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique"`
	Sparse bool     `json:"sparse"`
}

func (j *js) GetQuery(line string) (q Cmd, err error) {
	val, err := j.otto.Run(line + ".result()")
	if err != nil {
		return
	}
	result := val.String()
	if jErr := json.Unmarshal([]byte(result), &q); jErr != nil {
		err = fmt.Errorf("js error: %s", result)
		return
	}
	return
}

func (j *js) RegisterCollection(name string) {
	if _, err := j.otto.Run(fmt.Sprintf(`db[%[1]s] = new Collection(%[1]s);`, strconv.Quote(name))); err != nil {
		fmt.Fprintln(os.Stderr, "js: can't register collection:", err)
	}
}

func (j *js) UnregisterCollection(name string) {
	if _, err := j.otto.Run(fmt.Sprintf(`delete db[%s];`, strconv.Quote(name))); err != nil {
		fmt.Fprintln(os.Stderr, "js: can't unregister collection:", err)
	}
}
