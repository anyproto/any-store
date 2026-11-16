package btree

// The page-1 contract under a random workload: catalog changes (page 1 is
// the catalog's root), puts from a few bytes to several pages (overflow
// chains), deletes (the freelist), savepoints rolled back and released,
// whole transactions rolled back, in-place transactions that leave the header
// alone, cookie-only schema changes, a 10-page writer cache (spills), and
// automatic checkpoints (writer restarts). The page-1 checks of
// page1_check.go run at every write begin and after every commit, in every
// process (TestMain turns them on).
//
// Two processes take turns in lockstep: the parent draws, from the seed,
// which process runs each transaction and the seed of its content, and tells
// the child over a pipe. So a failing seed replays the same sequence of
// transactions. (Frame-level order inside a transaction is not fixed: the
// savepoint restore iterates a Go map. The checks are logical.) Each process
// keeps the model of its own namespaces; at the end the parent compares the
// whole namespace set and every value, its own directly, the child's by
// digest, and checks the schema cookie against both processes' schema
// commits.

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type p1Model map[string]map[string][]byte

func (m p1Model) clone() p1Model {
	c := make(p1Model, len(m))
	for ns, kv := range m {
		c2 := make(map[string][]byte, len(kv))
		for k, v := range kv {
			c2[k] = v
		}
		c[ns] = c2
	}
	return c
}

func p1Sorted[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// p1Proc is one process's side of the workload: its namespaces (prefix-N),
// their committed contents, and its count of schema commits.
type p1Proc struct {
	db      *DB
	prefix  string
	model   p1Model
	nsSeq   int
	schemas int
}

// tx runs one random write transaction drawn from rnd.
func (pr *p1Proc) tx(rnd *rand.Rand) error {
	tx, err := pr.db.BeginWrite()
	if err != nil {
		return fmt.Errorf("begin write: %w", err)
	}
	work := pr.model.clone()
	type sp struct {
		id int
		m  p1Model
	}
	var stack []sp
	schema := false
	valSize := func() int {
		switch r := rnd.Intn(10); {
		case r < 6:
			return 10 + rnd.Intn(200)
		case r < 9:
			return 500 + rnd.Intn(2500)
		default:
			return 5000 + rnd.Intn(15000)
		}
	}
	fail := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	nOps := 1 + rnd.Intn(25)
	if rnd.Intn(100) < 40 {
		// In place: same-size overwrites of small values. No allocation, no
		// free: the header stays, and page 1 is left out unless the cookie
		// moves (a quarter of them: a metadata-only schema change).
		nOps = 0
		for _, name := range p1Sorted(work) {
			h, err := tx.GetNamespace(name)
			if err != nil {
				return fail(err)
			}
			for _, k := range p1Sorted(work[name]) {
				v := work[name][k]
				if len(v) > 200 || rnd.Intn(4) != 0 {
					continue
				}
				nv := make([]byte, len(v))
				rnd.Read(nv)
				if err := tx.Put(h, []byte(k), nv); err != nil {
					return fail(err)
				}
				work[name][k] = nv
				schema = schema || rnd.Intn(4) == 0
			}
			break
		}
	}
	for op := 0; op < nOps; op++ {
		names := p1Sorted(work)
		switch r := rnd.Intn(100); {
		case r < 6 || len(names) == 0:
			pr.nsSeq++
			name := fmt.Sprintf("%s-%d", pr.prefix, pr.nsSeq)
			if _, err := tx.CreateNamespace(name); err != nil {
				return fail(fmt.Errorf("create %s: %w", name, err))
			}
			work[name] = map[string][]byte{}
			schema = true
		case r < 9 && len(names) > 1:
			name := names[rnd.Intn(len(names))]
			if err := tx.DeleteNamespace(name); err != nil {
				return fail(fmt.Errorf("delete %s: %w", name, err))
			}
			delete(work, name)
			schema = true
		case r < 60:
			name := names[rnd.Intn(len(names))]
			h, err := tx.GetNamespace(name)
			if err != nil {
				return fail(err)
			}
			for j := rnd.Intn(30); j >= 0; j-- {
				k := fmt.Sprintf("k%04d", rnd.Intn(400))
				v := make([]byte, valSize())
				rnd.Read(v)
				if err := tx.Put(h, []byte(k), v); err != nil {
					return fail(fmt.Errorf("put: %w", err))
				}
				work[name][k] = v
			}
		case r < 82:
			name := names[rnd.Intn(len(names))]
			h, err := tx.GetNamespace(name)
			if err != nil {
				return fail(err)
			}
			for j := rnd.Intn(40); j >= 0; j-- {
				k := fmt.Sprintf("k%04d", rnd.Intn(400))
				if _, ok := work[name][k]; ok {
					if err := tx.Delete(h, []byte(k)); err != nil {
						return fail(fmt.Errorf("delete: %w", err))
					}
					delete(work[name], k)
				}
			}
		case r < 90:
			id, err := tx.Savepoint()
			if err != nil {
				return fail(err)
			}
			stack = append(stack, sp{id, work.clone()})
		case r < 96 && len(stack) > 0:
			top := stack[len(stack)-1]
			if err := tx.RollbackToSavepoint(top.id); err != nil {
				return fail(fmt.Errorf("rollback to: %w", err))
			}
			work = top.m.clone()
			stack = stack[:len(stack)-1]
		case len(stack) > 0:
			top := stack[len(stack)-1]
			if err := tx.ReleaseSavepoint(top.id); err != nil {
				return fail(err)
			}
			stack = stack[:len(stack)-1]
		}
	}
	if rnd.Intn(100) < 12 {
		return tx.Rollback()
	}
	tx.MarkDataChanged()
	if schema {
		tx.MarkSchemaChanged()
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	pr.model = work
	if schema {
		pr.schemas++
	}
	return nil
}

// p1Digest hashes the namespaces of db named prefix-*, read at one snapshot,
// or those of model when db is nil.
func p1Digest(db *DB, prefix string, model p1Model) (string, error) {
	h := sha256.New()
	if db == nil {
		for _, name := range p1Sorted(model) {
			fmt.Fprintf(h, "ns %s\n", name)
			for _, k := range p1Sorted(model[name]) {
				fmt.Fprintf(h, "%s=%x\n", k, model[name][k])
			}
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	names, err := db.ListNamespaces()
	if err != nil {
		return "", err
	}
	slices.Sort(names)
	rtx, err := db.BeginRead()
	if err != nil {
		return "", err
	}
	defer rtx.Rollback()
	for _, name := range names {
		if !strings.HasPrefix(name, prefix+"-") {
			continue
		}
		fmt.Fprintf(h, "ns %s\n", name)
		ns, err := rtx.GetNamespace(name)
		if err != nil {
			return "", err
		}
		c := rtx.NewCursor(ns)
		for err := c.First(); err == nil && c.Valid(); err = c.Next() {
			k, kerr := c.Key()
			v, verr := c.Value()
			if kerr != nil || verr != nil {
				c.Close()
				return "", fmt.Errorf("ns %s: %v %v", name, kerr, verr)
			}
			fmt.Fprintf(h, "%s=%x\n", k, v)
		}
		c.Close()
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// p1PropOpts are the options of the workload's handles.
func p1PropOpts(mode string, codec bool) Options {
	opts := Options{PageSize: 4096, CacheSize: 10, AutoCheckpointAfter: 300}
	switch mode {
	case "in-process":
		opts.InProcess = true
	case "in-memory":
		opts.InMemory = true
	}
	if codec {
		opts.Key = []byte("page-1 property test passphrase")
		opts.KDFIterations = 2
	}
	return opts
}

const (
	p1PropChildEnv  = "BTREE_TEST_P1_PROP_CHILD"
	p1PropMutateEnv = "BTREE_TEST_P1_MUTATE"
	p1PropSeedEnv   = "BTREE_TEST_P1_SEED"
)

// p1Child is the parent's end of the child process.
type p1Child struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Scanner
	log *bytes.Buffer
}

func (c *p1Child) ask(t *testing.T, req string) string {
	t.Helper()
	_, err := fmt.Fprintln(c.in, req)
	require.NoError(t, err, "child: %s", c.log)
	require.True(t, c.out.Scan(), "child ended: %s", c.log)
	line := c.out.Text()
	require.False(t, strings.HasPrefix(line, "err "), "child: %s\n%s", line, c.log)
	return line
}

func TestPage1Prop_Lockstep(t *testing.T) {
	if os.Getenv(p1PropMutateEnv) == "1" {
		testIgnorePage1HeaderDiff = true
		defer func() { testIgnorePage1HeaderDiff = false }()
	}
	seeds := []int64{1, 2}
	if s := os.Getenv(p1PropSeedEnv); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		require.NoError(t, err)
		seeds = []int64{v}
	}
	const steps = 150
	for _, v := range []struct {
		mode  string
		codec bool
	}{{"multi-process", false}, {"multi-process", true}, {"in-process", false}, {"in-memory", false}} {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/codec=%v/seed=%d", v.mode, v.codec, seed), func(t *testing.T) {
				opts := p1PropOpts(v.mode, v.codec)
				path := filepath.Join(t.TempDir(), "p1.db")
				db, err := testOpen(t, path, opts)
				require.NoError(t, err)
				defer db.Close()
				sc0 := db.pager.header.SchemaCookie
				parent := &p1Proc{db: db, prefix: "p", model: p1Model{}}
				var child *p1Child
				if v.mode == "multi-process" {
					child = startP1Child(t, path, v.codec)
				}
				rnd := rand.New(rand.NewSource(seed))
				for i := 0; i < steps; i++ {
					txSeed := rnd.Int63()
					if child != nil && rnd.Intn(2) == 1 {
						child.ask(t, "tx "+strconv.FormatInt(txSeed, 10))
						continue
					}
					require.NoError(t, parent.tx(rand.New(rand.NewSource(txSeed))), "step %d", i)
				}

				require.NoError(t, db.IntegrityCheck())
				want, err := p1Digest(nil, "p", parent.model)
				require.NoError(t, err)
				got, err := p1Digest(db, "p", nil)
				require.NoError(t, err)
				require.Equal(t, want, got, "the parent's namespaces")
				wantNames := p1Sorted(parent.model)
				schemas := parent.schemas
				if child != nil {
					f := strings.Fields(child.ask(t, "state"))
					require.Len(t, f, 4)
					got, err := p1Digest(db, "c", nil)
					require.NoError(t, err)
					require.Equal(t, f[1], got, "the child's namespaces")
					n, err := strconv.Atoi(f[2])
					require.NoError(t, err)
					schemas += n
					if f[3] != "-" {
						wantNames = append(wantNames, strings.Split(f[3], ",")...)
					}
					child.ask(t, "quit")
					require.NoError(t, child.cmd.Wait(), "child: %s", child.log)
				}
				names, err := db.ListNamespaces()
				require.NoError(t, err)
				slices.Sort(names)
				slices.Sort(wantNames)
				require.Equal(t, wantNames, names, "the whole namespace set")
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				sc := rtx.SnapshotSchemaCookie()
				require.NoError(t, rtx.Rollback())
				require.Equal(t, sc0+uint32(schemas), sc, "one cookie step per schema commit of either process")

				if v.mode != "in-memory" {
					require.NoError(t, db.Close())
					db2, err := testOpen(t, path, opts)
					require.NoError(t, err)
					defer db2.Close()
					require.NoError(t, db2.IntegrityCheck())
					got, err := p1Digest(db2, "p", nil)
					require.NoError(t, err)
					require.Equal(t, want, got, "after a reopen")
				}
			})
		}
	}
}

func startP1Child(t *testing.T, path string, codec bool) *p1Child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPage1Prop_Child$", "-test.timeout=300s")
	cmd.Env = append(os.Environ(), p1PropChildEnv+"="+path+"|"+strconv.FormatBool(codec))
	in, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	log := &bytes.Buffer{}
	cmd.Stderr = log
	require.NoError(t, cmd.Start())
	c := &p1Child{cmd: cmd, in: in, out: bufio.NewScanner(stdout), log: log}
	c.out.Buffer(make([]byte, 1<<20), 1<<20)
	t.Cleanup(func() {
		_ = in.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	require.Equal(t, "ready", c.ask(t, "hello"))
	return c
}

// TestPage1Prop_Child is the child of TestPage1Prop_Lockstep: it runs the
// transactions the parent sends, on namespaces c-*.
func TestPage1Prop_Child(t *testing.T) {
	arg := os.Getenv(p1PropChildEnv)
	if arg == "" {
		t.Skip("child process only")
	}
	if os.Getenv(p1PropMutateEnv) == "1" {
		testIgnorePage1HeaderDiff = true
	}
	path, codecArg, _ := strings.Cut(arg, "|")
	codec, err := strconv.ParseBool(codecArg)
	require.NoError(t, err)
	db, err := Open(path, p1PropOpts("multi-process", codec))
	require.NoError(t, err)
	defer db.Close()
	pr := &p1Proc{db: db, prefix: "c", model: p1Model{}}
	in := bufio.NewScanner(os.Stdin)
	reply := func(s string) { fmt.Println(s) }
	for in.Scan() {
		f := strings.Fields(in.Text())
		switch f[0] {
		case "hello":
			reply("ready")
		case "tx":
			seed, err := strconv.ParseInt(f[1], 10, 64)
			if err == nil {
				err = pr.tx(rand.New(rand.NewSource(seed)))
			}
			if err != nil {
				reply("err " + err.Error())
				return
			}
			reply("ok")
		case "state":
			d, err := p1Digest(nil, "c", pr.model)
			if err != nil {
				reply("err " + err.Error())
				return
			}
			got, err := p1Digest(db, "c", nil)
			if err != nil || got != d {
				reply(fmt.Sprintf("err the child's own view: %v", err))
				return
			}
			names := strings.Join(p1Sorted(pr.model), ",")
			if names == "" {
				names = "-"
			}
			reply(fmt.Sprintf("state %s %d %s", d, pr.schemas, names))
		case "quit":
			reply("bye")
			return
		}
	}
}

// TestPage1Prop_MutationIsCaught: with the header comparison left out of the
// page-1 decision, the property test fails, in one process and in two.
func TestPage1Prop_MutationIsCaught(t *testing.T) {
	if os.Getenv(p1PropMutateEnv) != "" || os.Getenv(p1PropChildEnv) != "" {
		t.Skip("inside a mutation run")
	}
	skipInLegacyFormatSuite(t)
	for _, variant := range []string{"in-process/codec=false/seed=1", "multi-process/codec=false/seed=1"} {
		t.Run(strings.ReplaceAll(variant, "/", "_"), func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestPage1Prop_Lockstep$/^"+strings.ReplaceAll(variant, "/", "$/^")+"$", "-test.timeout=300s")
			cmd.Env = append(os.Environ(), p1PropMutateEnv+"=1")
			out, err := cmd.CombinedOutput()
			require.Error(t, err, "the mutated rule went unnoticed:\n%s", out)
			require.NotContains(t, string(out), "no tests to run")
			t.Logf("caught: %s", p1FirstFailure(out))
		})
	}
}

func p1FirstFailure(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "Error:") || strings.Contains(line, "panic:") || strings.Contains(line, "invariant") {
			return strings.TrimSpace(line)
		}
	}
	return "(see output)"
}
