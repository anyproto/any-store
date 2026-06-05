package vector

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// This file builds a *realistic* benchmark dataset instead of uniform-random
// vectors (which are the pathological worst case for HNSW). It generates
// synthetic markdown documents with topic structure and embeds them with the
// feature-hashing trick (a deterministic, offline stand-in for a real embedding
// model — like scikit-learn's HashingVectorizer). Documents that share a topic
// share vocabulary, so their vectors cluster — the geometry real sentence/doc
// embeddings have, where HNSW recall is high and representative.

// 12 topics, each with distinctive vocabulary; plus a shared "common" vocab.
var mdTopics = [][]string{
	{"kubernetes", "container", "pod", "cluster", "deployment", "ingress", "namespace", "helm", "node", "scaling", "sidecar", "operator"},
	{"invoice", "payment", "ledger", "accrual", "balance", "revenue", "expense", "tax", "audit", "receipt", "payable", "reconcile"},
	{"protein", "enzyme", "cellular", "genome", "mitochondria", "synthesis", "membrane", "molecular", "receptor", "peptide", "assay", "vivo"},
	{"glacier", "moraine", "sediment", "tectonic", "basalt", "erosion", "volcanic", "strata", "fossil", "quartz", "magma", "crust"},
	{"sonata", "concerto", "timbre", "counterpoint", "cadence", "octave", "tempo", "orchestral", "dissonance", "fugue", "vibrato", "libretto"},
	{"midfielder", "striker", "offside", "penalty", "dribble", "fixture", "tackle", "formation", "keeper", "winger", "aggregate", "relegation"},
	{"espresso", "fermentation", "marinade", "umami", "braise", "sourdough", "caramelize", "emulsion", "saute", "brisket", "garnish", "proofing"},
	{"orbit", "telescope", "nebula", "redshift", "exoplanet", "asteroid", "luminosity", "spectroscopy", "gravity", "quasar", "parsec", "corona"},
	{"plaintiff", "tort", "statute", "appellate", "deposition", "injunction", "liability", "precedent", "litigation", "subpoena", "damages", "counsel"},
	{"watercolor", "impasto", "chiaroscuro", "gouache", "perspective", "pigment", "fresco", "etching", "palette", "composition", "canvas", "gesso"},
	{"turbine", "torque", "bearing", "actuator", "hydraulic", "flange", "tolerance", "alloy", "fatigue", "weld", "bushing", "camshaft"},
	{"monsoon", "isobar", "cyclone", "humidity", "troposphere", "precipitation", "barometric", "albedo", "convection", "jetstream", "dewpoint", "front"},
}

var mdCommon = strings.Fields(`the a of to and in is for on with this that we can will it as are be by from or an at use also into each more most some many such how when which note example value system process result based using over after before between during`)

// genMarkdownDoc produces a markdown document with topic structure, returns the
// rendered markdown (for size accounting), its content tokens (for embedding),
// and the topic id.
func genMarkdownDoc(rng *rand.Rand) (md string, tokens []string, topic int) {
	topic = rng.Intn(len(mdTopics))
	tv := mdTopics[topic]
	nTokens := 90 + rng.Intn(340) // ~90..430 content words

	word := func() string {
		r := rng.Float64()
		switch {
		case r < 0.55: // topic vocabulary dominates
			return tv[rng.Intn(len(tv))]
		case r < 0.85:
			return mdCommon[rng.Intn(len(mdCommon))]
		default: // a little cross-topic noise (vocabulary overlap)
			ot := mdTopics[rng.Intn(len(mdTopics))]
			return ot[rng.Intn(len(ot))]
		}
	}

	tokens = make([]string, 0, nTokens)
	var b strings.Builder

	title := []string{strings.Title(tv[rng.Intn(len(tv))]), tv[rng.Intn(len(tv))], "notes"}
	fmt.Fprintf(&b, "# %s\n\n", strings.Join(title, " "))
	tokens = append(tokens, tv[rng.Intn(len(tv))], tv[rng.Intn(len(tv))])

	produced := 0
	for produced < nTokens {
		// section heading
		fmt.Fprintf(&b, "## %s %s\n\n", strings.Title(word()), word())
		nSec := 40 + rng.Intn(80)
		bullet := rng.Float64() < 0.35
		if bullet {
			nItems := 3 + rng.Intn(4)
			for i := 0; i < nItems && produced < nTokens; i++ {
				b.WriteString("- ")
				for w := 0; w < 4+rng.Intn(6); w++ {
					t := word()
					tokens = append(tokens, t)
					b.WriteString(t)
					b.WriteByte(' ')
					produced++
				}
				b.WriteByte('\n')
			}
			b.WriteByte('\n')
		} else {
			// paragraph of sentences
			for s := 0; s < nSec && produced < nTokens; {
				slen := 8 + rng.Intn(12)
				for w := 0; w < slen && produced < nTokens; w++ {
					t := word()
					tokens = append(tokens, t)
					if w == 0 {
						b.WriteString(strings.Title(t))
					} else {
						b.WriteString(t)
					}
					b.WriteByte(' ')
					produced++
					s++
				}
				b.WriteString(". ")
			}
			b.WriteString("\n\n")
		}
	}
	return b.String(), tokens, topic
}

// embedTokens turns content tokens into a unit-norm vector via the signed
// feature-hashing trick: each token hashes to a dimension and a sign, weights
// accumulate, then the vector is L2-normalised (cosine geometry).
func embedTokens(tokens []string, dim int) []float32 {
	v := make([]float32, dim)
	for _, t := range tokens {
		h := fnv1a(t)
		idx := h % uint32(dim)
		if h&0x10000 == 0 {
			v[idx] += 1
		} else {
			v[idx] -= 1
		}
	}
	var norm float32
	for _, x := range v {
		norm += x * x
	}
	if norm > 0 {
		inv := float32(1 / math.Sqrt(float64(norm)))
		for i := range v {
			v[i] *= inv
		}
	}
	return v
}

func fnv1a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// genMDDataset builds n documents, returning their embeddings, doc ids, topic
// labels, and the average rendered markdown size in bytes.
func genMDDataset(n, dim int, seed int64) (vecs [][]float32, ids []uint64, topics []int, avgBytes float64) {
	rng := rand.New(rand.NewSource(seed))
	vecs = make([][]float32, n)
	ids = make([]uint64, n)
	topics = make([]int, n)
	var totalBytes int64
	for i := 0; i < n; i++ {
		md, tokens, topic := genMarkdownDoc(rng)
		vecs[i] = embedTokens(tokens, dim)
		ids[i] = uint64(i + 1)
		topics[i] = topic
		totalBytes += int64(len(md))
	}
	return vecs, ids, topics, float64(totalBytes) / float64(n)
}

// TestMDDataset is the realistic-scale capstone: 50–100k topic-clustered
// markdown-document embeddings, run through the in-memory arena, the paged
// (Option B) index, and the route-in-RAM/rerank hybrid.
func TestMDDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("realistic dataset is large; skipped in -short")
	}
	const (
		n   = 75000 // bump to 100000 for the top of the range
		dim = 768   // representative doc-embedding width (e5/bge-base class)
		k   = 10
	)

	t0 := time.Now()
	vecs, ids, _, avgBytes := genMDDataset(n, dim, 2024)
	queries, _, _, _ := genMDDataset(2000, dim, 7) // held-out, same distribution
	t.Logf("generated %d markdown docs (avg %.0f bytes/doc) + embedded dim=%d in %s",
		n, avgBytes, dim, time.Since(t0).Round(time.Millisecond))
	t.Logf("raw vectors: %d x %d x 4 = %.1f MiB; markdown corpus ~%.0f MiB",
		n, dim, float64(n*dim*4)/(1<<20), float64(n)*avgBytes/(1<<20))

	// build in-memory arena (cosine)
	tb := time.Now()
	g := NewFlatHNSW(dim, Cosine, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(ids[i], vecs[i])
	}
	t.Logf("build flat HNSW (in-mem): %s  (%.0f docs/s)", time.Since(tb).Round(time.Millisecond), float64(n)/time.Since(tb).Seconds())

	// recall ground truth (brute, cosine) over a query subset, swept over ef to
	// show the recall<->latency knob (MongoDB's numCandidates / our EfSearch).
	const qRecall = 200
	brute := NewBrute(dim, Cosine)
	for i := range vecs {
		brute.Add(ids[i], vecs[i])
	}
	truth := make([][]SearchResult, qRecall)
	for i := 0; i < qRecall; i++ {
		truth[i] = brute.Search(queries[i], k)
	}
	for _, ef := range []int{64, 128, 256} {
		g.EfSearch = ef
		var recall float64
		s := time.Now()
		for i := 0; i < qRecall; i++ {
			recall += recallAt(g.Search(queries[i], k), truth[i])
		}
		us := float64(time.Since(s).Microseconds()) / qRecall
		t.Logf("recall@%d ef=%-3d: %.3f   (%.0f us/q in-mem)", k, ef, recall/qRecall, us)
	}
	g.EfSearch = 64 // restore for the latency table below

	// latency: in-memory
	timeSearch := func(fn func(q []float32)) float64 {
		for i := 0; i < 200; i++ {
			fn(queries[i%len(queries)])
		}
		const iters = 3000
		s := time.Now()
		for i := 0; i < iters; i++ {
			fn(queries[i%len(queries)])
		}
		return float64(time.Since(s).Nanoseconds()) / iters
	}
	memNs := timeSearch(func(q []float32) { g.Search(q, k) })

	// paged + hybrid (Option B) over an in-memory btree
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p, err := BuildPagedFromFlat(g, db, "emb", Cosine)
	if err != nil {
		t.Fatal(err)
	}
	g0 := p.gets
	var c1 int
	pagNs := timeSearchN(func(q []float32) { p.Search(q, k); c1++ }, queries, 800)
	pagGets := float64(p.gets-g0) / float64(c1)

	g0 = p.gets
	var c2 int
	hybNs := timeSearchN(func(q []float32) { p.SearchHybrid(q, k); c2++ }, queries, 800)
	hybGets := float64(p.gets-g0) / float64(c2)

	fullMiB := float64(g.MemBytes()) / (1 << 20)
	topoMiB := float64(p.TopologyBytes()) / (1 << 20)
	t.Logf("--- search (k=%d, ef=64) ---", k)
	t.Logf("A in-memory arena : %8.1f us/q   RAM=%6.1f MiB (full)            0 gets/q", memNs/1000, fullMiB)
	t.Logf("B paged vectors   : %8.1f us/q   RAM=%6.1f MiB (topology only) %4.0f gets/q  (%.1fx)", pagNs/1000, topoMiB, pagGets, pagNs/memNs)
	t.Logf("B' hybrid         : %8.1f us/q   RAM=routing-slab + topology   %4.0f gets/q  (%.1fx)", hybNs/1000, hybGets, hybNs/memNs)
	t.Logf("doc-id mapping (DocFlatHNSW []byte ids) adds ~90 B/id => ~%.1f MiB at %d docs", 90*float64(n)/(1<<20), n)
}

func timeSearchN(fn func(q []float32), queries [][]float32, iters int) float64 {
	for i := 0; i < 100; i++ {
		fn(queries[i%len(queries)])
	}
	s := time.Now()
	for i := 0; i < iters; i++ {
		fn(queries[i%len(queries)])
	}
	return float64(time.Since(s).Nanoseconds()) / float64(iters)
}
