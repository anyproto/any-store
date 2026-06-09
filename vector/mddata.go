package vector

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
)

// mddata.go builds a *realistic* benchmark dataset instead of uniform-random
// vectors (which are the pathological worst case for HNSW). It generates
// synthetic markdown documents with topic structure and embeds them with the
// feature-hashing trick — a deterministic, offline stand-in for a real embedding
// model (scikit-learn's HashingVectorizer). Documents that share a topic share
// vocabulary, so their vectors cluster: the geometry real sentence/document
// embeddings have, where HNSW recall is high and representative.
//
// It lives in a non-test file so both the test suite and the standalone
// cmd/vectorbench binary can use it.

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

// GenMDDataset builds n synthetic markdown documents, returning their
// embeddings, sequential doc ids, topic labels, and the average rendered
// markdown size in bytes.
func GenMDDataset(n, dim int, seed int64) (vecs [][]float32, ids []uint64, topics []int, avgBytes float64) {
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
