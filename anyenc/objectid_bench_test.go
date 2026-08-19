package anyenc

import "testing"

func BenchmarkObjectID_New(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkOID = NewObjectID()
	}
}

func BenchmarkObjectID_Marshal(b *testing.B) {
	a := &Arena{}
	v := a.NewObjectID(NewObjectID())
	dst := make([]byte, 0, 1+objectIDLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkBytes = v.MarshalTo(dst[:0])
	}
}

func BenchmarkObjectID_Parse(b *testing.B) {
	a := &Arena{}
	enc := a.NewObjectID(NewObjectID()).MarshalTo(nil)
	p := &Parser{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkVal, _ = p.ParseOwned(enc)
	}
}

func BenchmarkObjectID_Accessor(b *testing.B) {
	a := &Arena{}
	v := a.NewObjectID(NewObjectID())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkOID, _ = v.ObjectID()
	}
}

// RoundTrip mirrors the hot document path: build via a pooled arena, marshal,
// then parse via a pooled parser — the amortized-zero-alloc steady state.
func BenchmarkObjectID_RoundTrip(b *testing.B) {
	a := &Arena{}
	p := &Parser{}
	id := NewObjectID()
	dst := make([]byte, 0, 1+objectIDLen)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		dst = a.NewObjectID(id).MarshalTo(dst[:0])
		sinkVal, _ = p.ParseOwned(dst)
	}
}

func BenchmarkObjectID_Hex(b *testing.B) {
	id := NewObjectID()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkHex = id.Hex()
	}
}

func BenchmarkObjectID_FromHex(b *testing.B) {
	h := NewObjectID().Hex()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkOID, _ = ObjectIDFromHex(h)
	}
}

var sinkHex string
