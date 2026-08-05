package aggregate

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func evalExprOn(t *testing.T, exprJson string, doc *anyenc.Value) *anyenc.Value {
	t.Helper()
	e, err := ParseExpr(anyenc.MustParseJson(exprJson))
	require.NoError(t, err)
	v, err := e.Eval(&anyenc.Arena{}, doc)
	require.NoError(t, err)
	return v
}

func TestArithExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 10, "b": 3, "s": "str", "z": 0, "nul": null}`)
	eval := func(t *testing.T, exprJson string) *anyenc.Value {
		return evalExprOn(t, exprJson, doc)
	}
	num := func(t *testing.T, exprJson string) float64 {
		v := eval(t, exprJson)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return v.GetFloat64()
	}
	null := func(t *testing.T, exprJson string) {
		v := eval(t, exprJson)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("nested composition", func(t *testing.T) {
		assert.Equal(t, float64(17), num(t, `{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`))
	})
	t.Run("variadic and shorthand", func(t *testing.T) {
		assert.Equal(t, float64(24), num(t, `{"$multiply": [2, 3, 4]}`))
		assert.Equal(t, float64(10), num(t, `{"$add": "$a"}`))
		assert.Equal(t, float64(5), num(t, `{"$multiply": [5]}`))
	})
	t.Run("empty operand list yields the identity", func(t *testing.T) {
		assert.Equal(t, float64(0), num(t, `{"$add": []}`))
		assert.Equal(t, float64(1), num(t, `{"$multiply": []}`))
	})
	t.Run("subtract and divide", func(t *testing.T) {
		assert.Equal(t, float64(7), num(t, `{"$subtract": ["$a", "$b"]}`))
		assert.Equal(t, float64(2.5), num(t, `{"$divide": ["$a", 4]}`))
	})
	t.Run("divide by zero", func(t *testing.T) {
		null(t, `{"$divide": ["$a", 0]}`)
		null(t, `{"$divide": ["$a", "$z"]}`)
	})
	t.Run("null and missing propagate", func(t *testing.T) {
		null(t, `{"$add": ["$nope", 1]}`)
		null(t, `{"$add": ["$nul", 1]}`)
		null(t, `{"$subtract": [{"$literal": null}, 1]}`)
	})
	t.Run("non-finite result", func(t *testing.T) {
		null(t, `{"$multiply": [1e308, 10]}`)
		null(t, `{"$divide": [1e308, 1e-308]}`)
		// The overflowing nested expr is already null; it propagates.
		null(t, `{"$subtract": [{"$multiply": [1e308, 10]}, 1]}`)
	})
	t.Run("non-numeric operand", func(t *testing.T) {
		null(t, `{"$add": ["$s", 1]}`)
		null(t, `{"$multiply": [true, 2]}`)
	})
}

// TestArithDateEval pins date arithmetic in $add/$subtract: one dateTime
// among numbers shifts by their millis sum; [date,date] subtraction is a
// millis number; anything else with a date is null.
func TestArithDateEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"d1": {"$date": 1000}, "d2": {"$date": 2000}, "s": "x", "n": 5}`)
	date := func(t *testing.T, exprJson string, wantMs int64) {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeDateTime, v.Type(), exprJson)
		ms, err := v.DateTimeMillis()
		require.NoError(t, err)
		assert.Equal(t, wantMs, ms, exprJson)
	}
	num := func(t *testing.T, exprJson string) float64 {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeNumber, v.Type(), exprJson)
		return v.GetFloat64()
	}
	null := func(t *testing.T, exprJson string) {
		v := evalExprOn(t, exprJson, doc)
		require.NotNil(t, v, exprJson)
		assert.Equal(t, anyenc.TypeNull, v.Type(), exprJson)
	}

	t.Run("$add date plus numbers", func(t *testing.T) {
		date(t, `{"$add": ["$d1", 500]}`, 1500)
		date(t, `{"$add": [250, "$d1", 250]}`, 1500) // position-independent
		date(t, `{"$add": ["$d1"]}`, 1000)
		date(t, `{"$add": ["$d1", 0.9]}`, 1000) // fraction of a milli truncates
	})
	t.Run("$add nests", func(t *testing.T) {
		date(t, `{"$add": [{"$add": ["$d1", 200]}, 300]}`, 1500)
		v := evalExprOn(t, `{"$subtract": [{"$add": ["$d1", 500]}, "$d1"]}`, doc)
		assert.Equal(t, float64(500), v.GetFloat64())
	})
	t.Run("$add two dates is null", func(t *testing.T) {
		null(t, `{"$add": ["$d1", "$d2"]}`)
	})
	t.Run("$subtract date minus date", func(t *testing.T) {
		assert.Equal(t, float64(1000), num(t, `{"$subtract": ["$d2", "$d1"]}`))
		assert.Equal(t, float64(-1000), num(t, `{"$subtract": ["$d1", "$d2"]}`))
	})
	t.Run("$subtract date minus number", func(t *testing.T) {
		date(t, `{"$subtract": ["$d1", 250]}`, 750)
	})
	t.Run("fractional millis truncate toward zero", func(t *testing.T) {
		date(t, `{"$add": ["$d1", -1.9]}`, 999)
		date(t, `{"$subtract": ["$d1", 1.9]}`, 999)
	})
	t.Run("$subtract number minus date is null", func(t *testing.T) {
		null(t, `{"$subtract": [2000, "$d1"]}`)
	})
	t.Run("dates with non-numeric partners are null", func(t *testing.T) {
		null(t, `{"$add": ["$d1", "$s"]}`)
		null(t, `{"$subtract": ["$d1", "$s"]}`)
		null(t, `{"$subtract": ["$d1", "$nope"]}`)
	})
	t.Run("$multiply and $divide stay numeric-only", func(t *testing.T) {
		null(t, `{"$multiply": ["$d1", 2]}`)
		null(t, `{"$divide": ["$d1", 2]}`)
	})
}

// mustUTC parses an RFC3339 timestamp for date-operator expectations.
func mustUTC(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return tm.UTC()
}

// evalDate evaluates exprJson expecting a dateTime and returns it as UTC.
func evalDate(t *testing.T, exprJson string, doc *anyenc.Value) time.Time {
	t.Helper()
	v := evalExprOn(t, exprJson, doc)
	require.Equal(t, anyenc.TypeDateTime, v.Type(), exprJson)
	ms, err := v.DateTimeMillis()
	require.NoError(t, err)
	return time.UnixMilli(ms).UTC()
}

func evalNull(t *testing.T, exprJson string, doc *anyenc.Value) {
	t.Helper()
	v := evalExprOn(t, exprJson, doc)
	require.NotNil(t, v, exprJson)
	assert.Equal(t, anyenc.TypeNull, v.Type(), exprJson)
}

func TestDateAddExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{
		"jan31":  {"$date": "2026-01-31T10:30:00Z"},
		"leap":   {"$date": "2024-01-31T00:00:00Z"},
		"feb29":  {"$date": "2024-02-29T12:00:00Z"},
		"preDst": {"$date": "2026-03-28T12:00:00Z"},
		"preFall":  {"$date": "2026-10-24T10:00:00Z"},
		"postFall": {"$date": "2026-10-25T11:00:00Z"},
		"amb":    {"$date": "2026-10-24T00:30:00Z"},
		"mid":    {"$date": "2026-03-14T23:30:00Z"},
		"apollo": {"$date": "1969-07-20T20:17:00Z"},
		"mar31":  {"$date": "2026-03-31T00:00:00Z"},
		"n": 5, "s": "x", "nul": null}`)
	date := func(t *testing.T, exprJson, want string) {
		assert.Equal(t, mustUTC(t, want), evalDate(t, exprJson, doc), exprJson)
	}

	t.Run("fixed-span units", func(t *testing.T) {
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "hour", "amount": 5}}`,
			"2026-03-28T17:00:00Z")
		date(t, `{"$dateAdd": {"startDate": "$apollo", "unit": "week", "amount": 2}}`,
			"1969-08-03T20:17:00Z")
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "millisecond", "amount": 1500}}`,
			"2026-03-28T12:00:01.5Z")
	})
	t.Run("day and week are DST-aware in the operative timezone", func(t *testing.T) {
		// Berlin springs forward 2026-03-29 02:00 (a 23h day) and falls back
		// 2026-10-25 03:00 (a 25h day): +1 day preserves the local clock.
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "day", "amount": 1, "timezone": "Europe/Berlin"}}`,
			"2026-03-29T11:00:00Z") // local 13:00 → 13:00, 23 absolute hours
		date(t, `{"$dateAdd": {"startDate": "$preFall", "unit": "day", "amount": 1, "timezone": "Europe/Berlin"}}`,
			"2026-10-25T11:00:00Z") // local 12:00 → 12:00, 25 absolute hours
		date(t, `{"$dateAdd": {"startDate": "$postFall", "unit": "day", "amount": -1, "timezone": "Europe/Berlin"}}`,
			"2026-10-24T10:00:00Z") // negative amounts walk back DST-aware too
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "week", "amount": -1, "timezone": "Europe/Berlin"}}`,
			"2026-03-21T12:00:00Z")
		// Without a timezone the UTC day is a plain 24h span.
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "day", "amount": 1}}`,
			"2026-03-29T12:00:00Z")
	})
	t.Run("day landing on a repeated wall time takes the earlier pass", func(t *testing.T) {
		// Oct 24 02:30 CEST + 1 day is Oct 25 02:30, which occurs twice;
		// the earlier (CEST) instant is exactly 24h later.
		date(t, `{"$dateAdd": {"startDate": "$amb", "unit": "day", "amount": 1, "timezone": "Europe/Berlin"}}`,
			"2026-10-25T00:30:00Z")
	})
	t.Run("month clamps to the last day", func(t *testing.T) {
		// time.AddDate would normalize Jan 31 + 1 month to Mar 3.
		date(t, `{"$dateAdd": {"startDate": "$jan31", "unit": "month", "amount": 1}}`,
			"2026-02-28T10:30:00Z")
		date(t, `{"$dateAdd": {"startDate": "$leap", "unit": "month", "amount": 1}}`,
			"2024-02-29T00:00:00Z") // leap year keeps the 29th
	})
	t.Run("quarter and year clamp too", func(t *testing.T) {
		date(t, `{"$dateAdd": {"startDate": "$jan31", "unit": "quarter", "amount": 1}}`,
			"2026-04-30T10:30:00Z")
		date(t, `{"$dateAdd": {"startDate": "$feb29", "unit": "year", "amount": 1}}`,
			"2025-02-28T12:00:00Z")
	})
	t.Run("calendar math happens in the operative timezone", func(t *testing.T) {
		// 2026-03-14T23:30Z is already March 15 in Berlin; +1 month lands on
		// local April 15 00:30 CEST = 2026-04-14T22:30Z.
		date(t, `{"$dateAdd": {"startDate": "$mid", "unit": "month", "amount": 1, "timezone": "Europe/Berlin"}}`,
			"2026-04-14T22:30:00Z")
		date(t, `{"$dateAdd": {"startDate": "$mid", "unit": "month", "amount": 1}}`,
			"2026-04-14T23:30:00Z") // default UTC
	})
	t.Run("negative amounts", func(t *testing.T) {
		date(t, `{"$dateAdd": {"startDate": "$mar31", "unit": "month", "amount": -1}}`,
			"2026-02-28T00:00:00Z")
		date(t, `{"$dateAdd": {"startDate": "$apollo", "unit": "day", "amount": -1}}`,
			"1969-07-19T20:17:00Z")
	})
	t.Run("amount is an expression", func(t *testing.T) {
		date(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "day", "amount": {"$add": [1, 1]}}}`,
			"2026-03-30T12:00:00Z")
	})
	t.Run("null on faults", func(t *testing.T) {
		evalNull(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "day", "amount": 1.5}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$nope", "unit": "day", "amount": 1}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$nul", "unit": "day", "amount": 1}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$s", "unit": "day", "amount": 1}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$n", "unit": "day", "amount": 1}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "day", "amount": "$nope"}}`, doc)
		evalNull(t, `{"$dateAdd": {"startDate": "$preDst", "unit": "year", "amount": 1e18}}`, doc)
	})
}

func TestDateDiffExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{
		"a2359": {"$date": "2026-01-01T23:59:00Z"},
		"b0001": {"$date": "2026-01-02T00:01:00Z"},
		"d0":    {"$date": "2026-01-31T00:00:00Z"},
		"d1":    {"$date": "2026-01-31T23:59:00Z"},
		"dstA":  {"$date": "2026-03-28T22:59:00Z"},
		"dstB":  {"$date": "2026-03-28T23:00:00Z"},
		"h1":    {"$date": "2026-03-29T00:30:00Z"},
		"h2":    {"$date": "2026-03-29T01:30:00Z"},
		"feb1":  {"$date": "2026-02-01T00:00:00Z"},
		"sat":   {"$date": "2026-01-03T12:00:00Z"},
		"sun":   {"$date": "2026-01-04T12:00:00Z"},
		"mon":   {"$date": "2026-01-05T12:00:00Z"},
		"pre":   {"$date": "1969-12-31T23:59:00Z"},
		"post":  {"$date": "1970-01-01T00:01:00Z"},
		"q1":    {"$date": "2026-03-31T23:00:00Z"},
		"q2":    {"$date": "2026-04-01T01:00:00Z"},
		"y1":    {"$date": "2025-12-31T00:00:00Z"},
		"y2":    {"$date": "2026-01-01T00:00:00Z"},
		"s": "x"}`)
	diff := func(t *testing.T, start, end, unit, extra string) float64 {
		j := `{"$dateDiff": {"startDate": "` + start + `", "endDate": "` + end + `", "unit": "` + unit + `"` + extra + `}}`
		v := evalExprOn(t, j, doc)
		require.Equal(t, anyenc.TypeNumber, v.Type(), j)
		return v.GetFloat64()
	}

	t.Run("boundary crossings, not elapsed time", func(t *testing.T) {
		assert.Equal(t, float64(1), diff(t, "$a2359", "$b0001", "day", ""))
		assert.Equal(t, float64(0), diff(t, "$d0", "$d1", "day", ""))
		assert.Equal(t, float64(1), diff(t, "$a2359", "$b0001", "hour", ""))
		assert.Equal(t, float64(2), diff(t, "$a2359", "$b0001", "minute", ""))
		assert.Equal(t, float64(120), diff(t, "$a2359", "$b0001", "second", ""))
		assert.Equal(t, float64(120000), diff(t, "$a2359", "$b0001", "millisecond", ""))
	})
	t.Run("negative when start is after end", func(t *testing.T) {
		assert.Equal(t, float64(-1), diff(t, "$b0001", "$a2359", "day", ""))
		assert.Equal(t, float64(-1), diff(t, "$feb1", "$d1", "month", ""))
	})
	t.Run("day boundaries are local midnights", func(t *testing.T) {
		// dstA/dstB straddle midnight in Berlin but not in UTC.
		assert.Equal(t, float64(1), diff(t, "$dstA", "$dstB", "day", `, "timezone": "Europe/Berlin"`))
		assert.Equal(t, float64(0), diff(t, "$dstA", "$dstB", "day", ""))
	})
	t.Run("hour boundaries are absolute across DST", func(t *testing.T) {
		// Berlin local 01:30 CET → 03:30 CEST spans one absolute hour.
		assert.Equal(t, float64(1), diff(t, "$h1", "$h2", "hour", `, "timezone": "Europe/Berlin"`))
		// Local calendar days still count once: Mar 28 23:59 → Mar 29 03:30.
		assert.Equal(t, float64(1), diff(t, "$dstA", "$h2", "day", `, "timezone": "Europe/Berlin"`))
	})
	t.Run("month, quarter, year", func(t *testing.T) {
		assert.Equal(t, float64(1), diff(t, "$d1", "$feb1", "month", ""))
		assert.Equal(t, float64(1), diff(t, "$q1", "$q2", "quarter", ""))
		// At +02:00 both instants are April 1 local: same quarter.
		assert.Equal(t, float64(0), diff(t, "$q1", "$q2", "quarter", `, "timezone": "+02:00"`))
		assert.Equal(t, float64(1), diff(t, "$y1", "$y2", "year", ""))
	})
	t.Run("week respects startOfWeek", func(t *testing.T) {
		assert.Equal(t, float64(1), diff(t, "$sat", "$sun", "week", ""))
		assert.Equal(t, float64(0), diff(t, "$sun", "$mon", "week", ""))
		assert.Equal(t, float64(0), diff(t, "$sat", "$sun", "week", `, "startOfWeek": "monday"`))
		assert.Equal(t, float64(1), diff(t, "$sun", "$mon", "week", `, "startOfWeek": "MONDAY"`))
	})
	t.Run("pre-1970", func(t *testing.T) {
		assert.Equal(t, float64(1), diff(t, "$pre", "$post", "day", ""))
		assert.Equal(t, float64(120000), diff(t, "$pre", "$post", "millisecond", ""))
	})
	t.Run("null on faults", func(t *testing.T) {
		evalNull(t, `{"$dateDiff": {"startDate": "$nope", "endDate": "$y2", "unit": "day"}}`, doc)
		evalNull(t, `{"$dateDiff": {"startDate": "$y1", "endDate": "$s", "unit": "day"}}`, doc)
		evalNull(t, `{"$dateDiff": {"startDate": "$y1", "endDate": 5, "unit": "day"}}`, doc)
	})
}

func TestDateTruncExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{
		"d":     {"$date": "2026-08-05T17:45:30.123Z"},
		"jan20": {"$date": "2000-01-20T05:00:00Z"},
		"tue":   {"$date": "2026-01-06T12:00:00Z"},
		"h":     {"$date": "2000-01-01T13:30:00Z"},
		"tzh":   {"$date": "2026-01-01T10:00:00Z"},
		"dst":   {"$date": "2026-03-29T15:00:00Z"},
		"fb1":   {"$date": "2026-10-25T00:30:00Z"},
		"fb2":   {"$date": "2026-10-25T01:30:00Z"},
		"fbm1":  {"$date": "2026-10-25T00:30:30Z"},
		"fbm2":  {"$date": "2026-10-25T01:30:30Z"},
		"sf":    {"$date": "2026-03-29T02:30:00Z"},
		"pre":   {"$date": "1999-12-31T00:00:00Z"},
		"pre70": {"$date": "1969-07-20T20:17:00Z"},
		"s": "x"}`)
	trunc := func(t *testing.T, params, want string) {
		j := `{"$dateTrunc": {` + params + `}}`
		assert.Equal(t, mustUTC(t, want), evalDate(t, j, doc), j)
	}

	t.Run("component truncation", func(t *testing.T) {
		trunc(t, `"date": "$d", "unit": "millisecond"`, "2026-08-05T17:45:30.123Z") // identity
		trunc(t, `"date": "$d", "unit": "second"`, "2026-08-05T17:45:30Z")
		trunc(t, `"date": "$d", "unit": "minute"`, "2026-08-05T17:45:00Z")
		trunc(t, `"date": "$d", "unit": "hour"`, "2026-08-05T17:00:00Z")
		trunc(t, `"date": "$d", "unit": "day"`, "2026-08-05T00:00:00Z")
		trunc(t, `"date": "$d", "unit": "month"`, "2026-08-01T00:00:00Z")
		trunc(t, `"date": "$d", "unit": "quarter"`, "2026-07-01T00:00:00Z")
		trunc(t, `"date": "$d", "unit": "year"`, "2026-01-01T00:00:00Z")
	})
	t.Run("day bins anchor at 2000-01-01", func(t *testing.T) {
		trunc(t, `"date": "$jan20", "unit": "day", "binSize": 14`, "2000-01-15T00:00:00Z")
		trunc(t, `"date": "$pre", "unit": "day", "binSize": 14`, "1999-12-18T00:00:00Z")
		trunc(t, `"date": "$pre70", "unit": "day", "binSize": 14`, "1969-07-12T00:00:00Z")
	})
	t.Run("week anchors at the first startOfWeek on or after 2000-01-01", func(t *testing.T) {
		trunc(t, `"date": "$tue", "unit": "week"`, "2026-01-04T00:00:00Z") // Sunday
		trunc(t, `"date": "$tue", "unit": "week", "startOfWeek": "monday"`, "2026-01-05T00:00:00Z")
		trunc(t, `"date": "$tue", "unit": "week", "binSize": 2, "startOfWeek": "monday"`,
			"2025-12-29T00:00:00Z")
	})
	t.Run("sub-day bins anchor at 2000-01-01, not the epoch", func(t *testing.T) {
		// An epoch anchor would put 13:30 into the 12:00 bin.
		trunc(t, `"date": "$h", "unit": "hour", "binSize": 5`, "2000-01-01T10:00:00Z")
	})
	t.Run("sub-day truncation uses the local wall clock", func(t *testing.T) {
		// 2026-01-01T10:00Z is 15:30 at +05:30; the local hour starts 15:00.
		trunc(t, `"date": "$tzh", "unit": "hour", "timezone": "+05:30"`, "2026-01-01T09:30:00Z")
	})
	t.Run("day truncation lands on the local midnight", func(t *testing.T) {
		trunc(t, `"date": "$dst", "unit": "day", "timezone": "Europe/Berlin"`, "2026-03-28T23:00:00Z")
	})
	t.Run("fall-back repeated hour truncates within each pass", func(t *testing.T) {
		// Berlin falls back 2026-10-25 03:00 CEST → 02:00 CET: local 02:30
		// occurs twice, and each instant truncates to its own pass's 02:00.
		trunc(t, `"date": "$fb1", "unit": "hour", "timezone": "Europe/Berlin"`, "2026-10-25T00:00:00Z")
		trunc(t, `"date": "$fb2", "unit": "hour", "timezone": "Europe/Berlin"`, "2026-10-25T01:00:00Z")
		trunc(t, `"date": "$fbm1", "unit": "minute", "timezone": "Europe/Berlin"`, "2026-10-25T00:30:00Z")
		trunc(t, `"date": "$fbm2", "unit": "minute", "timezone": "Europe/Berlin"`, "2026-10-25T01:30:00Z")
	})
	t.Run("spring-forward with a multi-hour bin", func(t *testing.T) {
		// Local 04:30 CEST sits in the wall-clock 04:00 2h-bin.
		trunc(t, `"date": "$sf", "unit": "hour", "binSize": 2, "timezone": "Europe/Berlin"`,
			"2026-03-29T02:00:00Z")
	})
	t.Run("monotone and idempotent across DST transitions", func(t *testing.T) {
		a := &anyenc.Arena{}
		for _, unit := range []string{"hour", "minute"} {
			e := mustExpr(t, `{"$dateTrunc": {"date": "$d", "unit": "`+unit+`", "timezone": "Europe/Berlin"}}`)
			truncMs := func(ms int64) int64 {
				a.Reset()
				d := a.NewObject()
				d.Set("d", a.NewDateTimeMillis(ms))
				v, err := e.Eval(a, d)
				require.NoError(t, err)
				got, err := v.DateTimeMillis()
				require.NoError(t, err)
				return got
			}
			for _, w := range [][2]string{
				{"2026-03-28T23:00:00Z", "2026-03-29T04:00:00Z"}, // spring forward
				{"2026-10-24T22:00:00Z", "2026-10-25T04:00:00Z"}, // fall back
			} {
				prev := int64(math.MinInt64)
				for ms := mustUTC(t, w[0]).UnixMilli(); ms <= mustUTC(t, w[1]).UnixMilli(); ms += 600000 {
					got := truncMs(ms)
					require.LessOrEqual(t, got, ms, "must not move forward: %s at %d", unit, ms)
					require.GreaterOrEqual(t, got, prev, "must be monotone: %s at %d", unit, ms)
					require.Equal(t, got, truncMs(got), "must be idempotent: %s at %d", unit, ms)
					prev = got
				}
			}
		}
	})
	t.Run("calendar bins", func(t *testing.T) {
		trunc(t, `"date": "$d", "unit": "year", "binSize": 10`, "2020-01-01T00:00:00Z")
		trunc(t, `"date": "$d", "unit": "quarter", "binSize": 3`, "2026-04-01T00:00:00Z")
		trunc(t, `"date": "$pre70", "unit": "month"`, "1969-07-01T00:00:00Z")
	})
	t.Run("null on faults", func(t *testing.T) {
		evalNull(t, `{"$dateTrunc": {"date": "$nope", "unit": "day"}}`, doc)
		evalNull(t, `{"$dateTrunc": {"date": "$s", "unit": "day"}}`, doc)
		evalNull(t, `{"$dateTrunc": {"date": 5, "unit": "day"}}`, doc)
	})
}

func TestYearWeekExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{
		"sun23":  {"$date": "2023-01-01T12:00:00Z"},
		"mon24":  {"$date": "2024-01-01T12:00:00Z"},
		"thu26":  {"$date": "2026-01-01T12:00:00Z"},
		"dec22":  {"$date": "2022-12-31T12:00:00Z"},
		"dec23":  {"$date": "2023-12-31T12:00:00Z"},
		"nye":    {"$date": "2025-12-31T23:30:00Z"},
		"apollo": {"$date": "1969-07-20T20:17:00Z"},
		"s": "x"}`)
	num := func(t *testing.T, exprJson string) float64 {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeNumber, v.Type(), exprJson)
		return v.GetFloat64()
	}

	t.Run("$year", func(t *testing.T) {
		assert.Equal(t, float64(2023), num(t, `{"$year": "$sun23"}`))
		assert.Equal(t, float64(1969), num(t, `{"$year": "$apollo"}`))
		assert.Equal(t, float64(2025), num(t, `{"$year": "$nye"}`))
	})
	t.Run("$year object form with timezone", func(t *testing.T) {
		assert.Equal(t, float64(2026), num(t, `{"$year": {"date": "$nye", "timezone": "+01:00"}}`))
	})
	t.Run("$week is Sunday-based 0-53", func(t *testing.T) {
		assert.Equal(t, float64(1), num(t, `{"$week": "$sun23"}`)) // Jan 1 on a Sunday
		assert.Equal(t, float64(0), num(t, `{"$week": "$mon24"}`)) // Jan 1 on a Monday
		assert.Equal(t, float64(0), num(t, `{"$week": "$thu26"}`))
		assert.Equal(t, float64(52), num(t, `{"$week": "$dec22"}`))
		assert.Equal(t, float64(53), num(t, `{"$week": "$dec23"}`)) // Sunday Dec 31 after a Sunday Jan 1
		assert.Equal(t, float64(29), num(t, `{"$week": "$apollo"}`))
	})
	t.Run("$week object form with timezone", func(t *testing.T) {
		// At +01:00 the instant is already 2026-01-01: week 0 of the new year.
		assert.Equal(t, float64(0), num(t, `{"$week": {"date": "$nye", "timezone": "+01:00"}}`))
		assert.Equal(t, float64(52), num(t, `{"$week": "$nye"}`))
	})
	t.Run("null on faults", func(t *testing.T) {
		evalNull(t, `{"$year": "$nope"}`, doc)
		evalNull(t, `{"$year": "$s"}`, doc)
		evalNull(t, `{"$week": {"date": "$s", "timezone": "+01:00"}}`, doc)
		evalNull(t, `{"$week": [5]}`, doc)
	})
}

func TestRoundExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"x": 3.7}`)
	num := func(t *testing.T, exprJson string) float64 {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return v.GetFloat64()
	}
	null := func(t *testing.T, exprJson string) {
		v := evalExprOn(t, exprJson, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("half to even", func(t *testing.T) {
		assert.Equal(t, float64(2), num(t, `{"$round": [1.5]}`))
		assert.Equal(t, float64(2), num(t, `{"$round": [2.5]}`))
		assert.Equal(t, float64(-2), num(t, `{"$round": [-1.5]}`))
		assert.Equal(t, float64(0), num(t, `{"$round": [0.5]}`))
	})
	t.Run("positive place", func(t *testing.T) {
		// 2.345's double (2.3450000000000002) sits above the midpoint: rounds up.
		assert.Equal(t, 2.35, num(t, `{"$round": [2.345, 2]}`))
		// 1.25 scales to exactly 12.5: a true tie, half to even.
		assert.Equal(t, 1.2, num(t, `{"$round": [1.25, 1]}`))
	})
	t.Run("negative place", func(t *testing.T) {
		assert.Equal(t, float64(1230), num(t, `{"$round": [1234.5, -1]}`))
		assert.Equal(t, float64(1200), num(t, `{"$round": [1234.5, -2]}`))
	})
	t.Run("place beyond float64 resolution is identity", func(t *testing.T) {
		assert.Equal(t, 1.5, num(t, `{"$round": [1.5, 100]}`))
	})
	t.Run("single non-array operand", func(t *testing.T) {
		assert.Equal(t, float64(4), num(t, `{"$round": "$x"}`))
	})
	t.Run("invalid place", func(t *testing.T) {
		null(t, `{"$round": [1.5, 1.5]}`)
		null(t, `{"$round": [1.5, 101]}`)
		null(t, `{"$round": [1.5, -21]}`)
		null(t, `{"$round": [1.5, "two"]}`)
	})
	t.Run("null and missing propagate", func(t *testing.T) {
		null(t, `{"$round": ["$nope"]}`)
		null(t, `{"$round": [{"$literal": null}, 2]}`)
	})
}

func TestAbsExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"n": -5, "s": "str"}`)
	t.Run("numeric", func(t *testing.T) {
		assert.Equal(t, float64(5), evalExprOn(t, `{"$abs": "$n"}`, doc).GetFloat64())
		assert.Equal(t, 1.5, evalExprOn(t, `{"$abs": [1.5]}`, doc).GetFloat64())
	})
	t.Run("null, missing, non-numeric", func(t *testing.T) {
		for _, j := range []string{`{"$abs": "$nope"}`, `{"$abs": [{"$literal": null}]}`, `{"$abs": "$s"}`} {
			v := evalExprOn(t, j, doc)
			require.NotNil(t, v, j)
			assert.Equal(t, anyenc.TypeNull, v.Type(), j)
		}
	})
}

func TestConcatExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"s1": "héllo", "s2": "wörld", "n": 5, "nul": null}`)
	str := func(t *testing.T, exprJson string) string {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeString, v.Type())
		return string(v.GetStringBytes())
	}
	null := func(t *testing.T, exprJson string) {
		v := evalExprOn(t, exprJson, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("multibyte and empty strings", func(t *testing.T) {
		assert.Equal(t, "héllo → wörld", str(t, `{"$concat": ["$s1", " → ", "$s2"]}`))
		assert.Equal(t, "ab", str(t, `{"$concat": ["a", "", "b"]}`))
		assert.Equal(t, "", str(t, `{"$concat": [""]}`))
		assert.Equal(t, "", str(t, `{"$concat": []}`))
	})
	t.Run("single non-array operand", func(t *testing.T) {
		assert.Equal(t, "héllo", str(t, `{"$concat": "$s1"}`))
	})
	t.Run("null, missing, non-string", func(t *testing.T) {
		null(t, `{"$concat": ["$s1", "$nul"]}`)
		null(t, `{"$concat": ["$s1", "$nope"]}`)
		null(t, `{"$concat": ["$s1", "$n"]}`)
	})
}

// countingExpr wraps an Expr and counts Eval calls — the structural proof of
// branch laziness for $cond/$switch/$ifNull.
type countingExpr struct {
	inner Expr
	n     int
}

func (c *countingExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	c.n++
	return c.inner.Eval(a, doc)
}

func (c *countingExpr) String() string { return c.inner.String() }

func mustExpr(t *testing.T, exprJson string) Expr {
	t.Helper()
	e, err := ParseExpr(anyenc.MustParseJson(exprJson))
	require.NoError(t, err)
	return e
}

func TestCondExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"z": 0, "f": false, "nul": null, "es": "", "s": "x", "ea": [], "eo": {}, "n": 0.0}`)
	branch := func(t *testing.T, condExpr string) string {
		v := evalExprOn(t, `{"$cond": [`+condExpr+`, "then", "else"]}`, doc)
		require.Equal(t, anyenc.TypeString, v.Type())
		return string(v.GetStringBytes())
	}

	t.Run("truthiness", func(t *testing.T) {
		// Mongo coercion: false, 0, null, missing → false; everything else
		// (including "", [], {}) → true.
		for cond, want := range map[string]string{
			`0`:       "else",
			`"$z"`:    "else",
			`"$n"`:    "else", // 0.0
			`false`:   "else",
			`"$f"`:    "else",
			`"$nul"`:  "else",
			`"$nope"`: "else", // missing
			`""`:      "then",
			`"$es"`:   "then",
			`"x"`:     "then",
			`"$s"`:    "then",
			`"$ea"`:   "then", // []
			`"$eo"`:   "then", // {}
			`[]`:      "then",
			`{}`:      "then",
			`-1`:      "then",
			`true`:    "then",
		} {
			assert.Equal(t, want, branch(t, cond), "cond=%s", cond)
		}
	})
	t.Run("object form", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": {"if": "$s", "then": 1, "else": 2}}`, doc)
		assert.Equal(t, float64(1), v.GetFloat64())
	})
	t.Run("untaken branch does not affect the result", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": [true, "ok", {"$divide": [1, 0]}]}`, doc)
		assert.Equal(t, "ok", string(v.GetStringBytes()))
	})
	t.Run("laziness is structural", func(t *testing.T) {
		then := &countingExpr{inner: mustExpr(t, `"t"`)}
		els := &countingExpr{inner: mustExpr(t, `"e"`)}
		e := &CondExpr{If: mustExpr(t, `"$s"`), Then: then, Else: els}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, "t", string(v.GetStringBytes()))
		assert.Equal(t, 1, then.n)
		assert.Zero(t, els.n, "untaken branch must not be evaluated")
	})
	t.Run("nests with other operators", func(t *testing.T) {
		v := evalExprOn(t, `{"$add": [10, {"$cond": [{"$eq": ["$z", 0]}, 1, 2]}]}`, doc)
		assert.Equal(t, float64(11), v.GetFloat64())
	})
}

func TestSwitchExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 5}`)
	t.Run("first truthy case wins", func(t *testing.T) {
		v := evalExprOn(t, `{"$switch": {"branches": [
			{"case": {"$lt": ["$a", 3]}, "then": "low"},
			{"case": {"$lt": ["$a", 10]}, "then": "mid"},
			{"case": true, "then": "any"}
		]}}`, doc)
		assert.Equal(t, "mid", string(v.GetStringBytes()))
	})
	t.Run("default", func(t *testing.T) {
		v := evalExprOn(t, `{"$switch": {"branches": [{"case": false, "then": 1}], "default": "$a"}}`, doc)
		assert.Equal(t, float64(5), v.GetFloat64())
	})
	t.Run("no match and no default yields null", func(t *testing.T) {
		// Mongo raises here; streaming eval has no per-document error channel.
		v := evalExprOn(t, `{"$switch": {"branches": [{"case": false, "then": 1}]}}`, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	})
	t.Run("laziness is structural", func(t *testing.T) {
		case2 := &countingExpr{inner: mustExpr(t, `true`)}
		then2 := &countingExpr{inner: mustExpr(t, `2`)}
		def := &countingExpr{inner: mustExpr(t, `0`)}
		e := &SwitchExpr{
			Cases:   []Expr{mustExpr(t, `true`), case2},
			Thens:   []Expr{mustExpr(t, `1`), then2},
			Default: def,
		}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, float64(1), v.GetFloat64())
		assert.Zero(t, case2.n, "later cases must not be evaluated")
		assert.Zero(t, then2.n)
		assert.Zero(t, def.n)
	})
}

func TestIfNullExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 1, "nul": null, "z": 0, "f": false}`)
	t.Run("two operands", func(t *testing.T) {
		assert.Equal(t, float64(1), evalExprOn(t, `{"$ifNull": ["$a", 9]}`, doc).GetFloat64())
		assert.Equal(t, float64(9), evalExprOn(t, `{"$ifNull": ["$nul", 9]}`, doc).GetFloat64())
		assert.Equal(t, float64(9), evalExprOn(t, `{"$ifNull": ["$nope", 9]}`, doc).GetFloat64())
	})
	t.Run("falsy but present values pass through", func(t *testing.T) {
		assert.Equal(t, float64(0), evalExprOn(t, `{"$ifNull": ["$z", 9]}`, doc).GetFloat64())
		assert.Equal(t, anyenc.TypeFalse, evalExprOn(t, `{"$ifNull": ["$f", 9]}`, doc).Type())
	})
	t.Run("four operands take the first non-null", func(t *testing.T) {
		v := evalExprOn(t, `{"$ifNull": ["$nope", "$nul", "$a", 9]}`, doc)
		assert.Equal(t, float64(1), v.GetFloat64())
	})
	t.Run("all null yields the last operand", func(t *testing.T) {
		v := evalExprOn(t, `{"$ifNull": ["$nul", "$nope", {"$literal": null}]}`, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	})
	t.Run("laziness is structural", func(t *testing.T) {
		rest := &countingExpr{inner: mustExpr(t, `9`)}
		e := &IfNullExpr{Args: []Expr{mustExpr(t, `"$a"`), rest}}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, float64(1), v.GetFloat64())
		assert.Zero(t, rest.n, "replacement must not be evaluated when unused")
	})
}

func TestCompareExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 1, "b": 2, "nul": null}`)
	boolOf := func(t *testing.T, exprJson string, d *anyenc.Value) bool {
		v := evalExprOn(t, exprJson, d)
		require.NotNil(t, v)
		switch v.Type() {
		case anyenc.TypeTrue:
			return true
		case anyenc.TypeFalse:
			return false
		}
		t.Fatalf("not a bool: %s -> %s", exprJson, v.Type())
		return false
	}
	cmpOf := func(t *testing.T, exprJson string, d *anyenc.Value) int {
		v := evalExprOn(t, exprJson, d)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return int(v.GetFloat64())
	}

	t.Run("same-type", func(t *testing.T) {
		for _, tc := range []struct {
			lo, hi string
		}{
			{`1`, `2`},
			{`-2`, `-1`},
			{`"a"`, `"b"`},
			{`"a"`, `"ab"`}, // prefix orders first
			{`false`, `true`},
			{`[1]`, `[1,2]`}, // elementwise: prefix orders first
			{`[1,2]`, `[1,3]`},
			{`{"x":1}`, `{"x":2}`},
		} {
			pair := tc.lo + "," + tc.hi
			assert.True(t, boolOf(t, `{"$lt": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$lte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$gt": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$gte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$ne": [`+pair+`]}`, doc), pair)
			assert.Equal(t, -1, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
			assert.Equal(t, 1, cmpOf(t, `{"$cmp": [`+tc.hi+`,`+tc.lo+`]}`, doc), pair)
		}
		for _, v := range []string{`1`, `"a"`, `true`, `false`, `null`, `[1,2]`, `{"x":1}`} {
			pair := v + "," + v
			assert.True(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$lte": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$gte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$ne": [`+pair+`]}`, doc), pair)
			assert.Zero(t, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
		}
	})
	t.Run("negative zero equals zero", func(t *testing.T) {
		assert.True(t, boolOf(t, `{"$eq": [-0.0, 0]}`, doc))
		assert.Zero(t, cmpOf(t, `{"$cmp": [-0.0, 0]}`, doc))
		assert.False(t, boolOf(t, `{"$lt": [-0.0, 0]}`, doc))
	})
	t.Run("missing equals null", func(t *testing.T) {
		assert.True(t, boolOf(t, `{"$eq": ["$nope", null]}`, doc))
		assert.True(t, boolOf(t, `{"$eq": ["$nope", "$nul"]}`, doc))
		assert.True(t, boolOf(t, `{"$eq": ["$nope", "$also.missing"]}`, doc))
		assert.Equal(t, -1, cmpOf(t, `{"$cmp": ["$nope", 0]}`, doc), "null sorts before numbers")
	})
	t.Run("cross-type order is anyenc tag order and $lt agrees with $cmp", func(t *testing.T) {
		// null < number < string < false < true < array < object.
		ordered := []string{`null`, `-1`, `0`, `1.5`, `"a"`, `"b"`, `false`, `true`, `[1]`, `[2]`, `{"x":1}`}
		for i, lo := range ordered {
			for _, hi := range ordered[i+1:] {
				pair := lo + "," + hi
				assert.True(t, boolOf(t, `{"$lt": [`+pair+`]}`, doc), pair)
				assert.Equal(t, -1, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
				assert.Equal(t, 1, cmpOf(t, `{"$cmp": [`+hi+`,`+lo+`]}`, doc), pair)
				assert.False(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			}
		}
	})
	t.Run("object equality is field-order-sensitive", func(t *testing.T) {
		// Marshaled-bytes order, consistent with $group key equality
		// (divergence from Mongo's order-insensitive document comparison).
		assert.False(t, boolOf(t, `{"$eq": [{"a": 1, "b": 2}, {"b": 2, "a": 1}]}`, doc))
	})
	t.Run("dateTime values", func(t *testing.T) {
		a := &anyenc.Arena{}
		d := a.NewObject()
		d.Set("d1", a.NewDateTimeMillis(1000))
		d.Set("d2", a.NewDateTimeMillis(2000))
		d.Set("d1b", a.NewDateTimeMillis(1000))
		assert.True(t, boolOf(t, `{"$lt": ["$d1", "$d2"]}`, d))
		assert.True(t, boolOf(t, `{"$eq": ["$d1", "$d1b"]}`, d))
		assert.Equal(t, -1, cmpOf(t, `{"$cmp": ["$d1", "$d2"]}`, d))
		// dateTime tag sorts after every JSON-expressible type.
		assert.True(t, boolOf(t, `{"$lt": [{"x": 1}, "$d1"]}`, d))
	})
	t.Run("comparison feeds $cond", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": [{"$gte": ["$b", "$a"]}, "yes", "no"]}`, doc)
		assert.Equal(t, "yes", string(v.GetStringBytes()))
	})
}

// TestExprEvalAllocFree pins the hot-path contract: operator evaluation
// allocates nothing per document once the arena cache is warm.
func TestExprEvalAllocFree(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4, "s1": "héllo → w", "s2": "0123456789",
		"d": {"$date": "2026-03-28T12:00:00Z"}, "d2": {"$date": "2026-08-05T06:00:00Z"}}`)
	for _, tc := range []struct{ name, json string }{
		{"arith", `{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`},
		{"concat", `{"$concat": ["$s1", "$s2"]}`},
		{"cond over comparison", `{"$cond": [{"$lt": ["$a", "$b"]}, {"$add": ["$a", 1]}, "$b"]}`},
		{"switch", `{"$switch": {"branches": [
			{"case": {"$eq": ["$s1", "$s2"]}, "then": 1},
			{"case": {"$gt": ["$a", "$b"]}, "then": 2},
			{"case": true, "then": "$a"}
		], "default": 0}}`},
		{"compare containers", `{"$cmp": [["$s1", "$a"], ["$s2", "$b"]]}`},
		{"ifNull", `{"$ifNull": ["$nope", "$a"]}`},
		{"dateAdd", `{"$dateAdd": {"startDate": "$d", "unit": "month", "amount": 5, "timezone": "Europe/Berlin"}}`},
		{"dateDiff", `{"$dateDiff": {"startDate": "$d", "endDate": "$d2", "unit": "month", "timezone": "Europe/Berlin"}}`},
		{"dateTrunc", `{"$dateTrunc": {"date": "$d", "unit": "week", "binSize": 2, "startOfWeek": "monday"}}`},
		{"dateTrunc sub-day", `{"$dateTrunc": {"date": "$d", "unit": "hour", "binSize": 5, "timezone": "Europe/Berlin"}}`},
		{"dateTrunc month", `{"$dateTrunc": {"date": "$d", "unit": "month"}}`},
		{"week", `{"$week": "$d"}`},
		{"week olson tz", `{"$week": {"date": "$d", "timezone": "Europe/Berlin"}}`},
		{"add date", `{"$add": ["$d", 3600000]}`},
		{"subtract dates", `{"$subtract": ["$d2", "$d"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := ParseExpr(anyenc.MustParseJson(tc.json))
			require.NoError(t, err)
			a := &anyenc.Arena{}
			// Warm up the arena value cache.
			for i := 0; i < 1000; i++ {
				a.Reset()
				if _, err := e.Eval(a, doc); err != nil {
					t.Fatal(err)
				}
			}
			res := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					a.Reset()
					v, err := e.Eval(a, doc)
					if err != nil || v == nil {
						b.Fatal(v, err)
					}
				}
			})
			assert.Zero(t, res.AllocsPerOp(), "expression eval must be alloc-free in steady state")
		})
	}
}

func BenchmarkArithExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCondExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$cond": [{"$lt": ["$a", "$b"]}, {"$add": ["$a", 1]}, "$b"]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSwitchExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$switch": {"branches": [
		{"case": {"$lt": ["$a", 3]}, "then": "low"},
		{"case": {"$lt": ["$a", 10]}, "then": "mid"},
		{"case": true, "then": "high"}
	], "default": "none"}}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 5}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDateAddExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(
		`{"$dateAdd": {"startDate": "$d", "unit": "day", "amount": 5, "timezone": "Europe/Berlin"}}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"d": {"$date": "2026-03-28T12:00:00Z"}}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDateDiffExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(
		`{"$dateDiff": {"startDate": "$d", "endDate": "$d2", "unit": "month", "timezone": "Europe/Berlin"}}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(
		`{"d": {"$date": "2026-03-28T12:00:00Z"}, "d2": {"$date": "2026-08-05T06:00:00Z"}}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcatExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$concat": ["$s1", "$s2"]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"s1": "héllo → w", "s2": "0123456789"}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}
