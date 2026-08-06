package aggregate

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// dateUnit is a calendar/time unit accepted by the date operators.
type dateUnit uint8

const (
	unitYear dateUnit = iota
	unitQuarter
	unitMonth
	unitWeek
	unitDay
	unitHour
	unitMinute
	unitSecond
	unitMillisecond
)

var dateUnitNames = [...]string{
	"year", "quarter", "month", "week", "day", "hour", "minute", "second", "millisecond",
}

func (u dateUnit) String() string { return dateUnitNames[u] }

// dateUnitMs is the fixed millisecond span of the sub-month units; zero for
// the calendar units (year/quarter/month), which have no fixed length.
var dateUnitMs = [...]int64{0, 0, 0, 7 * dayMs, dayMs, 3600000, 60000, 1000, 1}

const dayMs = 86400000

// Mongo's $dateTrunc reference point 2000-01-01T00:00:00 (a Saturday), as a
// civil day serial and as millis.
const (
	refDays2000 = 10957
	refMs2000   = refDays2000 * dayMs
)

// dateYearMax bounds years handed to time.Date: within it neither the
// construction nor UnixMilli can overflow int64 (the millis range ends in
// year ±292278994); outside, the result is unrepresentable → null.
const dateYearMax = 292_000_000

var weekdayNames = [...]string{
	"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
}

// dateOpObject collects the named parameters of a date operator's object
// form. slots maps parameter name to destination; a non-object operand and
// unknown or duplicate keys are parse errors.
func dateOpObject(op string, v *anyenc.Value, slots map[string]**anyenc.Value) error {
	if v.Type() != anyenc.TypeObject {
		return &query.ParseError{Op: op, Reason: op + " requires an object"}
	}
	obj, _ := v.Object()
	var perr error
	obj.Visit(func(key []byte, item *anyenc.Value) {
		if perr != nil {
			return
		}
		slot, ok := slots[string(key)]
		if !ok {
			perr = atPath(&query.ParseError{
				Op:     op,
				Reason: "unknown " + op + " parameter: " + string(key),
			}, string(key))
			return
		}
		if *slot != nil {
			perr = atPath(&query.ParseError{
				Op:     op,
				Reason: "duplicate " + op + " parameter: " + string(key),
			}, string(key))
			return
		}
		*slot = item
	})
	return perr
}

// literalStringParam enforces the parse-time-literal contract of
// unit/timezone/startOfWeek: a plain string, not an expression — a deliberate
// divergence from Mongo, which accepts dynamic expressions here (see
// docs/aggregation.md).
func literalStringParam(op, name string, v *anyenc.Value) (string, error) {
	if v.Type() == anyenc.TypeString {
		s := string(v.GetStringBytes())
		if !strings.HasPrefix(s, "$") {
			return s, nil
		}
	}
	return "", atPath(&query.ParseError{
		Op:     op,
		Reason: op + " '" + name + "' must be a literal string (expressions are not supported)",
	}, name)
}

func parseDateUnit(op string, v *anyenc.Value) (dateUnit, error) {
	s, err := literalStringParam(op, "unit", v)
	if err != nil {
		return 0, err
	}
	for i, n := range dateUnitNames {
		if n == s {
			return dateUnit(i), nil
		}
	}
	return 0, atPath(&query.ParseError{
		Op:     op,
		Reason: "unknown " + op + " unit: " + s,
	}, "unit")
}

// resolveTimezone maps a literal timezone spelling to a *time.Location once
// at parse time: an Olson name ("Europe/Berlin") or a fixed offset ("+02:00",
// "-0500", "+02").
func resolveTimezone(op string, v *anyenc.Value) (*time.Location, string, error) {
	s, err := literalStringParam(op, "timezone", v)
	if err != nil {
		return nil, "", err
	}
	if s != "" {
		if s[0] == '+' || s[0] == '-' {
			if off, ok := fixedZoneOffset(s); ok {
				return time.FixedZone(s, off), s, nil
			}
		} else if loc, lerr := time.LoadLocation(s); lerr == nil {
			return loc, s, nil
		}
	}
	return nil, "", atPath(&query.ParseError{
		Op:     op,
		Reason: "unknown timezone: " + s,
	}, "timezone")
}

// fixedZoneOffset parses ±HH, ±HHMM and ±HH:MM into seconds east of UTC.
func fixedZoneOffset(s string) (seconds int, ok bool) {
	sign := 1
	if s[0] == '-' {
		sign = -1
	}
	rest := s[1:]
	var hh, mm string
	switch len(rest) {
	case 2:
		hh, mm = rest, "00"
	case 4:
		hh, mm = rest[:2], rest[2:]
	case 5:
		if rest[2] != ':' {
			return 0, false
		}
		hh, mm = rest[:2], rest[3:]
	default:
		return 0, false
	}
	h, ok1 := twoDigits(hh)
	m, ok2 := twoDigits(mm)
	if !ok1 || !ok2 || h > 23 || m > 59 {
		return 0, false
	}
	return sign * (h*3600 + m*60), true
}

func twoDigits(s string) (int, bool) {
	if len(s) != 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return 0, false
	}
	return int(s[0]-'0')*10 + int(s[1]-'0'), true
}

// parseStartOfWeek accepts case-insensitive full weekday names.
func parseStartOfWeek(op string, v *anyenc.Value) (time.Weekday, string, error) {
	s, err := literalStringParam(op, "startOfWeek", v)
	if err != nil {
		return 0, "", err
	}
	for i, n := range weekdayNames {
		if strings.EqualFold(n, s) {
			return time.Weekday(i), s, nil
		}
	}
	return 0, "", atPath(&query.ParseError{
		Op:     op,
		Reason: "unknown " + op + " startOfWeek: " + s,
	}, "startOfWeek")
}

// parseBinSize parses the literal $dateTrunc binSize: a positive integer,
// capped so binSize×unit-millis stays inside int64.
func parseBinSize(op string, v *anyenc.Value) (int64, error) {
	if v.Type() == anyenc.TypeNumber {
		f, _ := v.Float64()
		if f == math.Trunc(f) && f >= 1 && f <= 1<<31 {
			return int64(f), nil
		}
	}
	return 0, atPath(&query.ParseError{
		Op:     op,
		Reason: op + " 'binSize' must be a positive integer",
	}, "binSize")
}

// evalDateTime evaluates sub as a dateTime operand. ok=false means the
// operator result is null: the operand is missing, null, or not a dateTime
// (Mongo errors for the latter; same no-error-channel rationale as
// evalNumber).
func evalDateTime(sub Expr, a *anyenc.Arena, doc *anyenc.Value) (ms int64, ok bool, err error) {
	v, err := sub.Eval(a, doc)
	if err != nil || v == nil || v.Type() != anyenc.TypeDateTime {
		return 0, false, err
	}
	ms, _ = v.DateTimeMillis()
	return ms, true, nil
}

// earliestLocal builds the instant for the given local components. A wall
// time repeated by a DST fall-back resolves to its earliest occurrence
// (time.Date's pick is unspecified); a wall time skipped by a spring-forward
// keeps time.Date's forward normalization.
func earliestLocal(y int, m time.Month, d, hh, mi, ss, ns int, loc *time.Location) time.Time {
	t := time.Date(y, m, d, hh, mi, ss, ns, loc)
	_, off := t.Zone()
	// A repeated wall time sits within the shift after a fall-back, so the
	// offset 12h earlier is the pre-transition one; the earlier occurrence
	// is the same wall clock at that larger offset.
	if _, offBefore := t.Add(-12 * time.Hour).Zone(); offBefore > off {
		t2 := t.Add(time.Duration(off-offBefore) * time.Second)
		y2, m2, d2 := t2.Date()
		hh2, mi2, ss2 := t2.Clock()
		if y2 == y && m2 == m && d2 == d && hh2 == hh && mi2 == mi && ss2 == ss {
			return t2
		}
	}
	return t
}

// dateTimeResult converts a computed time to an arena dateTime; a value whose
// millis don't round-trip (outside the int64-millis range) maps to null.
func dateTimeResult(a *anyenc.Arena, t time.Time) *anyenc.Value {
	ms := t.UnixMilli()
	if !time.UnixMilli(ms).Equal(t) {
		return a.NewNull()
	}
	return a.NewDateTimeMillis(ms)
}

// shiftMillis shifts a dateTime by a float64 amount of millis (fraction
// truncated); NaN, out-of-range deltas and overflow → not ok.
func shiftMillis(ms int64, delta float64) (int64, bool) {
	if math.IsNaN(delta) || delta < -9.2e18 || delta > 9.2e18 {
		return 0, false
	}
	return addInt64(ms, int64(delta))
}

func addInt64(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}

func subInt64(a, b int64) (int64, bool) {
	s := a - b
	if (b > 0 && s > a) || (b < 0 && s < a) {
		return 0, false
	}
	return s, true
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func floorMod(a, b int64) int64 { return a - floorDiv(a, b)*b }

// civilDays returns days from 1970-01-01 to the proleptic-Gregorian date
// y-m-d (Hinnant's days_from_civil; valid over the whole int64-millis range).
func civilDays(y int, m time.Month, d int) int64 {
	yy := int64(y)
	if m <= time.February {
		yy--
	}
	era := floorDiv(yy, 400)
	yoe := yy - era*400                // [0, 399]
	mp := (int64(m) + 9) % 12          // Mar=0 .. Feb=11
	doy := (153*mp+2)/5 + int64(d) - 1 // [0, 365]
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// civilFromDays is the inverse of civilDays (Hinnant's civil_from_days).
func civilFromDays(z int64) (y int, m time.Month, d int) {
	z += 719468
	era := floorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	yy := yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	m0 := mp + 3
	if m0 > 12 {
		m0 -= 12
	}
	if m0 <= 2 {
		yy++
	}
	return int(yy), time.Month(m0), d
}

var monthDays = [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func daysInMonth(y int, m time.Month) int {
	if m == time.February && y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		return 29
	}
	return monthDays[m-1]
}

func localDays(ms int64, loc *time.Location) int64 {
	y, m, d := time.UnixMilli(ms).In(loc).Date()
	return civilDays(y, m, d)
}

func writeTZ(b *strings.Builder, tz string) {
	if tz != "" {
		b.WriteString(",timezone:\"")
		b.WriteString(tz)
		b.WriteByte('"')
	}
}

// DateAddExpr evaluates $dateAdd. year/quarter/month are calendar-aware in
// the operative timezone with Mongo's day-of-month clamping (Jan 31 + 1 month
// = Feb 28; time.AddDate would normalize to Mar 3); week/day add calendar
// days in the operative timezone, preserving the local clock across DST
// (Mongo semantics — a repeated wall time resolves to its earlier
// occurrence); hour/minute/second/millisecond are fixed millis spans. A
// missing/null/non-dateTime startDate, a non-integral amount and an
// out-of-range result → null (Mongo errors; see evalNumber).
type DateAddExpr struct {
	Start  Expr
	Amount Expr
	Unit   dateUnit
	Loc    *time.Location
	TZ     string // original timezone spelling, "" when defaulted to UTC
}

func parseDateAdd(v *anyenc.Value) (Expr, error) {
	const op = "$dateAdd"
	var startV, unitV, amountV, tzV *anyenc.Value
	if err := dateOpObject(op, v, map[string]**anyenc.Value{
		"startDate": &startV, "unit": &unitV, "amount": &amountV, "timezone": &tzV,
	}); err != nil {
		return nil, err
	}
	if startV == nil || unitV == nil || amountV == nil {
		return nil, &query.ParseError{Op: op, Reason: op + " requires 'startDate', 'unit' and 'amount'"}
	}
	e := &DateAddExpr{Loc: time.UTC}
	var err error
	if e.Start, err = ParseExpr(startV); err != nil {
		return nil, atPath(err, "startDate")
	}
	if e.Unit, err = parseDateUnit(op, unitV); err != nil {
		return nil, err
	}
	if e.Amount, err = ParseExpr(amountV); err != nil {
		return nil, atPath(err, "amount")
	}
	if tzV != nil {
		if e.Loc, e.TZ, err = resolveTimezone(op, tzV); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *DateAddExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	ms, ok, err := evalDateTime(e.Start, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	f, ok, err := evalNumber(e.Amount, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok || f != math.Trunc(f) || f < -(1<<53) || f > 1<<53 {
		return a.NewNull(), nil
	}
	n := int64(f)
	switch e.Unit {
	case unitHour, unitMinute, unitSecond, unitMillisecond: // fixed spans
		u := dateUnitMs[e.Unit]
		delta := n * u
		if n != 0 && delta/n != u {
			return a.NewNull(), nil
		}
		res, ok := addInt64(ms, delta)
		if !ok {
			return a.NewNull(), nil
		}
		return a.NewDateTimeMillis(res), nil
	case unitWeek, unitDay:
		// Calendar days in the operative timezone: the local clock is
		// preserved across DST transitions (a 23h/25h absolute span).
		if e.Unit == unitWeek {
			n *= 7 // |n| ≤ 2^53: no overflow
		}
		t := time.UnixMilli(ms).In(e.Loc)
		y, mo, d := t.Date()
		hh, mi, ss := t.Clock()
		y2, m2, d2 := civilFromDays(civilDays(y, mo, d) + n)
		if y2 < -dateYearMax || y2 > dateYearMax {
			return a.NewNull(), nil
		}
		return dateTimeResult(a, earliestLocal(y2, m2, d2, hh, mi, ss, t.Nanosecond(), e.Loc)), nil
	}
	// year/quarter/month: month arithmetic on the local components with
	// Mongo's end-of-month clamping.
	months := n
	switch e.Unit {
	case unitYear:
		months = n * 12
	case unitQuarter:
		months = n * 3
	}
	t := time.UnixMilli(ms).In(e.Loc)
	y, mo, d := t.Date()
	hh, mi, ss := t.Clock()
	tm := int64(y)*12 + int64(mo-1) + months
	y2, m2 := floorDiv(tm, 12), time.Month(floorMod(tm, 12)+1)
	if y2 < -dateYearMax || y2 > dateYearMax {
		return a.NewNull(), nil
	}
	if last := daysInMonth(int(y2), m2); d > last {
		d = last
	}
	return dateTimeResult(a, earliestLocal(int(y2), m2, d, hh, mi, ss, t.Nanosecond(), e.Loc)), nil
}

func (e *DateAddExpr) String() string {
	var b strings.Builder
	b.WriteString("{$dateAdd:{startDate:")
	b.WriteString(e.Start.String())
	b.WriteString(",unit:\"")
	b.WriteString(e.Unit.String())
	b.WriteString("\",amount:")
	b.WriteString(e.Amount.String())
	writeTZ(&b, e.TZ)
	b.WriteString("}}")
	return b.String()
}

// DateDiffExpr evaluates $dateDiff: the signed count of unit boundaries
// crossed from startDate to endDate (Mongo semantics, not elapsed time — a
// day diff of 23:59 → 00:01 is 1). year/quarter/month/week/day boundaries are
// local calendar boundaries in the operative timezone; hour/minute/second/
// millisecond are absolute-millis boundaries (timezone-independent).
// startOfWeek (default sunday) applies to unit "week" only.
type DateDiffExpr struct {
	Start, End Expr
	Unit       dateUnit
	Loc        *time.Location
	TZ         string
	WeekStart  time.Weekday
	WeekName   string // original startOfWeek spelling, "" when defaulted
}

func parseDateDiff(v *anyenc.Value) (Expr, error) {
	const op = "$dateDiff"
	var startV, endV, unitV, tzV, sowV *anyenc.Value
	if err := dateOpObject(op, v, map[string]**anyenc.Value{
		"startDate": &startV, "endDate": &endV, "unit": &unitV,
		"timezone": &tzV, "startOfWeek": &sowV,
	}); err != nil {
		return nil, err
	}
	if startV == nil || endV == nil || unitV == nil {
		return nil, &query.ParseError{Op: op, Reason: op + " requires 'startDate', 'endDate' and 'unit'"}
	}
	e := &DateDiffExpr{Loc: time.UTC, WeekStart: time.Sunday}
	var err error
	if e.Start, err = ParseExpr(startV); err != nil {
		return nil, atPath(err, "startDate")
	}
	if e.End, err = ParseExpr(endV); err != nil {
		return nil, atPath(err, "endDate")
	}
	if e.Unit, err = parseDateUnit(op, unitV); err != nil {
		return nil, err
	}
	if tzV != nil {
		if e.Loc, e.TZ, err = resolveTimezone(op, tzV); err != nil {
			return nil, err
		}
	}
	if sowV != nil {
		if e.WeekStart, e.WeekName, err = parseStartOfWeek(op, sowV); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *DateDiffExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	sms, ok, err := evalDateTime(e.Start, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	ems, ok, err := evalDateTime(e.End, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	switch e.Unit {
	case unitMillisecond:
		return a.NewNumberFloat64(float64(ems) - float64(sms)), nil
	case unitSecond, unitMinute, unitHour:
		u := dateUnitMs[e.Unit]
		return a.NewNumberFloat64(float64(floorDiv(ems, u) - floorDiv(sms, u))), nil
	case unitDay:
		return a.NewNumberFloat64(float64(localDays(ems, e.Loc) - localDays(sms, e.Loc))), nil
	case unitWeek:
		// Day-serial residue of the week-start weekday: day 0 (1970-01-01)
		// is a Thursday, so serial d has weekday floorMod(d+4, 7) (Sunday=0)
		// and weeks begin at d ≡ WeekStart-4 (mod 7).
		r := floorMod(int64(e.WeekStart)-4, 7)
		sw := floorDiv(localDays(sms, e.Loc)-r, 7)
		ew := floorDiv(localDays(ems, e.Loc)-r, 7)
		return a.NewNumberFloat64(float64(ew - sw)), nil
	}
	// year, quarter, month: local calendar components.
	sy, sm, _ := time.UnixMilli(sms).In(e.Loc).Date()
	ey, em, _ := time.UnixMilli(ems).In(e.Loc).Date()
	var d int64
	switch e.Unit {
	case unitYear:
		d = int64(ey) - int64(sy)
	case unitQuarter:
		d = (int64(ey)*4 + int64(em-1)/3) - (int64(sy)*4 + int64(sm-1)/3)
	default: // unitMonth
		d = (int64(ey)*12 + int64(em-1)) - (int64(sy)*12 + int64(sm-1))
	}
	return a.NewNumberFloat64(float64(d)), nil
}

func (e *DateDiffExpr) String() string {
	var b strings.Builder
	b.WriteString("{$dateDiff:{startDate:")
	b.WriteString(e.Start.String())
	b.WriteString(",endDate:")
	b.WriteString(e.End.String())
	b.WriteString(",unit:\"")
	b.WriteString(e.Unit.String())
	b.WriteByte('"')
	writeTZ(&b, e.TZ)
	if e.WeekName != "" {
		b.WriteString(",startOfWeek:\"")
		b.WriteString(e.WeekName)
		b.WriteByte('"')
	}
	b.WriteString("}}")
	return b.String()
}

// DateTruncExpr evaluates $dateTrunc: truncate down to the containing
// binSize×unit bin. Bins anchor at Mongo's documented reference point —
// 2000-01-01T00:00:00 (in the operative timezone) for every unit except
// week, which anchors at the first startOfWeek on or after 2000-01-01 (a
// Saturday). year/quarter/month/week/day truncate local calendar components
// (a fall-back-repeated local midnight resolves to its earlier occurrence);
// hour/minute/second/millisecond subtract the local wall-clock residue on the
// absolute timeline, so truncation stays monotone and idempotent across DST
// transitions.
type DateTruncExpr struct {
	Date      Expr
	Unit      dateUnit
	Bin       int64 // effective binSize, >= 1
	Loc       *time.Location
	TZ        string
	WeekStart time.Weekday
	WeekName  string

	binSet bool // binSize was spelled explicitly: render it in String
}

func parseDateTrunc(v *anyenc.Value) (Expr, error) {
	const op = "$dateTrunc"
	var dateV, unitV, binV, tzV, sowV *anyenc.Value
	if err := dateOpObject(op, v, map[string]**anyenc.Value{
		"date": &dateV, "unit": &unitV, "binSize": &binV,
		"timezone": &tzV, "startOfWeek": &sowV,
	}); err != nil {
		return nil, err
	}
	if dateV == nil || unitV == nil {
		return nil, &query.ParseError{Op: op, Reason: op + " requires 'date' and 'unit'"}
	}
	e := &DateTruncExpr{Bin: 1, Loc: time.UTC, WeekStart: time.Sunday}
	var err error
	if e.Date, err = ParseExpr(dateV); err != nil {
		return nil, atPath(err, "date")
	}
	if e.Unit, err = parseDateUnit(op, unitV); err != nil {
		return nil, err
	}
	if binV != nil {
		if e.Bin, err = parseBinSize(op, binV); err != nil {
			return nil, err
		}
		e.binSet = true
	}
	if tzV != nil {
		if e.Loc, e.TZ, err = resolveTimezone(op, tzV); err != nil {
			return nil, err
		}
	}
	if sowV != nil {
		if e.WeekStart, e.WeekName, err = parseStartOfWeek(op, sowV); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func (e *DateTruncExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	ms, ok, err := evalDateTime(e.Date, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	t := time.UnixMilli(ms).In(e.Loc)
	switch e.Unit {
	case unitYear, unitQuarter, unitMonth:
		y, mo, _ := t.Date()
		var y2 int64
		var m2 time.Month
		switch e.Unit {
		case unitYear:
			y2 = 2000 + floorDiv(int64(y)-2000, e.Bin)*e.Bin
			m2 = time.January
		case unitQuarter:
			q := 4*2000 + floorDiv(int64(y)*4+int64(mo-1)/3-4*2000, e.Bin)*e.Bin
			y2, m2 = floorDiv(q, 4), time.Month(floorMod(q, 4)*3+1)
		default: // unitMonth
			m := 12*2000 + floorDiv(int64(y)*12+int64(mo-1)-12*2000, e.Bin)*e.Bin
			y2, m2 = floorDiv(m, 12), time.Month(floorMod(m, 12)+1)
		}
		if y2 < -dateYearMax || y2 > dateYearMax {
			return a.NewNull(), nil
		}
		return dateTimeResult(a, earliestLocal(int(y2), m2, 1, 0, 0, 0, 0, e.Loc)), nil
	case unitWeek, unitDay:
		y, mo, d := t.Date()
		days := civilDays(y, mo, d)
		ref, step := int64(refDays2000), e.Bin
		if e.Unit == unitWeek {
			// First startOfWeek on or after 2000-01-01, a Saturday (=6).
			ref += floorMod(int64(e.WeekStart)-6, 7)
			step *= 7
		}
		start := days - floorMod(days-ref, step)
		y2, m2, d2 := civilFromDays(start)
		if y2 < -dateYearMax || y2 > dateYearMax {
			return a.NewNull(), nil
		}
		return dateTimeResult(a, earliestLocal(y2, m2, d2, 0, 0, 0, 0, e.Loc)), nil
	}
	// hour, minute, second, millisecond: subtract the wall-clock residue on
	// the absolute timeline. Reassembling wall components would collapse a
	// fall-back-repeated wall time onto one occurrence — two instants an
	// hour apart sharing a bin, and results jumping forward — while the
	// residue form is identical outside transition windows and stays
	// monotone and idempotent inside them.
	_, off := t.Zone()
	lms, ok := addInt64(ms, int64(off)*1000)
	if !ok {
		return a.NewNull(), nil
	}
	step := e.Bin * dateUnitMs[e.Unit] // bin ≤ 2^31: no overflow
	diff, ok := subInt64(lms, refMs2000)
	if !ok {
		return a.NewNull(), nil
	}
	res, ok := subInt64(ms, floorMod(diff, step))
	if !ok {
		return a.NewNull(), nil
	}
	return a.NewDateTimeMillis(res), nil
}

func (e *DateTruncExpr) String() string {
	var b strings.Builder
	b.WriteString("{$dateTrunc:{date:")
	b.WriteString(e.Date.String())
	b.WriteString(",unit:\"")
	b.WriteString(e.Unit.String())
	b.WriteByte('"')
	if e.binSet {
		b.WriteString(",binSize:")
		b.WriteString(strconv.FormatInt(e.Bin, 10))
	}
	writeTZ(&b, e.TZ)
	if e.WeekName != "" {
		b.WriteString(",startOfWeek:\"")
		b.WriteString(e.WeekName)
		b.WriteByte('"')
	}
	b.WriteString("}}")
	return b.String()
}

// datePart identifies a date-component operator.
type datePart uint8

const (
	partYear datePart = iota
	partWeek
)

var datePartNames = [...]string{"$year", "$week"}

func (p datePart) String() string { return datePartNames[p] }

// DatePartExpr evaluates $year and $week in the operative timezone. $week is
// Mongo's Sunday-based 0-53 week of year (days before the year's first
// Sunday are week 0) — not the ISO week. Both spellings parse: a plain date
// expression and the {date, timezone?} object form.
type DatePartExpr struct {
	Part datePart
	Date Expr
	Loc  *time.Location
	TZ   string
}

func parseDatePart(p datePart, v *anyenc.Value) (Expr, error) {
	op := p.String()
	e := &DatePartExpr{Part: p, Loc: time.UTC}
	// Any non-operator object operand is the {date, timezone?} form, so a
	// misspelled or missing key is a structured parse error rather than a
	// document literal silently evaluating to null; $-keyed objects stay
	// expressions (operators, $literal).
	if v.Type() == anyenc.TypeObject && !hasOperatorKey(v) {
		var dateV, tzV *anyenc.Value
		if err := dateOpObject(op, v, map[string]**anyenc.Value{
			"date": &dateV, "timezone": &tzV,
		}); err != nil {
			return nil, err
		}
		if dateV == nil {
			return nil, &query.ParseError{Op: op, Reason: op + " requires 'date'"}
		}
		var err error
		if e.Date, err = ParseExpr(dateV); err != nil {
			return nil, atPath(err, "date")
		}
		if tzV != nil {
			if e.Loc, e.TZ, err = resolveTimezone(op, tzV); err != nil {
				return nil, err
			}
		}
		return e, nil
	}
	args, err := parseOperands(op, v, 1, 1)
	if err != nil {
		return nil, err
	}
	e.Date = args[0]
	return e, nil
}

// hasOperatorKey reports whether the object carries a $-prefixed key, i.e.
// parses as an operator or $literal expression rather than a parameter
// object.
func hasOperatorKey(v *anyenc.Value) bool {
	obj, _ := v.Object()
	found := false
	obj.Visit(func(key []byte, _ *anyenc.Value) {
		if len(key) > 0 && key[0] == '$' {
			found = true
		}
	})
	return found
}

func (e *DatePartExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	ms, ok, err := evalDateTime(e.Date, a, doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.NewNull(), nil
	}
	t := time.UnixMilli(ms).In(e.Loc)
	if e.Part == partYear {
		return a.NewNumberInt(t.Year()), nil
	}
	return a.NewNumberInt((t.YearDay() - 1 + 7 - int(t.Weekday())) / 7), nil
}

func (e *DatePartExpr) String() string {
	if e.TZ == "" {
		return opString(e.Part.String(), []Expr{e.Date})
	}
	var b strings.Builder
	b.WriteByte('{')
	b.WriteString(e.Part.String())
	b.WriteString(":{date:")
	b.WriteString(e.Date.String())
	writeTZ(&b, e.TZ)
	b.WriteString("}}")
	return b.String()
}
