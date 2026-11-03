// Package cron computes fire times for the standard five-field schedule
// expression stored in workflow.schedule_cron. It has no IO: the caller owns the
// clock, the timezone and the persistence of next_fire_at. V0 deliberately stops
// at computing times — turning a fire into a Task belongs to Q5/Q6, still UNSET.
package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule is a validated, immutable expression. Keep it value-typed so callers
// can share it without a mutex; nothing here mutates after Parse.
type Schedule struct {
	minute field
	hour   field
	dom    field
	month  field
	dow    field
}

type field struct {
	values map[int]bool
	star   bool // field was exactly "*", which changes dom/dow OR semantics
}

func parseField(expr string, lo, hi int) (field, error) {
	f := field{values: map[int]bool{}, star: expr == "*"}
	for _, part := range strings.Split(expr, ",") {
		if err := addPart(f, part, lo, hi); err != nil {
			return field{}, fmt.Errorf("cron field %q: %w", expr, err)
		}
	}
	if len(f.values) == 0 {
		return field{}, fmt.Errorf("cron field %q matched nothing", expr)
	}
	return f, nil
}

func addPart(f field, part string, lo, hi int) error {
	base := part
	step := 1
	if i := strings.Index(part, "/"); i >= 0 {
		base = part[:i]
		n, err := strconv.Atoi(part[i+1:])
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid step in %q", part)
		}
		step = n
	}

	var a, b int
	switch {
	case base == "*":
		a, b = lo, hi
	case strings.Contains(base, "-"):
		p := strings.SplitN(base, "-", 2)
		x, err := strconv.Atoi(p[0])
		if err != nil {
			return fmt.Errorf("invalid range %q", part)
		}
		y, err := strconv.Atoi(p[1])
		if err != nil {
			return fmt.Errorf("invalid range %q", part)
		}
		a, b = x, y
	default:
		v, err := strconv.Atoi(base)
		if err != nil {
			return fmt.Errorf("invalid value %q", part)
		}
		a, b = v, v
		// "5/15" means 5,20,35,50 in cron, not just 5.
		if step > 1 {
			b = hi
		}
	}
	if a < lo || b > hi || a > b {
		return fmt.Errorf("%q outside [%d,%d]", part, lo, hi)
	}
	for v := a; v <= b; v += step {
		f.values[v] = true
	}
	return nil
}

// Parse validates minute hour dom month dow. Sunday is both 0 and 7.
func Parse(expr string) (Schedule, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return Schedule{}, fmt.Errorf("cron: want 5 fields, got %d in %q", len(parts), expr)
	}
	var s Schedule
	var err error
	if s.minute, err = parseField(parts[0], 0, 59); err != nil {
		return Schedule{}, err
	}
	if s.hour, err = parseField(parts[1], 0, 23); err != nil {
		return Schedule{}, err
	}
	if s.dom, err = parseField(parts[2], 1, 31); err != nil {
		return Schedule{}, err
	}
	if s.month, err = parseField(parts[3], 1, 12); err != nil {
		return Schedule{}, err
	}
	if s.dow, err = parseField(parts[4], 0, 7); err != nil {
		return Schedule{}, err
	}
	if s.dow.values[7] {
		s.dow.values[0] = true
		delete(s.dow.values, 7)
	}
	return s, nil
}

// NextAfter returns the first fire time strictly after t, expressed in loc.
// Impossible expressions (e.g. "0 9 30 2 *") fail instead of silently returning
// a placeholder; the scheduler must never persist a fabricated next_fire_at.
func (s Schedule) NextAfter(t time.Time, loc *time.Location) (time.Time, error) {
	cur := t.In(loc).Truncate(time.Minute).Add(time.Minute)
	deadline := cur.AddDate(4, 0, 0)
	for cur.Before(deadline) {
		if !s.month.values[int(cur.Month())] {
			cur = time.Date(cur.Year(), cur.Month()+1, 1, 0, 0, 0, 0, loc)
			continue
		}
		if !s.matchesDay(cur) {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, 0, 0, 0, 0, loc)
			continue
		}
		if !s.hour.values[cur.Hour()] {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day(), cur.Hour()+1, 0, 0, 0, loc)
			continue
		}
		if !s.minute.values[cur.Minute()] {
			cur = cur.Add(time.Minute)
			continue
		}
		return cur, nil
	}
	return time.Time{}, fmt.Errorf("cron: no fire time within 4 years")
}

// cron's dom/dow rule: when both are restricted the day matches on either;
// when only one is restricted that one governs.
func (s Schedule) matchesDay(t time.Time) bool {
	dom := s.dom.values[t.Day()]
	dow := s.dow.values[int(t.Weekday())]
	switch {
	case s.dom.star && s.dow.star:
		return true
	case s.dom.star:
		return dow
	case s.dow.star:
		return dom
	default:
		return dom || dow
	}
}

// NextAfter is the convenience form for callers that hold the raw expression.
func NextAfter(expr string, after time.Time, loc *time.Location) (time.Time, error) {
	s, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return s.NextAfter(after, loc)
}
