package cron

import (
	"testing"
	"time"
)

// Asia/Shanghai has had no DST since 1991, so a fixed +08 zone is exact here and
// avoids depending on a tzdata installation on Windows.
var shanghai = time.FixedZone("CST", 8*3600)

func at(y int, m time.Month, d, h, min int) time.Time {
	return time.Date(y, m, d, h, min, 0, 0, shanghai)
}

func wantNext(t *testing.T, expr string, after, want time.Time) {
	t.Helper()
	got, err := NextAfter(expr, after, shanghai)
	if err != nil {
		t.Fatalf("%q: %v", expr, err)
	}
	if !got.Equal(want) {
		t.Fatalf("%q from %s = %s, want %s", expr, after.Format(time.RFC3339), got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestNextAfterDailyRollsToTomorrow(t *testing.T) {
	wantNext(t, "0 8 * * *", at(2026, 11, 3, 10, 7), at(2026, 11, 4, 8, 0))
}

func TestNextAfterDailySameDay(t *testing.T) {
	wantNext(t, "0 8 * * *", at(2026, 11, 3, 7, 0), at(2026, 11, 3, 8, 0))
}

func TestNextAfterStepAndList(t *testing.T) {
	wantNext(t, "*/15 * * * *", at(2026, 11, 3, 10, 7), at(2026, 11, 3, 10, 15))
	wantNext(t, "5/15 * * * *", at(2026, 11, 3, 10, 7), at(2026, 11, 3, 10, 20))
	wantNext(t, "0,30 9 * * *", at(2026, 11, 3, 9, 5), at(2026, 11, 3, 9, 30))
}

func TestNextAfterWeekdayRange(t *testing.T) {
	// Fri 18:00 must skip the weekend and land on Mon 09:00.
	wantNext(t, "0 9-17 * * 1-5", at(2026, 11, 6, 18, 0), at(2026, 11, 9, 9, 0))
}

func TestDomDowIsOrWhenBothRestricted(t *testing.T) {
	// "day 5 OR Monday": 2026-11-05 (Wed) is earlier than the following Monday.
	wantNext(t, "0 9 5 * 1", at(2026, 11, 3, 10, 0), at(2026, 11, 5, 9, 0))
}

func TestSundayAcceptsBothZeroAndSeven(t *testing.T) {
	wantNext(t, "0 9 * * 0", at(2026, 11, 3, 10, 0), at(2026, 11, 8, 9, 0))
	wantNext(t, "0 9 * * 7", at(2026, 11, 3, 10, 0), at(2026, 11, 8, 9, 0))
}

func TestImpossibleExpressionFailsRatherThanFabricating(t *testing.T) {
	if _, err := NextAfter("0 9 30 2 *", at(2026, 11, 3, 10, 0), shanghai); err == nil {
		t.Fatal("February 30 has no fire time; must not return a placeholder")
	}
}

func TestParseRejectsMalformed(t *testing.T) {
	bad := []string{
		"0 8 * *",     // four fields
		"0 8 * * * *", // six fields
		"60 8 * * *",  // minute out of range
		"0 24 * * *",  // hour out of range
		"0 9 * 13 *",  // month out of range
		"0 9 * * 8",   // dow out of range
		"*/0 * * * *", // zero step
		"abc * * * *", // non-numeric
		"0 9-8 * * *", // reversed range
	}
	for _, expr := range bad {
		if _, err := Parse(expr); err == nil {
			t.Errorf("Parse(%q) should fail", expr)
		}
	}
}
