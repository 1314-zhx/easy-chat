package redis

import "testing"

func TestHotLeaseAgeGates(t *testing.T) {
	logic := int64(10 * 60)
	detectEnd := int64(14 * 60)

	cases := []struct {
		age    int64
		detect bool
		skip   bool
	}{
		{0, false, true},
		{logic - 1, false, true},
		{logic, true, false},
		{detectEnd - 1, true, false},
		{detectEnd, true, false},
		{15 * 60, true, false},
	}
	for _, c := range cases {
		skip := c.age < logic
		detect := !skip
		if skip != c.skip || detect != c.detect {
			t.Fatalf("age=%d skip=%v detect=%v", c.age, skip, detect)
		}
	}
}
