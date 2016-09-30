package util

import "testing"

func TestCast(t *testing.T) {
	for _, c := range []struct {
		in   float64
		want int
	}{
		{1, 1},
		{3.4, 3},
		{3.5, 4},
		{-0.5, -1},
	} {
		out := Cast(c.in)
		if out != c.want {
			t.Errorf("Cast(%f) == %d, want %d", c.in, out, c.want)
		}
	}
}

func TestRound(t *testing.T) {
	for _, c := range []struct {
		in1  float64
		in2  int
		want float64
	}{
		{10.14563, 1, 10.1},
		{10.14563, 2, 10.15},
		{10.14563, 3, 10.146},
		{10.14563, 4, 10.1456},
	} {
		out := Round(c.in1, c.in2)
		if out != c.want {
			t.Errorf("Cast(%f, %d) == %f, want %f", c.in1, c.in2, out, c.want)
		}
	}
}

func TestSum(t *testing.T) {
	for _, c := range []struct {
		in   []float64
		want float64
	}{
		{[]float64{1, 2}, 3},
		{[]float64{1, 2, 3}, 6},
		{[]float64{1, 2, 3, 4}, 10},
	} {
		out := Sum(c.in)
		if out != c.want {
			t.Errorf("Sum(%v) == %f, want %f", c.in, out, c.want)
		}
	}
}

func TestContains(t *testing.T) {
	for _, c := range []struct {
		in1  []int32
		in2  int32
		want bool
	}{
		{[]int32{1, 2, 3}, 1, true},
		{[]int32{1, 2, 3}, 3, true},
		{[]int32{1, 2, 3}, 9, false},
	} {
		out := Contains(c.in1, c.in2)
		if out != c.want {
			t.Errorf("Contains(%v, %d) == %t, want %t", c.in1, c.in2, out, c.want)
		}
	}
}

func TestHash(t *testing.T) {
	for _, c := range []struct {
		in   string
		want uint32
	}{
		{"test", 2949673445},
		{"test2", 2619553141},
		{"test3", 2602775522},
	} {
		out := Hash(c.in)
		if out != c.want {
			t.Errorf("Cast(%s) == %d, want %d", c.in, out, c.want)
		}
	}
}
