package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			t.Errorf("Round(%f, %d) == %f, want %f", c.in1, c.in2, out, c.want)
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
			t.Errorf("Hash(%s) == %d, want %d", c.in, out, c.want)
		}
	}
}

type MyStruct struct {
	StatusURL string `yaml:"status_url"`
	User      string
	password  string
	Tags      []string
	Timeout   int64
	Options   options
}

type options struct {
	Replication   bool
	GaleraCluster bool `yaml:"galera_cluster"`
}

func TestFillStruct(t *testing.T) {
	myData := make(map[string]interface{})
	myData["status_url"] = "http://localhost"
	myData["user"] = "admin"
	myData["password"] = "admin"
	myData["tags"] = []interface{}{"test"}

	result := &MyStruct{}
	err := FillStruct(myData, result)
	assert.NoError(t, err)
	assert.Equal(t, "http://localhost", result.StatusURL)
	assert.Equal(t, "admin", result.User)
	assert.Empty(t, result.password)
	assert.Equal(t, []string{"test"}, result.Tags)
}

func TestFillStructWithWrongType(t *testing.T) {
	myData := make(map[string]interface{})
	myData["user"] = 11

	result := &MyStruct{}
	err := FillStruct(myData, result)
	assert.Equal(t, "11", result.User)

	myData["timeout"] = "10"
	err = FillStruct(myData, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "yaml: unmarshal errors:")
}

func TestFillStructWithComplexType(t *testing.T) {
	myData := make(map[string]interface{})
	myData["options"] = map[string]interface{}{
		"replication":    true,
		"galera_cluster": false,
	}

	result := &MyStruct{}
	err := FillStruct(myData, result)
	assert.NoError(t, err)
	assert.True(t, result.Options.Replication)
	assert.False(t, result.Options.GaleraCluster)
}

func TestStringInSlice(t *testing.T) {
	for _, c := range []struct {
		in1  string
		in2  []string
		want bool
	}{
		{"Hello", []string{"Hello", "World"}, true},
		{"World", []string{"Hello", "World"}, true},
		{"Hi", []string{"Hello", "World"}, false},
	} {
		out := StringInSlice(c.in1, c.in2)
		if out != c.want {
			t.Errorf("Contains(%s, %v) == %t, want %t", c.in1, c.in2, out, c.want)
		}
	}
}
