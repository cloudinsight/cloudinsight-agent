// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileLineLogging(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)

	// The default logging level should be "info".
	Debug("This debug-level line should not show up in the output.")
	Infof("This %s-level line should show up in the output.", "info")

	re := `^time=".*" level=info msg="This info-level line should show up in the output." source="log_test.go:29" \n$`
	assert.Regexp(t, re, buf.String())
}

func TestSetLevel(t *testing.T) {
	var buf bytes.Buffer
	for _, c := range []struct {
		in    string
		want  error
		count int
		re    string
	}{
		{"error", nil, 1, `^time=".*" level=error msg="This error-level line should show up in the output." source=".*" $`},
		{"warn", nil, 2, `^time=".*" level=warning msg="This warn-level line should show up in the output." source=".*" $`},
		{"info", nil, 3, `^time=".*" level=info msg="This info-level line should show up in the output." source=".*" $`},
		{"debug", nil, 4, `^time=".*" level=debug msg="This debug-level line should show up in the output." source=".*" $`},
	} {
		SetOutput(&buf)
		out := SetLevel(c.in)
		if out != c.want {
			t.Fatalf("SetLevel(%q) == %q, want %q", c.in, out, c.want)
		}

		logging()
		lineSep := []byte{'\n'}
		lines := bytes.Split(buf.Bytes(), lineSep)
		length := len(lines) - 1
		assert.Equal(t, length, c.count)
		assert.Regexp(t, c.re, string(lines[length-1]))

		buf.Reset()
	}
}

func logging() {
	Errorf("This error-level line should show up in the output.")
	Warnf("This warn-level line should show up in the output.")
	Infof("This info-level line should show up in the output.")
	Debugf("This debug-level line should show up in the output.")
}
