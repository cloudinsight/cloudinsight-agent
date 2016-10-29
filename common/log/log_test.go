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

	re := `^time=".*" level=info msg="This info-level line should show up in the output." source="log_test.go:16" \n$`
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

		for _, logger := range []logFunc{logging, loggingWithFormat, loggingWithNewLine} {
			logger()
			lineSep := []byte{'\n'}
			lines := bytes.Split(buf.Bytes(), lineSep)
			length := len(lines) - 1
			assert.Equal(t, length, c.count)
			assert.Regexp(t, c.re, string(lines[length-1]))

			buf.Reset()
		}
	}
}

type logFunc func()

func logging() {
	Error("This error-level line should show up in the output.")
	Warn("This warn-level line should show up in the output.")
	Info("This info-level line should show up in the output.")
	Debug("This debug-level line should show up in the output.")
}

func loggingWithFormat() {
	Errorf("This %s-level line should show up in the output.", "error")
	Warnf("This %s-level line should show up in the output.", "warn")
	Infof("This %s-level line should show up in the output.", "info")
	Debugf("This %s-level line should show up in the output.", "debug")
}

func loggingWithNewLine() {
	Errorln("This error-level line should show up in the output.")
	Warnln("This warn-level line should show up in the output.")
	Infoln("This info-level line should show up in the output.")
	Debugln("This debug-level line should show up in the output.")
}
