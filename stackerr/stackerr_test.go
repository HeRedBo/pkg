package stackerr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gookit/goutil/dump"
)

// sentinel is a package-level error used for identity checks.
var sentinel = fmt.Errorf("sentinel error")

// customError is a user-defined error type for testing As-like traversal.
type customError struct {
	Code int
	Msg  string
}

func (e *customError) Error() string {
	return fmt.Sprintf("custom[%d]: %s", e.Code, e.Msg)
}

// ---------- helpers ----------

// chainUnwrap walks the Unwrap() chain and returns all errors.
func chainUnwrap(err error) []error {
	var chain []error
	for err != nil {
		chain = append(chain, err)
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return chain
}

// chainContains reports whether the Unwrap() chain contains an error whose
// Error() string equals target.
func chainContains(err error, target string) bool {
	for _, e := range chainUnwrap(err) {
		if e.Error() == target {
			return true
		}
	}
	return false
}

// chainAs searches the Unwrap() chain for a *customError.
func chainAs(err error) (*customError, bool) {
	for _, e := range chainUnwrap(err) {
		if ce, ok := e.(*customError); ok {
			return ce, true
		}
	}
	return nil, false
}

// ---------- tests ----------

func TestWithStack_NilError(t *testing.T) {
	got := WithStack(nil)
	if got != nil {
		t.Fatalf("WithStack(nil) = %v; want nil", got)
	}
}

func TestWithStack_BasicError(t *testing.T) {
	orig := fmt.Errorf("basic error")
	err := WithStack(orig)
	if err == nil {
		t.Fatal("WithStack returned nil for non-nil error")
	}
	se, ok := err.(*StackError)
	if !ok {
		t.Fatalf("expected *StackError, got %T", err)
	}
	if se.Error() != "basic error" {
		t.Fatalf("Error() = %q; want %q", se.Error(), "basic error")
	}
	frames := se.Stack()
	if len(frames) == 0 {
		t.Fatal("expected non-empty stack frames")
	}
}

func TestWithStack_NoDoubleWrap(t *testing.T) {
	orig := fmt.Errorf("original")
	first := WithStack(orig)
	second := WithStack(first)
	if first != second {
		t.Fatal("WithStack should not double-wrap an already-stacked error")
	}
}

func TestWrapf_NilError(t *testing.T) {
	got := Wrapf(nil, "context")
	if got != nil {
		t.Fatalf("Wrapf(nil, ...) = %v; want nil", got)
	}
}

func TestWrapf_BasicWrap(t *testing.T) {
	orig := fmt.Errorf("db failed")
	err := Wrapf(orig, "user %s", "alice")
	if err == nil {
		t.Fatal("Wrapf returned nil")
	}
	// The exact message should be "user alice: db failed"
	wantMsg := "user alice: db failed"
	if err.Error() != wantMsg {
		t.Fatalf("Error() = %q; want %q", err.Error(), wantMsg)
	}
	se, ok := err.(*StackError)
	if !ok {
		t.Fatalf("expected *StackError, got %T", err)
	}
	if len(se.Stack()) == 0 {
		t.Fatal("expected non-empty stack frames")
	}
}

func TestStackError_Error(t *testing.T) {
	orig := fmt.Errorf("something broke")
	err := WithStack(orig)
	if err.Error() != "something broke" {
		t.Fatalf("Error() = %q; want %q", err.Error(), "something broke")
	}

	dump.P(err)
}

func TestStackError_Unwrap(t *testing.T) {
	orig := fmt.Errorf("inner")
	se := &StackError{err: orig, frames: []Frame{{Function: "fake", File: "fake.go", Line: 1}}}
	unwrapped := se.Unwrap()
	if unwrapped != orig {
		t.Fatalf("Unwrap() returned %v; want %v", unwrapped, orig)
	}
}

func TestStackError_ErrorsIs(t *testing.T) {
	// Verify that manual Is-like traversal finds the sentinel error.
	wrapped := fmt.Errorf("layer1: %w", sentinel)
	err := WithStack(wrapped)
	if !chainContains(err, sentinel.Error()) {
		t.Fatalf("expected chain to contain sentinel %q, got chain: %v",
			sentinel.Error(), chainUnwrap(err))
	}
}

func TestStackError_ErrorsAs(t *testing.T) {
	ce := &customError{Code: 42, Msg: "not found"}
	wrapped := fmt.Errorf("request failed: %w", ce)
	err := WithStack(wrapped)
	got, ok := chainAs(err)
	if !ok {
		t.Fatal("expected to find *customError in chain")
	}
	if got.Code != 42 {
		t.Fatalf("customError.Code = %d; want 42", got.Code)
	}
}

func TestStackError_FormatV(t *testing.T) {
	orig := fmt.Errorf("hello world")
	se := &StackError{
		err:    orig,
		frames: []Frame{{Function: "main.Foo", File: "/a/b.go", Line: 10}},
	}
	got := fmt.Sprintf("%v", se)
	if got != "hello world" {
		t.Fatalf("%%v = %q; want %q", got, "hello world")
	}
	gotS := fmt.Sprintf("%s", se)
	if gotS != "hello world" {
		t.Fatalf("%%s = %q; want %q", gotS, "hello world")
	}
}

func TestStackError_FormatPlusV(t *testing.T) {
	orig := fmt.Errorf("oops")
	se := &StackError{
		err: orig,
		frames: []Frame{
			{Function: "main.GetUser", File: "/src/user.go", Line: 15},
			{Function: "main.Handle", File: "/src/handler.go", Line: 28},
		},
	}
	got := fmt.Sprintf("%+v", se)
	if !strings.HasPrefix(got, "oops\n") {
		t.Fatalf("%%+v should start with error message, got: %q", got)
	}
	if !strings.Contains(got, "main.GetUser") {
		t.Fatalf("%%+v should contain function name, got: %q", got)
	}
	if !strings.Contains(got, "/src/user.go:15") {
		t.Fatalf("%%+v should contain file:line, got: %q", got)
	}
	if !strings.Contains(got, "main.Handle") {
		t.Fatalf("%%+v should contain second frame, got: %q", got)
	}
}

func TestStackError_Stack(t *testing.T) {
	err := WithStack(fmt.Errorf("test"))
	se := err.(*StackError)
	frames := se.Stack()
	if len(frames) == 0 {
		t.Fatal("Stack() returned empty frames")
	}
	// The first frame should reference this test function.
	found := false
	for _, f := range frames {
		if strings.Contains(f.Function, "TestStackError_Stack") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected a frame containing 'TestStackError_Stack', frames: %+v", frames)
	}
}

func TestStackTrace_Extract(t *testing.T) {
	orig := fmt.Errorf("extract me")
	err := WithStack(orig)
	frames := StackTrace(err)
	if len(frames) == 0 {
		t.Fatal("StackTrace returned empty frames for a *StackError")
	}
	found := false
	for _, f := range frames {
		if strings.Contains(f.Function, "TestStackTrace_Extract") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected frame with TestStackTrace_Extract, got: %+v", frames)
	}
}

func TestStackTrace_NonStackError(t *testing.T) {
	plain := fmt.Errorf("plain error")
	frames := StackTrace(plain)
	if frames != nil {
		t.Fatalf("StackTrace(plain) = %+v; want nil", frames)
	}
}

func TestCaptureFrames_FilterRuntime(t *testing.T) {
	err := WithStack(fmt.Errorf("filter test"))
	se := err.(*StackError)
	for _, f := range se.Stack() {
		if strings.HasPrefix(f.Function, "runtime.") {
			t.Fatalf("found runtime frame that should have been filtered: %+v", f)
		}
	}
}
