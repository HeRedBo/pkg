// Package stackerr provides zero-dependency stack trace capture for Go errors.
//
// It wraps errors with call stack information without relying on any third-party
// libraries or even the standard "errors" package. Error chain traversal is done
// via the Unwrap() method, making it compatible with fmt.Errorf %w wrapping.
package stackerr

import (
	"fmt"
	"runtime"
	"strings"
)

// Frame represents a single stack frame in a call trace.
type Frame struct {
	Function string // Fully qualified function name (e.g. "main.GetUser")
	File     string // Source file path
	Line     int    // Line number within the source file
}

// String returns a human-readable representation of the frame.
func (f Frame) String() string {
	return fmt.Sprintf("%s\n\t%s:%d", f.Function, f.File, f.Line)
}

// StackError wraps an error together with the call stack that was captured
// at the point where the error was first annotated.
type StackError struct {
	err    error
	frames []Frame
}

// Error returns the underlying error message.
func (se *StackError) Error() string {
	return se.err.Error()
}

// Unwrap returns the next error in the chain, enabling compatibility with
// fmt.Errorf("%w") error wrapping and manual chain traversal.
func (se *StackError) Unwrap() error {
	return se.err
}

// Format implements fmt.Formatter.
//
//	%v / %s  — prints only the error message
//	%+v      — prints the error message followed by the full stack trace
func (se *StackError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s\n", se.err.Error())
			for _, f := range se.frames {
				fmt.Fprintf(s, "%s\n", f)
			}
			return
		}
		fmt.Fprintf(s, "%s", se.err.Error())
	case 's':
		fmt.Fprintf(s, "%s", se.err.Error())
	case 'q':
		fmt.Fprintf(s, "%q", se.err.Error())
	default:
		fmt.Fprintf(s, "%%!%c(*StackError)", verb)
	}
}

// Stack returns a copy of the captured stack frames.
// The returned slice can be safely modified by the caller.
func (se *StackError) Stack() []Frame {
	out := make([]Frame, len(se.frames))
	copy(out, se.frames)
	return out
}

// WithStack annotates err with a stack trace if it is not nil.
//
// If err is nil, WithStack returns nil.
// If err already carries a stack trace (i.e. it is or wraps a *StackError),
// the original err is returned unchanged — double wrapping is avoided.
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	if hasStack(err) {
		return err
	}
	return &StackError{
		err:    err,
		frames: captureFrames(2, 32),
	}
}

// Wrapf annotates err with a formatted message prefix and a stack trace.
//
// If err is nil, Wrapf returns nil.
// The returned error's message is "prefix: original", where prefix is produced
// by fmt.Sprintf(format, args...).
// A new stack trace is captured at the call site of Wrapf.
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	prefix := fmt.Sprintf(format, args...)
	wrapped := fmt.Errorf("%s: %w", prefix, err)
	return &StackError{
		err:    wrapped,
		frames: captureFrames(2, 32),
	}
}

// StackTrace extracts stack frames from err.
//
// It walks the error chain via Unwrap() looking for:
//   - a *StackError, whose frames are returned directly
//   - any error implementing Stack() []Frame, whose frames are returned
//
// If no stack-carrying error is found, nil is returned.
func StackTrace(err error) []Frame {
	for err != nil {
		switch e := err.(type) {
		case *StackError:
			return e.Stack()
		case interface{ Stack() []Frame }:
			return e.Stack()
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}
		err = u.Unwrap()
	}
	return nil
}

// hasStack reports whether err or any error in its chain already carries
// stack trace information. Traversal is done via Unwrap() without importing
// the "errors" package.
func hasStack(err error) bool {
	for err != nil {
		switch err.(type) {
		case *StackError:
			return true
		case interface{ Stack() []Frame }:
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// captureFrames collects up to max program counters starting from the caller
// at depth skip, then resolves them into Frame values. Frames belonging to
// the Go runtime (function name starts with "runtime.") are filtered out.
func captureFrames(skip, max int) []Frame {
	pcs := make([]uintptr, max)
	n := runtime.Callers(skip, pcs)
	if n == 0 {
		return nil
	}
	pcs = pcs[:n]

	var frames []Frame
	cf := runtime.CallersFrames(pcs)
	for {
		frame, more := cf.Next()
		if !isRuntimeFunc(frame.Function) {
			frames = append(frames, Frame{
				Function: frame.Function,
				File:     frame.File,
				Line:     frame.Line,
			})
		}
		if !more {
			break
		}
	}
	return frames
}

// isRuntimeFunc reports whether fn is a Go runtime internal function.
func isRuntimeFunc(fn string) bool {
	return strings.HasPrefix(fn, "runtime.")
}
