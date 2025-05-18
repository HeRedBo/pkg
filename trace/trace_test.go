package trace

import (
	"crypto/rand"
	"encoding/hex"
	"go.uber.org/zap"
	"io"
	"sync"
	"testing"
)

func TestNewTrace_GeneratesRandomID(t *testing.T) {
	// 测试 New() 函数在未提供ID时生成随机ID
	trace1 := New("")
	trace2 := New("")

	if trace1.ID() == "" || trace2.ID() == "" {
		t.Error("生成的Trace ID为空")
	}

	if trace1.ID() == trace2.ID() {
		t.Error("生成了重复的Trace ID")
	}
}

func TestNewTrace_UsesProvidedID(t *testing.T) {
	// 测试 New() 函数使用提供的ID
	expectedID := generateRandomID()
	trace := New(expectedID)
	if trace.ID() != expectedID {
		t.Errorf("期望的Trace ID是 %q，但得到了 %q", expectedID, trace.ID())
	}
}

func TestTrace_WithRequest(t *testing.T) {
	// 测试 WithRequest 方法
	trace := New("test-trace")
	req := &Request{
		Method:     "GET",
		DecodedURL: "https://example.com/api",
	}

	result := trace.WithRequest(req)

	if result.Request != req {
		t.Error("Request未正确设置")
	}
}

func TestTrace_WithResponse(t *testing.T) {
	// 测试 WithResponse 方法
	trace := New("test-trace")
	resp := &Response{
		HttpCode: 200,
		Body:     "成功",
	}

	result := trace.WithResponse(resp)

	if result.Response != resp {
		t.Error("Response未正确设置")
	}
}

func TestTrace_AppendDialog(t *testing.T) {
	// 测试 AppendDialog 方法的并发安全性
	trace := New("test-trace")
	dialog := &Dialog{
		Request: &Request{
			Method: "POST",
		},
	}

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trace.AppendDialog(dialog)
		}()
	}

	wg.Wait()

	if len(trace.ThirdPartyRequests) != numGoroutines {
		t.Errorf("期望添加 %d 个Dialog，但实际添加了 %d 个", numGoroutines, len(trace.ThirdPartyRequests))
	}
}

func TestTrace_AppendSQL(t *testing.T) {
	// 测试 AppendSQL 方法
	trace := New("test-trace")
	sql := &SQL{
		SQL:                   "SELECT * FROM users",
		AffectedRows:          10,
		CostMillisecond:       50,
		SlowLoggerMillisecond: 100,
	}

	result := trace.AppendSQL(sql)

	if len(result.SQLs) != 1 || result.SQLs[0] != sql {
		t.Error("SQL未正确添加")
	}
}

func TestTrace_AppendCache(t *testing.T) {
	// 测试 AppendCache 方法
	trace := New("test-trace")
	cache := &Cache{
		Name:                  "Redis",
		CMD:                   "GET",
		Key:                   "user:123",
		TTL:                   60,
		CostMillisecond:       5,
		SlowLoggerMillisecond: 10,
	}

	result := trace.AppendCache(cache)

	if len(result.Cache) != 1 || result.Cache[0] != cache {
		t.Error("Cache未正确添加")
	}
}

func TestDialog_AppendResponse(t *testing.T) {
	// 测试 Dialog 的 AppendResponse 方法的并发安全性
	dialog := &Dialog{}
	resp := &Response{
		HttpCode: 200,
	}

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dialog.AppendResponse(resp)
		}()
	}

	wg.Wait()

	if len(dialog.Responses) != numGoroutines {
		t.Errorf("期望添加 %d 个Response，但实际添加了 %d 个", numGoroutines, len(dialog.Responses))
	}
}

func TestTrace_SetLogger(t *testing.T) {
	// 测试 SetLogger 方法
	trace := New("test-trace")
	logger, _ := zap.NewDevelopment()

	trace.SetLogger(logger)

	if trace.Logger != logger {
		t.Error("Logger未正确设置")
	}
}

func TestTrace_SetAlwaysTrace(t *testing.T) {
	// 测试 SetAlwaysTrace 方法
	trace := New("test-trace")

	// 调用方法
	trace.SetAlwaysTrace(true)

	// 验证结果
	if !trace.AlwaysTrace {
		t.Error("AlwaysTrace未被设置为true")
	}
}

// 辅助函数：生成随机ID
func generateRandomID() string {
	buf := make([]byte, 10)
	io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}

// 测试案例 1：验证 New 函数生成 Trace ID 的逻辑
func TestNewTrace(t *testing.T) {
	// 测试自动生成 ID
	trace1 := New("")
	if trace1.ID() == "" {
		t.Error("Expected non-empty Trace ID, got empty")
	}

	// 测试自定义 ID
	trace2 := New("custom-id")
	if trace2.ID() != "custom-id" {
		t.Errorf("Expected ID 'custom-id', got '%s'", trace2.ID())
	}
}

// 测试案例 2：验证 WithRequest 和 WithResponse 方法
func TestTraceWithRequestAndResponse(t *testing.T) {
	trace := New("test-trace")
	req := &Request{Method: "GET", DecodedURL: "/api"}
	resp := &Response{HttpCode: 200}

	trace.WithRequest(req).WithResponse(resp)

	if trace.Request != req {
		t.Error("Request not set correctly")
	}
	if trace.Response != resp {
		t.Error("Response not set correctly")
	}
}

// 测试案例 3：测试并发安全追加 SQL 操作
func TestConcurrentAppendSQL(t *testing.T) {
	trace := New("concurrent-test")
	var wg sync.WaitGroup
	count := 100

	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			sql := &SQL{SQL: "SELECT 1"}
			trace.AppendSQL(sql)
		}()
	}
	wg.Wait()

	if len(trace.SQLs) != count {
		t.Errorf("Expected %d SQLs, got %d", count, len(trace.SQLs))
	}
}

// 测试案例 4：验证 Dialog 多次响应追加
func TestDialogAppendResponse(t *testing.T) {
	dialog := &Dialog{}
	resp1 := &Response{HttpCode: 500}
	resp2 := &Response{HttpCode: 200}

	dialog.AppendResponse(resp1)
	dialog.AppendResponse(resp2)

	if len(dialog.Responses) != 2 {
		t.Errorf("Expected 2 responses, got %d", len(dialog.Responses))
	}
	if dialog.Responses[1].HttpCode != 200 {
		t.Error("Last response should be 200")
	}
}

// 测试案例 5：测试日志关联性
func TestLoggerAssociation(t *testing.T) {
	logger := zap.NewNop() // 使用空日志防止输出干扰
	trace := New("logger-test")
	sql := &SQL{SQL: "SELECT 1"}

	trace.SetLogger(logger)
	trace.AppendSQL(sql)

	if trace.Logger != logger || sql.Logger != logger {
		t.Error("Logger not associated correctly")
	}
}
