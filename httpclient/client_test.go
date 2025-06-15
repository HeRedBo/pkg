package httpclient

import (
	"encoding/json"
	"github.com/gookit/goutil/dump"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// GET 请求测试（带查询参数）
func TestGet_WithQueryParams(t *testing.T) {
	// 模拟 HTTP 服务
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("name") == "test" && r.URL.Query().Get("age") == "18" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status": "ok"}`))
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	// 发起请求
	form := url.Values{"name": {"test"}, "age": {"18"}}
	code, body, err := Get(server.URL, form)
	// 验证结果
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("期望状态码 %d，实际 %d", http.StatusOK, code)
	}
	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	dump.P(result)
	if result["status"] != "ok" {
		t.Fatal("响应内容错误")
	}
}

func TestPostJSON_RetryOn500(t *testing.T) {
	// 模拟失败的 HTTP 服务（前两次返回 500，第三次返回 200）
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Server Error"))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Success"))
		}
	}))
	defer server.Close()

	// 配置重试参数

	code, body, err := PostJSON(
		server.URL,
		json.RawMessage(`{"key": "value"}`),
		WithOnFailedRetry(3, 10*time.Millisecond, nil),
	)

	// 验证重试是否成功
	if err != nil {
		t.Fatalf("重试失败: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("期望状态码 %d，实际 %d", http.StatusOK, code)
	}
	if string(body) != "Success" {
		t.Fatal("响应内容错误")
	}
	if attempts != 3 {
		t.Fatalf("期望重试 3 次，实际 %d 次", attempts)
	}
}
