package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mq/internal/broker"
)

func TestPublishFetchAndExpiredOffset(t *testing.T) {
	queue, err := broker.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := queue.Close(); err != nil {
			t.Error(err)
		}
	})
	handler, err := NewAuthenticatedHandlerWithAdmin(queue, "client-token", "admin-token")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	body := post(t, server.URL+"/v1/publish", "client-token", `{"topic":"events","key":"a2V5","value":"aGVsbG8="}`)
	var published struct {
		OK     bool   `json:"ok"`
		Offset uint64 `json:"offset"`
	}
	if err := json.Unmarshal(body, &published); err != nil || !published.OK || published.Offset != 0 {
		t.Fatalf("publish = %s", body)
	}

	body = post(t, server.URL+"/v1/fetch", "client-token", `{"topic":"events","offset":0,"limit":10}`)
	var fetched struct {
		OK      bool `json:"ok"`
		Records []struct {
			Offset uint64 `json:"offset"`
			Value  []byte `json:"value"`
		} `json:"records"`
	}
	if err := json.Unmarshal(body, &fetched); err != nil || !fetched.OK || len(fetched.Records) != 1 || string(fetched.Records[0].Value) != "hello" {
		t.Fatalf("fetch = %s", body)
	}

	retain := httptest.NewRequest(http.MethodPost, "/v1/topics/retain", bytes.NewBufferString(`{"topic":"events","start_offset":1}`))
	retain.Header.Set("Authorization", "Bearer admin-token")
	retain.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, retain)
	if response.Code != http.StatusOK {
		t.Fatalf("retain status = %d, body %s", response.Code, response.Body.String())
	}

	expired := httptest.NewRequest(http.MethodPost, "/v1/fetch", bytes.NewBufferString(`{"topic":"events","offset":0,"limit":1}`))
	expired.Header.Set("Authorization", "Bearer client-token")
	expired.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, expired)
	if response.Code != http.StatusGone {
		t.Fatalf("expired status = %d, body %s", response.Code, response.Body.String())
	}
}

func post(t *testing.T, url, token, payload string) []byte {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := readAll(response)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status %d: %s", url, response.StatusCode, body)
	}
	return body
}

func readAll(response *http.Response) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(response.Body)
	return buf.Bytes(), err
}
