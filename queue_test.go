package main

import (
	// "io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func req(m, p, q string) *http.Request {
	r, _ := http.NewRequest(m, "http://x"+p, nil)
	r.URL.RawQuery = q
	return r
}

func TestQueue(t *testing.T) {
	queues = make(map[string]*Queue) // reset

	// PUT без v → 400
	w := httptest.NewRecorder()
	qHandler(w, req("PUT", "/test", ""))
	if w.Code != 400 {
		t.Errorf("PUT no v: got %d", w.Code)
	}

	// PUT → 200
	w = httptest.NewRecorder()
	qHandler(w, req("PUT", "/test", "v=a"))
	if w.Code != 200 {
		t.Errorf("PUT a: got %d", w.Code)
	}
	w = httptest.NewRecorder()
	qHandler(w, req("PUT", "/test", "v=b"))
	if w.Code != 200 {
		t.Errorf("PUT b: got %d", w.Code)
	}

	// GET FIFO
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/test", ""))
	if w.Code != 200 || w.Body.String() != "a" {
		t.Errorf("GET a: %d/%s", w.Code, w.Body)
	}
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/test", ""))
	if w.Code != 200 || w.Body.String() != "b" {
		t.Errorf("GET b: %d/%s", w.Code, w.Body)
	}

	// GET из пустой → 404
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/test", ""))
	if w.Code != 404 {
		t.Errorf("GET empty: got %d", w.Code)
	}

	// GET с timeout=1, сообщение приходит позже → 200
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		w2 := httptest.NewRecorder()
		qHandler(w2, req("PUT", "/t2", "v=late"))
	}()
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/t2", "timeout=2"))
	if w.Code != 200 || w.Body.String() != "late" {
		t.Errorf("GET late: %d/%s", w.Code, w.Body)
	}
	wg.Wait()

	// GET с timeout=1, сообщение приходит позже → 1500
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1500 * time.Millisecond)
		w2 := httptest.NewRecorder()
		qHandler(w2, req("PUT", "/t2", "v=late"))
	}()
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/t2", "timeout=1"))
	if w.Code != 404 || w.Body.String() == "late" {
		t.Errorf("GET late: %d/%s", w.Code, w.Body)
	}
	wg.Wait()

	// GET с timeout=1, сообщение не приходит → 404
	w = httptest.NewRecorder()
	qHandler(w, req("GET", "/t3", "timeout=1"))
	if w.Code != 404 {
		t.Errorf("GET timeout: got %d", w.Code)
	}

	// Два получателя с timeout: первый запрос → первое сообщение
	queues["t4"] = &Queue{}
	var res1, res2 string
	var done1, done2 bool
	var mu sync.Mutex

	wg.Add(2)
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		qHandler(w, req("GET", "/t4", "timeout=3"))
		mu.Lock()
		if w.Code == 200 {
			res1 = w.Body.String()
			done1 = true
		}
		mu.Unlock()
	}()
	time.Sleep(50 * time.Millisecond) // гарантируем порядок подписки
	go func() {
		defer wg.Done()
		w := httptest.NewRecorder()
		qHandler(w, req("GET", "/t4", "timeout=3"))
		mu.Lock()
		if w.Code == 200 {
			res2 = w.Body.String()
			done2 = true
		}
		mu.Unlock()
	}()
	time.Sleep(100 * time.Millisecond) // оба подписались
	// Два PUT подряд
	qHandler(httptest.NewRecorder(), req("PUT", "/t4", "v=first"))
	qHandler(httptest.NewRecorder(), req("PUT", "/t4", "v=second"))
	wg.Wait()
	if !done1 || !done2 {
		t.Error("waiters didn't receive")
	}
	if res1 != "first" || res2 != "second" {
		t.Errorf("order: %s/%s", res1, res2)
	}
}

func TestEmptyQueueName(t *testing.T) {
	w := httptest.NewRecorder()
	qHandler(w, req("GET", "/", ""))
	if w.Code != 400 {
		t.Errorf("empty queue: got %d", w.Code)
	}
}

func TestConcurrentPutGet(t *testing.T) {
	queues = make(map[string]*Queue)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			w := httptest.NewRecorder()
			qHandler(w, req("PUT", "/conc", "v="+string(rune('0'+n))))
		}(i)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			qHandler(w, req("GET", "/conc", "timeout=1"))
		}()
	}
	wg.Wait()
	// Проверяем, что очередь пуста
	w := httptest.NewRecorder()
	qHandler(w, req("GET", "/conc", ""))
	if w.Code != 404 {
		t.Errorf("conc end: got %d", w.Code)
	}
}
