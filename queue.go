package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Queue struct {
	sync.Mutex
	items   []string
	waiters []chan string
}

var (
	m             sync.RWMutex
	queues        = make(map[string]*Queue)
	NotFoundError = errors.New("not found")
)

func (q *Queue) Put(v string) {
	q.Lock()
	defer q.Unlock()
	if len(q.waiters) > 0 {
		q.waiters[0] <- v
		q.waiters = q.waiters[1:]
	} else {
		q.items = append(q.items, v)
	}
}

func (q *Queue) Get(timeout time.Duration) (string, error) {
	q.Lock()
	if len(q.items) > 0 {
		m := q.items[0]
		q.items = q.items[1:]
		q.Unlock()
		return m, nil
	}
	if timeout == 0 {
		q.Unlock()
		return "", NotFoundError
	}
	wc := make(chan string, 1)
	q.waiters = append(q.waiters, wc)
	q.Unlock()
	select {
	case m := <-wc:
		return m, nil
	case <-time.After(timeout):
		q.Lock()
		select {
		case m := <-wc:
			q.Unlock()
			return m, nil
		default:
		}
		for i, c := range q.waiters {
			if c == wc {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		q.Unlock()
		return "", NotFoundError
	}
}

func GetQueue(name string) *Queue {
	m.RLock()
	q := queues[name]
	m.RUnlock()
	if q != nil {
		return q
	}
	m.Lock()
	defer m.Unlock()
	if q = queues[name]; q == nil {
		q = &Queue{}
		queues[name] = q
	}
	return q
}

func handler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[1:]
	if name == "" {
		w.WriteHeader(400)
		return
	}
	q := GetQueue(name)
	switch r.Method {
	case "PUT":
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(400)
			return
		}
		q.Put(v)
		w.WriteHeader(200)
	case "GET":
		var timeout time.Duration
		if t := r.URL.Query().Get("timeout"); t != "" {
			if n, e := strconv.Atoi(t); e == nil && n > 0 {
				timeout = time.Duration(n) * time.Second
			}
		}
		if m, e := q.Get(timeout); e == nil {
			w.Write([]byte(m))
			w.WriteHeader(200)
		} else {
			w.WriteHeader(404)
		}
	default:
		w.WriteHeader(405)
	}
}

func main() {
	port := ":8080"
	if len(os.Args) > 1 {
		port = ":" + os.Args[1]
	}

	srv := &http.Server{Addr: port}
	http.HandleFunc("/", handler)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ListenAndServe error: %v", err)
		}
		stop()
	}()

	log.Println("Server started")
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Shutdown error: %v", err)
		return
	}
	log.Println("Shutdown complete")

}
