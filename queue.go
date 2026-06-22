package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Q struct {
	sync.Mutex
	items   []string
	waiters []chan string
}

var (
	m  sync.RWMutex
	qs = make(map[string]*Q)
)

func gq(n string) *Q {
	m.RLock()
	q := qs[n]
	m.RUnlock()
	if q != nil {
		return q
	}
	m.Lock()
	defer m.Unlock()
	if q = qs[n]; q == nil {
		q = &Q{}
		qs[n] = q
	}
	return q
}

func h(w http.ResponseWriter, r *http.Request) {
	n := r.URL.Path[1:]
	if n == "" {
		w.WriteHeader(400)
		return
	}
	q := gq(n)
	switch r.Method {
	case "PUT":
		v := r.URL.Query().Get("v")
		if v == "" {
			w.WriteHeader(400)
			return
		}
		q.Lock()
		if len(q.waiters) > 0 {
			q.waiters[0] <- v
			q.waiters = q.waiters[1:]
		} else {
			q.items = append(q.items, v)
		}
		q.Unlock()
		w.WriteHeader(200)
	case "GET":
		var to time.Duration
		if t := r.URL.Query().Get("timeout"); t != "" {
			if n, e := strconv.Atoi(t); e == nil && n > 0 {
				to = time.Duration(n) * time.Second
			}
		}
		q.Lock()
		if len(q.items) > 0 {
			m := q.items[0]
			q.items = q.items[1:]
			q.Unlock()
			w.Write([]byte(m))
			return
		}
		if to == 0 {
			q.Unlock()
			w.WriteHeader(404)
			return
		}
		wc := make(chan string, 1)
		q.waiters = append(q.waiters, wc)
		q.Unlock()
		select {
		case m := <-wc:
			w.Write([]byte(m))
		case <-time.After(to):
			q.Lock()
			for i, c := range q.waiters {
				if c == wc {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
			q.Unlock()
			w.WriteHeader(404)
		}
	default:
		w.WriteHeader(405)
	}
}

func main() {
	p := ":8080"
	if len(os.Args) > 1 {
		p = ":" + os.Args[1]
	}

	srv := &http.Server{Addr: p}
	http.HandleFunc("/", h)

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
