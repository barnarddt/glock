package glock

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/corverroos/goku"
	"github.com/google/uuid"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/mock"
	"github.com/luno/reflex/rpatterns"
)

type Glock interface {
	Lock(ctx context.Context)
	Unlock(ctx context.Context)
}

type glock struct {
	gok       goku.Client
	key       string
	kv        goku.KV
	processID string
	mt        sync.Mutex
	waiting   bool
	timechan  chan bool
}

func New(client goku.Client, key string) (Glock, string) {
	client.Stream(key)
	gl := &glock{
		gok:       client,
		key:       key,
		mt:        sync.Mutex{},
		processID: uuid.New().String(),
		timechan:  make(chan bool, 1),
	}

	go gl.streamer(context.Background())

	return gl, gl.processID
}

func (g *glock) Lock(ctx context.Context) {
	var (
		kv  goku.KV
		err error
	)

	g.mt.Lock()
	g.waiting = true

	for {
		for {
			kv, err = g.gok.Get(ctx, g.key)
			if errors.Is(err, goku.ErrNotFound) {
				// First set so there's no key
				break
			}

			if err != nil {
				continue
			}

			if strings.HasPrefix(string(kv.Value), "locked") {
				g.awaitRetry()
			} else {
				break
			}
		}
		// Case for fist time trying to acquire lock.
		if err != nil {
			err = g.gok.Set(ctx, g.key, []byte("locked_"+g.processID),
				goku.WithCreateOnly(),
				goku.WithExpiresAt(time.Now().Add(time.Second*10)))
		} else {
			err = g.gok.Set(ctx, g.key, []byte("locked_"+g.processID),
				goku.WithPrevVersion(kv.Version),
				goku.WithExpiresAt(time.Now().Add(time.Second*10)))
		}

		if err != nil {
			// retry
			continue
		}

		// ensure the lock is ours
		kv, err = g.gok.Get(ctx, g.key)
		if err != nil {
			continue
		}

		// Sanity check, paranoid
		if string(kv.Value) != "locked_"+g.processID {
			continue
		}

		return
	}
}

func (g *glock) Unlock(ctx context.Context) {
	defer g.mt.Unlock()
	g.waiting = false

	for {
		err := g.gok.Set(ctx, g.key, []byte("unlocked"))
		if err == nil {
			g.kv = goku.KV{}
			return
		}
	}
}

func (g *glock) streamer(ctx context.Context) {
	sf := g.gok.Stream(g.key)

	cons := reflex.NewConsumer("lock_event_consumer",
		func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
			if reflex.IsAnyType(event.Type,
				goku.EventTypeDelete,
				goku.EventTypeExpire,
				goku.EventTypeSet) &&
				g.waiting {
				g.timechan <- true
			}
			return nil
		})

	rpatterns.RunForever(
		func() context.Context {
			return ctx
		},
		reflex.NewSpec(
			sf,
			mock.NewMockCStore(),
			cons,
			reflex.WithStreamFromHead(),
		),
	)
}

func (g *glock) awaitRetry() {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()

	// Retry lock every second or when an unlock event is fired
	select {
	case <-timeout:
		return
	case <-g.timechan:
		return
	}
}
