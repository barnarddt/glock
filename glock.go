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
	Lock() context.Context
	Unlock()
}

type glock struct {
	gok       goku.Client
	key       string
	processID string
	mt        sync.Mutex
	waiting   bool
	locked    bool
	cancel    context.CancelFunc
	timechan  chan bool
}

func New(client goku.Client, key string) Glock {
	gl := &glock{
		gok:       client,
		key:       key,
		mt:        sync.Mutex{},
		processID: uuid.New().String(),
		timechan:  make(chan bool, 10),
	}

	go gl.streamer(context.Background())

	return gl
}

// Lock returns a context, the context will be cancelled if keep alive fails for the lock
// that you have acquired. If the the context gets cancelled and the lock is no longer guarenteed.
// Calling Unlock once the context is cancelled is harmless and will result in a no-op.
func (g *glock) Lock() context.Context {
	var (
		kv  goku.KV
		err error
	)

	g.mt.Lock()
	g.waiting = true

	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel

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
				goku.WithExpiresAt(time.Now().Add(time.Second*20)))
		} else {
			err = g.gok.Set(ctx, g.key, []byte("locked_"+g.processID),
				goku.WithPrevVersion(kv.Version),
				goku.WithExpiresAt(time.Now().Add(time.Second*20)))
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

		g.locked = true
		go g.keepAlive(ctx)
		return ctx
	}
}

// Unlock once called the context received from Lock will be cancelled
func (g *glock) Unlock() {
	if !g.locked {
		// Called Unlock without actually having the lock,
		// do nothing or risk overriding someone else's lock
		return
	}
	defer g.mt.Unlock()
	g.waiting = false

	for {
		// ensure the lock is ours
		kv, err := g.gok.Get(context.Background(), g.key)
		if err != nil {
			continue
		}

		// Sanity check, that the lock is ours
		if string(kv.Value) != "locked_"+g.processID {
			return
		}

		err = g.gok.Set(context.Background(), g.key, []byte("unlocked"))
		if err == nil {
			g.locked = false
			g.cancel()
			g.cancel = nil
			return
		}
	}
}

func (g *glock) keepAlive(ctx context.Context) {
	firstRun := true
	explicitFail := time.Now().Add(time.Second*15)
	for {
		if firstRun {
			firstRun = false
			time.Sleep(time.Second * 5)
		}
		if !g.locked || errors.Is(ctx.Err(), context.Canceled) {
			return
		}

		if time.Now().After(explicitFail) {
			// We have tried and tried but cant update the lease for whatever reason,
			// cancel the context
			if g.cancel != nil {
				g.cancel()
			}
			return
		}

		// ensure the lock is ours
		kv, err := g.gok.Get(ctx, g.key)
		if errors.Is(err, context.Canceled) {
			// Unlock must have cancelled the context, do nothing
			return
		} else if errors.Is(err, goku.ErrNotFound) {
			if g.cancel != nil {
				g.cancel()
			}
			return
		}
		if err != nil {
			time.Sleep(time.Millisecond+50) //  don't spin
			continue
		}

		// If we don't have the lock anymore cancel the context so whoever
		// thinks they have it can get notified.
		if string(kv.Value) != "locked_"+g.processID {
			if g.cancel != nil {
				g.cancel()
			}
			return
		}

		// Extend the lease on the lock by 20 seconds
		err = g.gok.UpdateLease(ctx, kv.LeaseID, time.Now().Add(time.Second*20))
		if errors.Is(err, context.Canceled) {
			// Unlock must have cancelled the context, do nothing
			return
		} else if err != nil {
			// retry immediately to extend the lease
			time.Sleep(time.Millisecond+50) //  don't spin
			continue
		}

		// Extend the lease for another 20 seconds every 5 seconds.
		explicitFail = time.Now().Add(time.Second*15)
		time.Sleep(time.Second * 5)
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
				g.waiting &&
				len(g.timechan) == 0 {
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
