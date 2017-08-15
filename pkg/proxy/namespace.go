// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type NamespaceCmdStats struct {
	sync.RWMutex
	opmap map[string]*opStats
	total atomic2.Int64
	fails atomic2.Int64
	redis struct {
		errors atomic2.Int64
	}

	qps atomic2.Int64
}

type Namespace struct {
	Id        string
	Password  string
	KeyPrefix []byte
	stats     *NamespaceCmdStats
	sessions  struct {
		total atomic2.Int64
		alive atomic2.Int64
	}
}

func NewNamespace(id, password string, keyPrefix []byte) *Namespace {
	st := &NamespaceCmdStats{
		opmap: make(map[string]*opStats, 128),
	}
	ns := Namespace{
		Id:        id,
		Password:  password,
		KeyPrefix: keyPrefix,
		stats:     st,
	}
	go func() {
		for {
			start := time.Now()
			total := st.total.Int64()
			time.Sleep(time.Second)
			delta := st.total.Int64() - total
			normalized := math.Max(0, float64(delta)) * float64(time.Second) / float64(time.Since(start))
			st.qps.Set(int64(normalized + 0.5))
		}
	}()
	return &ns
}
func (ns *Namespace) OpTotal() int64 {
	return ns.stats.total.Int64()
}

func (ns *Namespace) OpFails() int64 {
	return ns.stats.fails.Int64()
}

func (ns *Namespace) OpRedisErrors() int64 {
	return ns.stats.redis.errors.Int64()
}

func (ns *Namespace) OpQPS() int64 {
	return ns.stats.qps.Int64()
}

func (ns *Namespace) getOpStats(opstr string, create bool) *opStats {
	ns.stats.RLock()
	s := ns.stats.opmap[opstr]
	ns.stats.RUnlock()

	if s != nil || !create {
		return s
	}

	ns.stats.Lock()
	s = ns.stats.opmap[opstr]
	if s == nil {
		s = &opStats{opstr: opstr}
		ns.stats.opmap[opstr] = s
	}
	ns.stats.Unlock()
	return s
}

func (ns *Namespace) GetOpStatsAll() []*OpStats {
	var all = make([]*OpStats, 0, 128)
	ns.stats.RLock()
	for _, s := range ns.stats.opmap {
		all = append(all, s.OpStats())
	}
	ns.stats.RUnlock()
	sort.Sort(sliceOpStats(all))
	return all
}
func (ns *Namespace) ResetStats() {
	ns.stats.Lock()
	ns.stats.opmap = make(map[string]*opStats, 128)
	ns.stats.Unlock()

	ns.stats.total.Set(0)
	ns.stats.fails.Set(0)
	ns.stats.redis.errors.Set(0)
	ns.sessions.total.Set(sessions.alive.Int64())
}

func (ns *Namespace) incrSessions() int64 {
	ns.sessions.total.Incr()
	return ns.sessions.alive.Incr()
}

func (ns *Namespace) decrSessions() {
	ns.sessions.alive.Decr()
}

func (ns *Namespace) SessionsTotal() int64 {
	return ns.sessions.total.Int64()
}

func (ns *Namespace) SessionsAlive() int64 {
	return ns.sessions.alive.Int64()
}

func (ns *Namespace) incrOpTotal(n int64) {
	ns.stats.total.Add(n)
}

func (ns *Namespace) incrOpStats(e *opStats) {
	s := ns.getOpStats(e.opstr, true)
	s.calls.Add(e.calls.Int64())
	s.nsecs.Add(e.nsecs.Int64())
	if n := e.fails.Int64(); n != 0 {
		s.fails.Add(n)
		ns.stats.fails.Add(n)
	}
	if n := e.redis.errors.Int64(); n != 0 {
		s.redis.errors.Add(n)
		ns.stats.redis.errors.Add(n)
	}
}
