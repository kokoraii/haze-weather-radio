package productrender

import (
	"context"
	"log"
	"sync"
	"time"
)

const renderWorkBacklog = 64

// Each job owns a read-only configuration snapshot. Only the dispatcher reloads
// configuration; workers never mutate a snapshot used by another job.
type renderWork struct {
	cfg     loadedConfig
	event   map[string]any
	cleanup time.Time
}

type renderWorkPool struct {
	jobs chan renderWork
	wg   sync.WaitGroup
}

func startRenderWorkPool(ctx context.Context, workers int, handle func(renderWork)) *renderWorkPool {
	p := &renderWorkPool{jobs: make(chan renderWork, renderWorkBacklog)}
	for range workers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-p.jobs:
					if ctx.Err() != nil {
						return
					}
					handle(job)
				}
			}
		}()
	}
	return p
}

func (p *renderWorkPool) trySubmit(job renderWork) bool {
	select {
	case p.jobs <- job:
		return true
	default:
		return false
	}
}

type renderWorkers struct {
	routine     *renderWorkPool
	interactive *renderWorkPool
	alerts      *renderWorkPool
}

func (s *Service) startWorkers(ctx context.Context) renderWorkers {
	handleRender := func(job renderWork) {
		worker := Service{cfg: job.cfg, bridge: s.bridge}
		worker.handleEvent(job.event)
	}
	// One CAP worker preserves update/cancellation order and owns tone dedupe.
	// Maintenance shares that lane so cleanup cannot race an alert update.
	alertWorker := &Service{bridge: s.bridge, options: Options{BridgeAddr: s.options.BridgeAddr}}
	handleAlert := func(job renderWork) {
		alertWorker.cfg = job.cfg
		if !job.cleanup.IsZero() {
			alertWorker.runScheduledCleanup(job.cleanup)
			return
		}
		alertWorker.handleCAPAlert(job.event)
	}
	workers := s.cfg.Root.Services.Go.ProductRender.Workers
	if workers <= 0 {
		workers = 4
	}
	workers = min(workers, 32)
	log.Printf("product render workers routine=%d interactive=2 alerts=1 backlog=%d", workers, renderWorkBacklog)
	return renderWorkers{
		routine:     startRenderWorkPool(ctx, workers, handleRender),
		interactive: startRenderWorkPool(ctx, 2, handleRender),
		alerts:      startRenderWorkPool(ctx, 1, handleAlert),
	}
}

func (w renderWorkers) wait() {
	w.routine.wg.Wait()
	w.interactive.wg.Wait()
	w.alerts.wg.Wait()
}

func (s *Service) dispatchWork(ctx context.Context, workers renderWorkers, event map[string]any) {
	s.refreshConfigIfNeeded()
	job := renderWork{cfg: s.cfg, event: event}
	switch stringAt(event, "type") {
	case "cap.alert.received":
		// CAP updates are never discarded when the bounded queue fills.
		select {
		case workers.alerts.jobs <- job:
		case <-ctx.Done():
		}
	case "product.render.request":
		if !workers.routine.trySubmit(job) {
			s.publishFailed(renderRequestFromEvent(event), "routine render queue is full; retry")
		}
	case "wx.on_demand.request":
		if !workers.interactive.trySubmit(job) {
			s.publishWxFailed(wxOnDemandRequestFromEvent(event), "on-demand render queue is full; retry")
		}
	case "lead.config.updated":
		s.refreshConfigNow()
	}
}
