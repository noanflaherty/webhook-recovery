/**
 * Everything that moves, in one place.
 *
 * Five pollers at four cadences, one metrics buffer, one interpolated clock.
 * Components below this are pure functions of what it returns, which is what
 * keeps the live and replay sources genuinely interchangeable -- neither of them
 * knows anything about timers.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import type { DataSource } from '../api/source'
import type {
  ConsumerRead,
  DecisionRead,
  MetricsBucket,
  ProcessRead,
  SimulationPatch,
  SimulationRead,
} from '../api/types'
import { consumersFrom, mergeBuckets, type ConsumerRef } from '../transform/series'

/** The simulation row is small and drives the clock, so it is polled hardest. */
const SIMULATION_MS = 500
const METRICS_MS = 1000
const CONSUMERS_MS = 1000
const DECISIONS_MS = 1500
const PROCESSES_MS = 3000
/** How often the interpolated clock re-renders. Cosmetic, so 10Hz is plenty. */
const CLOCK_MS = 100

/**
 * How many already-read buckets to re-request each poll.
 *
 * The conductor writes a bucket only once it is complete and then *upserts*, so
 * a bucket can be rewritten after this client first read it -- a failover
 * backfilling a gap, or a worker's `started_at` committing just after the
 * window that counted it was served (`app/conductor/metrics.py` lags two
 * buckets for exactly this reason). Following `next_since_bucket` strictly
 * would never re-fetch those, leaving the stale copy on the chart forever.
 *
 * Two buckets, to match the writer's own lag. `mergeBuckets` makes the overlap
 * free: re-reading a bucket that did not change is a no-op.
 */
const CURSOR_OVERLAP = 2

/** Everything that belongs to one run, so switching runs is one assignment. */
interface Snapshot {
  simulation: SimulationRead | null
  consumers: ConsumerRead[]
  buckets: MetricsBucket[]
  decisions: DecisionRead[]
  processes: ProcessRead[]
  error: string | null
  /** True until the first simulation response lands. */
  loading: boolean
}

const EMPTY: Snapshot = {
  simulation: null,
  consumers: [],
  buckets: [],
  decisions: [],
  processes: [],
  error: null,
  loading: true,
}

export interface RunState extends Snapshot {
  consumerRefs: ConsumerRef[]
  /** Interpolated between simulation polls, so the clock reads smoothly. */
  virtualNowS: number
  patch: (body: SimulationPatch) => Promise<void>
}

interface ClockAnchor {
  virtualS: number
  wallMs: number
  speed: number
  running: boolean
}

const STOPPED: ClockAnchor = { virtualS: 0, wallMs: 0, speed: 1, running: false }

/**
 * `source` is nullable so the cold-landing screen -- which has no run to poll --
 * can render without this hook being called conditionally.
 */
export function useRun(source: DataSource | null): RunState {
  const [snapshot, setSnapshot] = useState<Snapshot>(EMPTY)
  const [virtualNowS, setVirtualNowS] = useState(0)
  const anchorRef = useRef<ClockAnchor>(STOPPED)
  const cursorRef = useRef(-1)

  // Reset during render rather than in an effect. Bucket indices from one
  // simulation mean nothing in another, so the old run's data must not survive
  // even the single frame an effect-based reset would leave it on screen for.
  // The two refs are reset alongside the pollers that own them, below.
  const [renderedSource, setRenderedSource] = useState(source)
  if (source !== renderedSource) {
    setRenderedSource(source)
    setSnapshot(EMPTY)
    setVirtualNowS(0)
  }

  const anchorTo = useCallback((next: SimulationRead) => {
    anchorRef.current = {
      virtualS: next.virtual_now_s,
      wallMs: Date.now(),
      speed: next.speed_multiplier,
      running: next.status === 'running',
    }
    setVirtualNowS(next.virtual_now_s)
  }, [])

  // One effect owns every timer, so there is exactly one place a poll can leak.
  useEffect(() => {
    // Stopping the clock here rather than during render is what keeps the
    // interpolator from advancing a new run's clock from the old run's anchor;
    // the effect commits long before the 100ms tick could fire.
    cursorRef.current = -1
    anchorRef.current = STOPPED
    if (!source) return
    let cancelled = false

    const report = (err: unknown) => {
      if (!cancelled) {
        setSnapshot((s) => ({ ...s, error: err instanceof Error ? err.message : String(err) }))
      }
    }

    /**
     * Run `fn` now and then every `everyMs`, never twice at once.
     *
     * The guard matters at 20x on a slow connection: without it a poll that
     * takes longer than its interval queues another behind it, and the queue
     * only grows. Skipping a tick is the right answer -- the next one carries
     * the same information.
     */
    const start = (fn: () => Promise<void>, everyMs: number): (() => void) => {
      let inFlight = false
      const run = async () => {
        if (inFlight || cancelled) return
        inFlight = true
        try {
          await fn()
        } catch (err) {
          report(err)
        } finally {
          inFlight = false
        }
      }
      void run()
      const timer = setInterval(() => void run(), everyMs)
      return () => clearInterval(timer)
    }

    const stops = [
      start(async () => {
        const simulation = await source.getSimulation()
        if (cancelled) return
        anchorTo(simulation)
        setSnapshot((s) => ({ ...s, simulation, loading: false, error: null }))
      }, SIMULATION_MS),

      start(async () => {
        const page = await source.getMetrics(Math.max(-1, cursorRef.current - CURSOR_OVERLAP))
        if (cancelled) return
        cursorRef.current = Math.max(cursorRef.current, page.next_since_bucket)
        if (page.buckets.length === 0) return
        setSnapshot((s) => ({ ...s, buckets: mergeBuckets(s.buckets, page.buckets) }))
      }, METRICS_MS),

      start(async () => {
        const consumers = await source.getConsumers()
        if (!cancelled) setSnapshot((s) => ({ ...s, consumers }))
      }, CONSUMERS_MS),

      start(async () => {
        const page = await source.getDecisions()
        if (!cancelled) setSnapshot((s) => ({ ...s, decisions: page.decisions }))
      }, DECISIONS_MS),

      start(async () => {
        const processes = await source.getProcesses()
        if (!cancelled) setSnapshot((s) => ({ ...s, processes }))
      }, PROCESSES_MS),
    ]

    return () => {
      cancelled = true
      for (const stop of stops) stop()
    }
  }, [source, anchorTo])

  // The clock between polls. Virtual time is wall time times a multiplier --
  // the same arithmetic as `app/core/clock.py`, from the same shared epoch --
  // so this is not a guess at what the server will say next, and each poll
  // snaps it back to remove accumulated drift.
  useEffect(() => {
    const timer = setInterval(() => {
      const anchor = anchorRef.current
      if (!anchor.running) return
      setVirtualNowS(anchor.virtualS + ((Date.now() - anchor.wallMs) / 1000) * anchor.speed)
    }, CLOCK_MS)
    return () => clearInterval(timer)
  }, [])

  const patch = useCallback(
    async (body: SimulationPatch) => {
      if (!source) return
      try {
        const simulation = await source.patch(body)
        anchorTo(simulation)
        setSnapshot((s) => ({ ...s, simulation, error: null }))
      } catch (err) {
        setSnapshot((s) => ({ ...s, error: err instanceof Error ? err.message : String(err) }))
      }
    },
    [source, anchorTo],
  )

  // The consumer list is authoritative for identity and order; the buckets are
  // the fallback, so the charts still render if that request is the one failing.
  const consumerRefs = useMemo<ConsumerRef[]>(() => {
    if (snapshot.consumers.length > 0) {
      return snapshot.consumers.map((c) => ({ id: c.id, name: c.name }))
    }
    return consumersFrom(snapshot.buckets)
  }, [snapshot.consumers, snapshot.buckets])

  return { ...snapshot, consumerRefs, virtualNowS, patch }
}
