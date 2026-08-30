/**
 * These run against a constructed series rather than toy input.
 *
 * They used to import the committed replay fixtures — half a megabyte of
 * recorded run — for one reason: a rule that is subtly wrong (an off-by-one
 * window boundary, a gauge summed as a counter) fails against realistic data
 * and passes against three hand-written rows. The recording is gone, so the
 * shape is built here instead, and building it is an improvement on importing
 * it: the properties these tests actually depend on are now stated in the file
 * that depends on them rather than being emergent facts about an opaque blob.
 *
 * Four of those properties are load-bearing:
 *
 * - **A window where nobody attempts anything.** The outage. Without it the
 *   divide-by-zero case in `toShareWindows` is never exercised, which is the
 *   one it is most likely to get wrong.
 * - **Backlogs that reach zero at three different times**, so
 *   `deriveCaughtUpAfter` has something to be right or wrong about per
 *   consumer rather than globally.
 * - **Per-second buckets**, matching `metrics_bucket_virtual_s`.
 * - **Counters and gauges behaving differently** — `attempts` accumulates,
 *   `backlog` does not — because conflating them is the bug class these
 *   transforms exist to avoid.
 */
import { describe, expect, it } from 'vitest'

import type { MetricsBucket } from '../api/types'
import { OUTAGE_ENDS_AT_S, OUTAGE_STARTS_AT_S } from '../scenario'
import {
  consumersFrom,
  deriveCaughtUpAfter,
  mergeBuckets,
  toShareWindows,
  toWideSeries,
} from './series'

/** Long enough that the slowest consumer below finishes draining inside it. */
const RUN_LENGTH_S = 800

interface Spec {
  id: number
  name: string
  /** Events arriving per virtual second, all run long. */
  arrivalRate: number
  /** Attempts per virtual second once the provider is back. */
  drainRate: number
}

/** Three rates chosen to drain at three clearly separated times. */
const SPECS: Spec[] = [
  { id: 1, name: 'Acme Analytics', arrivalRate: 6, drainRate: 14 },
  { id: 2, name: 'Bolt Billing', arrivalRate: 6, drainRate: 20 },
  { id: 3, name: 'Clover CRM', arrivalRate: 0.8, drainRate: 10 },
]

function build(): { buckets: MetricsBucket[]; caughtUpAfter: Map<number, number> } {
  const buckets: MetricsBucket[] = []
  const caughtUpAfter = new Map<number, number>()
  const backlog = new Map(SPECS.map((s) => [s.id, 0]))

  for (let t = 0; t < RUN_LENGTH_S; t += 1) {
    const outage = t >= OUTAGE_STARTS_AT_S && t < OUTAGE_ENDS_AT_S
    for (const spec of SPECS) {
      const depth = backlog.get(spec.id)! + spec.arrivalRate
      // Nothing is attempted while the provider is down. Events still land in
      // the ledger, which is what makes the backlog climb through it.
      const attempts = outage ? 0 : Math.min(Math.floor(depth), spec.drainRate)
      const left = depth - attempts
      backlog.set(spec.id, left)

      if (left < 1 && t >= OUTAGE_ENDS_AT_S && !caughtUpAfter.has(spec.id)) {
        caughtUpAfter.set(spec.id, t - OUTAGE_ENDS_AT_S)
      }

      buckets.push({
        consumer_id: spec.id,
        consumer_name: spec.name,
        bucket_virtual_s: t,
        backlog: Math.floor(left),
        ready: outage ? 0 : Math.min(Math.floor(left), 8),
        in_flight: Math.min(attempts, 8),
        attempts,
        delivered: attempts,
        expired: 0,
        superseded: 0,
        failed: 0,
      })
    }
  }
  return { buckets, caughtUpAfter }
}

const { buckets: BUCKETS, caughtUpAfter: CAUGHT_UP } = build()
const REFS = consumersFrom(BUCKETS)

describe('the constructed series', () => {
  it('has the properties the tests below depend on', () => {
    expect(REFS).toHaveLength(SPECS.length)
    expect(CAUGHT_UP.size).toBe(SPECS.length)
    // Three distinct catch-up times, so per-consumer assertions cannot pass by
    // accident on a transform that returns the same answer for everyone.
    expect(new Set(CAUGHT_UP.values()).size).toBe(SPECS.length)
    expect(BUCKETS.some((b) => b.attempts > 0)).toBe(true)
    expect(
      BUCKETS.filter((b) => b.bucket_virtual_s === OUTAGE_STARTS_AT_S).every(
        (b) => b.attempts === 0,
      ),
    ).toBe(true)
  })
})

describe('toShareWindows', () => {
  it('conserves the raw attempt count', () => {
    const raw = BUCKETS.reduce((sum, b) => sum + b.attempts, 0)
    const windowed = toShareWindows(BUCKETS, REFS).reduce((sum, w) => sum + w.total, 0)
    expect(windowed).toBe(raw)
    expect(raw).toBeGreaterThan(0)
  })

  it('produces shares that sum to one, and never a NaN', () => {
    const windows = toShareWindows(BUCKETS, REFS)
    expect(windows.length).toBeGreaterThan(0)

    let scored = 0
    for (const window of windows) {
      const values = REFS.map((ref) => window[ref.name])
      for (const value of values) {
        expect(Number.isNaN(value)).toBe(false)
      }
      if (window.total === 0) {
        // The whole outage lands here: nobody attempts anything while the
        // provider is down, so the chart must break rather than divide by zero.
        expect(values.every((v) => v === null)).toBe(true)
        continue
      }
      scored += 1
      const sum = values.reduce<number>((acc, v) => acc + (v ?? 0), 0)
      expect(sum).toBeCloseTo(1, 9)
    }
    // Guard against the assertion above being vacuous.
    expect(scored).toBeGreaterThan(0)
    expect(windows.some((w) => w.total === 0)).toBe(true)
  })
})

describe('mergeBuckets', () => {
  it('replaces a revised bucket instead of duplicating it', () => {
    const first = BUCKETS.slice(0, 6)
    const revised: MetricsBucket = { ...first[0], attempts: first[0].attempts + 99 }

    const merged = mergeBuckets(first, [revised])

    expect(merged).toHaveLength(first.length)
    const match = merged.filter(
      (b) => b.bucket_virtual_s === revised.bucket_virtual_s && b.consumer_id === revised.consumer_id,
    )
    expect(match).toHaveLength(1)
    expect(match[0].attempts).toBe(revised.attempts)
  })

  it('keeps the buffer sorted when a page arrives out of order', () => {
    const merged = mergeBuckets([], [...BUCKETS].reverse())
    const keys = merged.map((b) => b.bucket_virtual_s * 1000 + b.consumer_id)
    expect(keys).toEqual([...keys].sort((a, b) => a - b))
    expect(merged).toHaveLength(BUCKETS.length)
  })
})

describe('deriveCaughtUpAfter', () => {
  it('agrees with the series it was built from', () => {
    for (const spec of SPECS) {
      const derived = deriveCaughtUpAfter(BUCKETS, spec.id)
      expect(derived).not.toBeNull()
      // Within one bucket: the construction knows the exact instant the
      // backlog crossed zero, while this reads it off a series discretized to
      // whole virtual seconds. Exact equality would assert that two different
      // measurements of the same quantity round identically.
      expect(Math.abs(derived! - CAUGHT_UP.get(spec.id)!)).toBeLessThanOrEqual(1)
    }
  })

  it('is null while a consumer is still draining', () => {
    const stillDraining = BUCKETS.filter((b) => b.consumer_id === 1).map((b) =>
      b.backlog === 0 && b.bucket_virtual_s >= OUTAGE_ENDS_AT_S ? { ...b, backlog: 7 } : b,
    )
    expect(deriveCaughtUpAfter(stillDraining, 1)).toBeNull()
  })
})

describe('toWideSeries', () => {
  it('emits one row per virtual second with every consumer as a column', () => {
    const rows = toWideSeries(BUCKETS, REFS)
    const distinct = new Set(BUCKETS.map((b) => b.bucket_virtual_s))
    expect(rows).toHaveLength(distinct.size)
    for (const ref of REFS) {
      expect(rows[0]).toHaveProperty(ref.name)
    }
    expect(rows.map((r) => r.t)).toEqual([...distinct].sort((a, b) => a - b))
  })
})
