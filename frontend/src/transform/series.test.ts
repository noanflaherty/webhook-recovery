/**
 * These run against the committed fixtures rather than against toy input.
 *
 * The fixtures are a real 640-virtual-second run generated from the backend's
 * own models, so a rule that is subtly wrong -- an off-by-one window boundary, a
 * gauge summed as a counter -- fails here against realistic data instead of
 * passing against three hand-written rows.
 */
import { describe, expect, it } from 'vitest'

import metricsFixture from '../fixtures/metrics.json'
import consumerFixture from '../fixtures/consumer.json'
import type { ConsumerRead, MetricsBucket, MetricsPage } from '../api/types'
import {
  consumersFrom,
  deriveCaughtUpAfter,
  mergeBuckets,
  toShareWindows,
  toWideSeries,
} from './series'

const BUCKETS = (metricsFixture as unknown as MetricsPage).buckets
const CONSUMERS = consumerFixture as unknown as ConsumerRead[]
const REFS = consumersFrom(BUCKETS)

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
  it('agrees with the run that generated the fixtures', () => {
    for (const consumer of CONSUMERS) {
      const derived = deriveCaughtUpAfter(BUCKETS, consumer.id)
      expect(derived).not.toBeNull()
      // Within one bucket. The fixture's own value is analytic; this one is
      // read off a series discretized to whole virtual seconds, so exact
      // equality would be asserting that two different measurements of the
      // same quantity round identically.
      expect(Math.abs(derived! - consumer.caught_up_after_s!)).toBeLessThanOrEqual(1)
    }
  })

  it('is null while a consumer is still draining', () => {
    const stillDraining = BUCKETS.filter((b) => b.consumer_id === 1).map((b) =>
      b.backlog === 0 && b.bucket_virtual_s >= 420 ? { ...b, backlog: 7 } : b,
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
