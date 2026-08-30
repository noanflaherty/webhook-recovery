/**
 * Turning metrics buckets into chart rows.
 *
 * All pure, and all tested, because this is where a *plausible* wrong chart
 * comes from. The backend has the same concern from the other side -- see the
 * module docstring on `app/conductor/metrics.py`, which is about the two ways
 * to silently miscount into a chart that looks right. These are the two ways to
 * silently mis-*draw* correct numbers.
 */
import { FAIRNESS_WINDOW_S, OUTAGE_ENDS_AT_S } from '../scenario'
import type { MetricsBucket } from '../api/types'

/** A consumer, in the order the charts should stack and colour them. */
export interface ConsumerRef {
  id: number
  name: string
}

/** One x-position, one numeric column per consumer name. */
export type WidePoint = { t: number } & Record<string, number | null>

/** One fairness window: raw attempts total, plus each consumer's share of it. */
export type SharePoint = { t: number; total: number } & Record<string, number | null>

/** Consumers present in the buffer, ordered by id -- the seeding order. */
export function consumersFrom(buckets: MetricsBucket[]): ConsumerRef[] {
  const seen = new Map<number, string>()
  for (const bucket of buckets) {
    if (!seen.has(bucket.consumer_id)) seen.set(bucket.consumer_id, bucket.consumer_name)
  }
  return [...seen.entries()]
    .map(([id, name]) => ({ id, name }))
    .sort((a, b) => a.id - b.id)
}

/**
 * Merge a page of buckets into the buffer, keyed on consumer *and* bucket.
 *
 * Not an append, which is the obvious reading of `next_since_bucket`. Metrics
 * rows are **upserted** and their counters are *derived* from
 * `attempt.started_at` rather than sampled (`app/conductor/metrics.py`), so a
 * bucket the client has already read can legitimately be rewritten -- a
 * failover backfills, or a late commit lands inside a window already served.
 * Appending would keep the stale copy *and* the corrected one, double-counting
 * one consumer in a 100%-stacked chart. Which is exactly the failure this whole
 * project is built to argue against, drawn by its own instrument.
 *
 * Pair it with a small overlap on the cursor -- see `useRun` -- so the revision
 * is actually re-fetched rather than merely mergeable.
 */
export function mergeBuckets(existing: MetricsBucket[], incoming: MetricsBucket[]): MetricsBucket[] {
  if (incoming.length === 0) return existing
  const byKey = new Map<string, MetricsBucket>()
  for (const bucket of existing) byKey.set(keyOf(bucket), bucket)
  for (const bucket of incoming) byKey.set(keyOf(bucket), bucket)
  return [...byKey.values()].sort(
    (a, b) => a.bucket_virtual_s - b.bucket_virtual_s || a.consumer_id - b.consumer_id,
  )
}

function keyOf(bucket: MetricsBucket): string {
  return `${bucket.bucket_virtual_s}:${bucket.consumer_id}`
}

/**
 * Backlog depth per consumer, one row per virtual second.
 *
 * A consumer absent from a bucket stays `null` rather than becoming zero. The
 * conductor writes the full consumer x bucket cross product precisely so that a
 * hole means "no data", never "nothing happened", and flattening that here
 * would throw away the distinction it went to the trouble of preserving.
 */
export function toWideSeries(buckets: MetricsBucket[], consumers: ConsumerRef[]): WidePoint[] {
  const rows = new Map<number, WidePoint>()
  for (const bucket of buckets) {
    let row = rows.get(bucket.bucket_virtual_s)
    if (!row) {
      row = { t: bucket.bucket_virtual_s }
      for (const consumer of consumers) row[consumer.name] = null
      rows.set(bucket.bucket_virtual_s, row)
    }
    row[bucket.consumer_name] = bucket.backlog
  }
  return [...rows.values()].sort((a, b) => a.t - b.t)
}

/**
 * Attempts summed into fairness windows, as shares of each window.
 *
 * Two deliberate choices.
 *
 * **The window is `fairness_window_virtual_s`, not one second.** That is the
 * window the conductor's own rate term averages over, so the chart's
 * granularity is the mechanism's rather than one picked to look smooth. It also
 * happens to be legible: a 900-second run at one bar per second is sub-pixel.
 *
 * **Shares are computed here, not by the chart.** Recharts' `stackOffset`
 * ="expand" would divide by a window total that is legitimately zero for the
 * whole five virtual minutes of the outage -- nobody attempts anything while
 * the provider is down -- and render `NaN`. Emitting `null` for those windows
 * instead breaks the area cleanly, so the outage reads as a gap in the record
 * rather than as a suspiciously flat band.
 */
export function toShareWindows(
  buckets: MetricsBucket[],
  consumers: ConsumerRef[],
  windowS: number = FAIRNESS_WINDOW_S,
): SharePoint[] {
  const windows = new Map<number, Map<number, number>>()
  for (const bucket of buckets) {
    const start = Math.floor(bucket.bucket_virtual_s / windowS) * windowS
    let counts = windows.get(start)
    if (!counts) {
      counts = new Map<number, number>()
      windows.set(start, counts)
    }
    counts.set(bucket.consumer_id, (counts.get(bucket.consumer_id) ?? 0) + bucket.attempts)
  }

  return [...windows.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([start, counts]) => {
      const total = [...counts.values()].reduce((sum, n) => sum + n, 0)
      const point: SharePoint = { t: start, total }
      for (const consumer of consumers) {
        point[consumer.name] = total === 0 ? null : (counts.get(consumer.id) ?? 0) / total
      }
      return point
    })
}

/**
 * Virtual seconds from the end of the outage until this consumer's backlog hit
 * zero, or null while it is still draining.
 *
 * Derived client-side because nothing computes it server-side yet:
 * `list_consumers` returns `caught_up_after_s=None` unconditionally. This is
 * the headline number of the fairness claim -- the gap between the consumer
 * with the biggest backlog and the one with the smallest -- so it is worth
 * having before the backend grows it.
 */
export function deriveCaughtUpAfter(buckets: MetricsBucket[], consumerId: number): number | null {
  let earliest: number | null = null
  for (const bucket of buckets) {
    if (bucket.consumer_id !== consumerId) continue
    if (bucket.bucket_virtual_s < OUTAGE_ENDS_AT_S) continue
    if (bucket.backlog !== 0) continue
    if (earliest === null || bucket.bucket_virtual_s < earliest) earliest = bucket.bucket_virtual_s
  }
  return earliest === null ? null : earliest - OUTAGE_ENDS_AT_S
}

/** Peak backlog per consumer, for the y-axis story and the cards. */
export function peakBacklog(buckets: MetricsBucket[], consumerId: number): number {
  let peak = 0
  for (const bucket of buckets) {
    if (bucket.consumer_id === consumerId) peak = Math.max(peak, bucket.backlog)
  }
  return peak
}
