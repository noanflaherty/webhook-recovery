/**
 * The scenario's shape, mirrored from `app/core/scenario.py` and
 * `app/core/clock.py`.
 *
 * These are duplicated rather than fetched because they are *axis constants*:
 * the outage band has to be drawn on the very first frame, before any request
 * has come back, and a chart that draws its reference band one poll late is
 * visibly wrong on load. They are frozen alongside the API contract, so the
 * duplication has the same lifetime as the rest of Phase 0's freeze.
 */
import type { Phase } from './api/types'

/**
 * Virtual time zero -- `app.core.clock.VIRTUAL_EPOCH_ZERO`, as epoch ms.
 *
 * Every simulation's virtual clock starts here and `metrics_snapshot` buckets
 * are keyed off it, so a virtual timestamp anywhere in the system converts to a
 * bucket index by subtracting this. Never `simulation.virtual_epoch`, which is
 * rewritten on every pause, resume and speed change.
 */
export const VIRTUAL_EPOCH_ZERO_MS = Date.UTC(2024, 0, 1)

export const OUTAGE_STARTS_AT_S = 120
export const OUTAGE_ENDS_AT_S = 420

/** `fairness_window_virtual_s` -- the window the scheduler itself averages over. */
export const FAIRNESS_WINDOW_S = 5

/** `app.core.scenario.phase_at`. */
export function phaseAt(
  virtualS: number,
  options: { outageOverride?: boolean | null; done?: boolean } = {},
): Phase {
  const { outageOverride = null, done = false } = options
  if (done) return 'done'
  if (outageOverride === true) return 'outage'
  if (outageOverride === false) {
    return virtualS < OUTAGE_STARTS_AT_S ? 'normal' : 'recovery'
  }
  if (virtualS < OUTAGE_STARTS_AT_S) return 'normal'
  if (virtualS < OUTAGE_ENDS_AT_S) return 'outage'
  return 'recovery'
}

export const PHASE_LABELS: Record<Phase, string> = {
  normal: 'normal',
  outage: 'provider down',
  recovery: 'recovering',
  done: 'finished',
}

/** `mm:ss` from virtual seconds -- how the scenario's own comments read. */
export function formatVirtual(virtualS: number): string {
  const total = Math.max(0, Math.floor(virtualS))
  const mm = Math.floor(total / 60)
  const ss = total % 60
  return `${mm}:${String(ss).padStart(2, '0')}`
}

/** Virtual seconds since epoch zero for an ISO timestamp from the API. */
export function toVirtualSeconds(iso: string): number {
  return (Date.parse(iso) - VIRTUAL_EPOCH_ZERO_MS) / 1000
}
