/**
 * The scenario's shape, mirrored from `app/core/scenario.py` and
 * `app/core/clock.py`.
 *
 * These are duplicated rather than fetched because they are *axis constants*:
 * the outage band has to be drawn on the very first frame, before any request
 * has come back, and a chart that draws its reference band one poll late is
 * visibly wrong on load. They are frozen alongside the API contract, so the
 * duplication has the same lifetime as the contract itself.
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


/**
 * What each consumer is *for*, mirrored from `app.core.scenario.CONSUMERS`.
 *
 * The cast is not three arbitrary customers -- each one isolates a single
 * variable, and the numbers on a card mean very little without knowing which.
 * Acme's backlog draining slowly is not a problem to fix; it is the control.
 * Clover's tiny backlog is not an accident; it is the entire fairness case.
 *
 * Keyed by name because the API contract is frozen and this is presentation
 * copy, not data: adding a `role` field to `ConsumerRead` would be a breaking
 * change to a contract two tracks were built against, to carry a sentence that
 * never varies per run.
 */
export interface ConsumerRole {
  /** One word, for the chip beside the name. */
  label: string
  /** One sentence: what this consumer is demonstrating. */
  blurb: string
}

export const CONSUMER_ROLES: Record<string, ConsumerRole> = {
  'Acme Analytics': {
    label: 'baseline',
    blurb:
      'Subscribes to all four event types and sets no policies, so every queued event has to be delivered. The control the other two are read against.',
  },
  'Bolt Billing': {
    label: 'policy',
    blurb:
      'The same subscriptions as Acme, but it coalesces subscription churn and drops balances older than 120s — so its backlog shrinks before it is ever sent, while every payment still lands.',
  },
  'Clover CRM': {
    label: 'fairness',
    blurb:
      'One low-volume event type, so its backlog is a fraction of the others’. With fair drain on it catches up in seconds; with it off it waits behind them.',
  },
}

/** The role for a consumer, or `null` for a cast this build does not know. */
export function roleFor(name: string): ConsumerRole | null {
  return CONSUMER_ROLES[name] ?? null
}
