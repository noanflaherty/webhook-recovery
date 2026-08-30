/**
 * The frozen API contract, hand-mirrored from `app/api/schemas.py`.
 *
 * Source of truth is that file, with `openapi.json` as the generated witness of
 * it. These are written by hand rather than generated: a codegen step would put
 * a backend build in front of every frontend type change, and the contract is
 * small enough that the mirror is cheaper than the toolchain.
 *
 * Ingest (`EventCreate` / `EventRead`) is deliberately absent -- the UI never
 * posts events. The producer inside the api process does that.
 */

/** `app.core.enums.SimStatus`. */
export type SimStatus = 'running' | 'paused' | 'done'

/** `app.core.enums.DeliveryState`. */
export type DeliveryState =
  | 'pending'
  | 'ready'
  | 'in_flight'
  | 'delivered'
  | 'expired'
  | 'superseded'
  | 'failed'

/** `app.core.enums.ProcessKind`. The api deliberately does not register. */
export type ProcessKind = 'conductor' | 'worker'

/**
 * `app.core.scenario`'s phases. Not an enum on the backend -- `phase` is a bare
 * `str` on `SimulationRead` -- so `string` would be the faithful mirror. It is
 * narrowed here because every consumer of it is a lookup that must be total.
 */
export type Phase = 'normal' | 'outage' | 'recovery' | 'done'

export interface HealthRead {
  status: string
  db: string
}

export interface SimulationCreate {
  scenario_name?: string | null
  speed_multiplier?: number | null
  fair_drain_enabled?: boolean | null
  global_attempts_per_s?: number | null
}

/** Every field optional -- omitted means "leave alone". */
export interface SimulationPatch {
  status?: SimStatus
  speed_multiplier?: number
  fair_drain_enabled?: boolean
  global_attempts_per_s?: number
  /** Force the outage on/off. Null in the response means the script decides. */
  outage_override?: boolean
}

export interface SimulationRead {
  id: string
  scenario_name: string
  status: SimStatus
  speed_multiplier: number
  fair_drain_enabled: boolean
  global_attempts_per_s: number
  outage_override: boolean | null

  /** ISO-8601. Current virtual time, computed by whichever process served this. */
  virtual_now: string
  /** The same instant in seconds since `VIRTUAL_EPOCH_ZERO` -- the charts' x-axis. */
  virtual_now_s: number
  phase: Phase

  created_at_wall: string
}

export interface ConsumerRead {
  id: number
  name: string
  weight: number
  concurrency_cap: number
  max_attempts_per_s: number

  backlog: number
  in_flight: number
  delivered: number
  expired: number
  superseded: number
  failed: number
  /**
   * Virtual seconds from the end of the outage until this consumer's backlog
   * hit zero. Always null: nothing computes it server-side, so the UI derives
   * it from the metrics series instead. See `deriveCaughtUpAfter`.
   */
  caught_up_after_s: number | null
}

/** One consumer, one virtual second. */
export interface MetricsBucket {
  consumer_id: number
  consumer_name: string
  bucket_virtual_s: number

  backlog: number
  ready: number
  in_flight: number
  attempts: number
  delivered: number
  expired: number
  superseded: number
  failed: number
}

export interface MetricsPage {
  simulation_id: string
  buckets: MetricsBucket[]
  /** Pass back as `?since_bucket=`. Unchanged when the page is empty. */
  next_since_bucket: number
}

export interface DecisionRead {
  delivery_id: number
  consumer_id: number
  consumer_name: string
  event_type: string
  entity_key: string
  state: DeliveryState
  /** Free text, display-only. */
  terminal_reason: string | null
  attempt_count: number
  occurred_at: string
  completed_at: string
}

/** Newest-first, replace-on-poll -- deliberately not a cursor. */
export interface DecisionsPage {
  simulation_id: string
  decisions: DecisionRead[]
}

export interface ProcessRead {
  id: string
  kind: ProcessKind
  hostname: string
  pid: number
  started_at_wall: string
  last_heartbeat_wall: string
  is_leader: boolean
  /** Real seconds, so the pulse does not need the two clocks to agree. */
  heartbeat_age_s: number
  in_flight: number
}
