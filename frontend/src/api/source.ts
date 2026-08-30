/**
 * The seam the whole UI is built against.
 *
 * Two implementations: `LiveSource` talks to the api, `ReplaySource` serves the
 * committed fixtures against a locally-driven virtual clock. No component knows
 * which one is behind it.
 *
 * That is what keeps the deployed URL useful when the backend is unavailable --
 * the platform has spun the stack down, say: the reference run still loads,
 * clearly labelled as recorded rather than live.
 */
import type {
  ConsumerRead,
  DecisionsPage,
  MetricsPage,
  ProcessRead,
  SimulationPatch,
  SimulationRead,
} from './types'

export type SourceKind = 'live' | 'replay'

export interface DataSource {
  readonly kind: SourceKind
  /** The run this source is bound to. Fixed for its lifetime. */
  readonly simulationId: string

  getSimulation(): Promise<SimulationRead>
  getConsumers(): Promise<ConsumerRead[]>
  /** Buckets strictly greater than `sinceBucket`. Pass `-1` for everything. */
  getMetrics(sinceBucket: number): Promise<MetricsPage>
  getDecisions(limit?: number): Promise<DecisionsPage>
  getProcesses(): Promise<ProcessRead[]>
  patch(body: SimulationPatch): Promise<SimulationRead>
}
