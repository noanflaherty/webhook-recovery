/**
 * The seam the whole UI is built against.
 *
 * Two implementations: `LiveSource` talks to the api, `ReplaySource` serves the
 * committed fixtures against a locally-driven virtual clock. No component knows
 * which one is behind it, which is what let this phase be built and demoed
 * while the backend it measures was still being written in another worktree.
 *
 * It keeps earning that after the split closes: a reviewer who lands on the
 * deployed URL after the platform has spun the stack down still gets the
 * reference run, clearly labelled as recorded rather than live.
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
