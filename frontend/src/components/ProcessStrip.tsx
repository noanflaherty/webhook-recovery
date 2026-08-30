/**
 * The live conductors and workers, and which conductor holds the lock.
 *
 * Carried over from the Phase 0 stub rather than rebuilt. It earns its space
 * for one reason: the fairness argument depends on admission being decided by a
 * *single* conductor, and this is the only place a reviewer can see that claim
 * is true of the running system rather than of the diagram.
 *
 * Liveness is a read-time filter on the backend, never a reaper -- a process
 * that stops heartbeating simply stops appearing here.
 */
import type { ProcessRead } from '../api/types'
import type { SourceKind } from '../api/source'

interface Props {
  processes: ProcessRead[]
  sourceKind: SourceKind
}

export function ProcessStrip({ processes, sourceKind }: Props) {
  const conductors = processes.filter((p) => p.kind === 'conductor')
  const workers = processes.filter((p) => p.kind === 'worker')
  const leaders = conductors.filter((p) => p.is_leader).length

  return (
    <section className="panel">
      <h2>
        Processes
        {sourceKind === 'replay' && <span className="chip muted">recorded</span>}
      </h2>
      <p className="caption">
        {processes.length === 0
          ? 'Nothing heartbeating inside the liveness window.'
          : `${conductors.length} conductor${conductors.length === 1 ? '' : 's'}, ${workers.length} worker${
              workers.length === 1 ? '' : 's'
            } — ${leaders} holding the admission lock.`}
      </p>
      <ul className="strip">
        {processes.map((process) => (
          <li key={process.id} className={process.is_leader ? 'proc leader' : 'proc'}>
            <span className="proc-kind">{process.kind}</span>
            <span className="proc-host">{process.hostname}</span>
            {process.is_leader && <span className="chip">leader</span>}
            {process.kind === 'worker' && (
              <span className="muted">{process.in_flight} in flight</span>
            )}
            <span className="muted">{process.heartbeat_age_s.toFixed(1)}s ago</span>
          </li>
        ))}
      </ul>
    </section>
  )
}
