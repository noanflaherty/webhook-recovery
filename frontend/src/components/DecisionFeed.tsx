/**
 * The most recent terminal decisions, newest first.
 *
 * Where the *second* claim is legible: `expired` and `superseded` are deliveries
 * the system decided were not worth sending, each with the reason attached. The
 * charts show that the backlog shrank; only this shows what it was traded for.
 *
 * Replace-on-poll, not append -- `delivery.id` is assigned at ingest rather than
 * at completion, so a cursor over it would silently skip decisions. See
 * `DecisionsPage` in `app/api/schemas.py`.
 */
import type { DecisionRead } from '../api/types'
import type { SourceKind } from '../api/source'
import { formatVirtual, toVirtualSeconds } from '../scenario'

interface Props {
  decisions: DecisionRead[]
  sourceKind: SourceKind
}

export function DecisionFeed({ decisions, sourceKind }: Props) {
  return (
    <section className="panel">
      <h2>Decisions</h2>
      {decisions.length === 0 ? (
        <p className="caption">
          {sourceKind === 'replay'
            ? 'The recorded run captured only its final 50 decisions, so this fills in near the end of the replay.'
            : 'Nothing has reached a terminal state yet.'}
        </p>
      ) : (
        <ul className="feed">
          {decisions.map((decision) => (
            <li key={decision.delivery_id}>
              <span className={`chip state-${decision.state}`}>{decision.state}</span>
              <span className="feed-consumer">{decision.consumer_name}</span>
              <span className="feed-event">
                {decision.event_type} <span className="muted">{decision.entity_key}</span>
              </span>
              <span className="muted feed-reason">
                {decision.terminal_reason ??
                  `${decision.attempt_count} attempt${decision.attempt_count === 1 ? '' : 's'}`}
              </span>
              <span className="muted">{formatVirtual(toVirtualSeconds(decision.completed_at))}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
