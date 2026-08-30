/**
 * The processes behind the run, and the one control that acts on them.
 *
 * This panel was cut in Phase 3 on the grounds that it proved something the
 * page never disputes. That was true while it only reported. It is back because
 * it now carries `kill`, and because the two failure paths that control opens up
 * are the only things on this page that no chart can draw:
 *
 * - kill a **worker** and it dies between claiming a batch and completing it, so
 *   its in-flight count holds a batch of leases nothing is going to answer for.
 *   The count falls when the lease expires and the conductor requeues the lot.
 * - kill the **leader conductor** and the advisory lock moves to the standby,
 *   because Postgres notices the connection go. A graceful stop cannot show
 *   this: it releases the lock on the way out.
 *
 * **Liveness is judged twice, and more tightly here than on the backend.** A
 * process killed ungracefully never deregisters, so its row survives the API's
 * 15-second window still reporting whatever it last wrote -- `is_leader`
 * included. Reading that literally puts two leaders on screen for fifteen
 * seconds during the one demo that is *about* leader failover, and the caption
 * then asserts something false about the single property the fairness argument
 * rests on. So a row that has missed several beats is drawn as silent and left
 * out of the leader count. It is a presentational judgement over a number
 * already on screen -- nothing decides anything by it, and leadership remains
 * the advisory lock and nothing else.
 */
import { useState } from 'react'

import { killProcess } from '../api/client'
import type { ProcessRead } from '../api/types'

interface Props {
  processes: ProcessRead[]
}

/**
 * How long a process may go quiet before its row is drawn as silent.
 *
 * `heartbeat_interval_s` is 3 and the API stops returning a row at 15, so this
 * sits between the two: late enough that three missed beats are needed and a
 * slow poll never trips it, early enough to be well clear of the window it is
 * qualifying.
 */
const SILENT_AFTER_S = 9

const silent = (process: ProcessRead) => process.heartbeat_age_s > SILENT_AFTER_S

export function ProcessStrip({ processes }: Props) {
  const conductors = processes.filter((p) => p.kind === 'conductor')
  const workers = processes.filter((p) => p.kind === 'worker')
  const leaders = conductors.filter((p) => p.is_leader && !silent(p)).length

  // Ids we have asked to die. Local, because the answer does not come back
  // through the API: the target is still alive and still heartbeating until it
  // reaches the point in its loop where it acts on the flag, so the only honest
  // thing to show in the meantime is that we asked.
  const [killing, setKilling] = useState<ReadonlySet<string>>(new Set())

  const onKill = (id: string) => {
    setKilling((s) => new Set(s).add(id))
    // Nothing to do on failure but let the row settle back. The strip re-polls
    // every few seconds, and a process that did not die stays in the list.
    void killProcess(id).catch(() => {
      setKilling((s) => {
        const next = new Set(s)
        next.delete(id)
        return next
      })
    })
  }

  return (
    <section className="panel">
      <h2>Processes</h2>
      <p className="caption">
        {processes.length === 0
          ? 'Nothing heartbeating inside the liveness window.'
          : `${conductors.length} conductor${conductors.length === 1 ? '' : 's'}, ${workers.length} worker${
              workers.length === 1 ? '' : 's'
            } — ${leaders} holding the admission lock. Kill one to watch what happens to the work it was holding.`}
      </p>
      <ul className="procs">
        {processes.map((process) => {
          const quiet = silent(process)
          return (
            <li
              key={process.id}
              className={[
                'proc',
                process.is_leader && !quiet ? 'leader' : '',
                quiet ? 'silent' : '',
              ]
                .filter(Boolean)
                .join(' ')}
            >
              <span className="proc-kind">{process.kind}</span>
              <span className="proc-host">{process.hostname}</span>
              {process.is_leader && !quiet && <span className="chip ok">leader</span>}
              {quiet && <span className="chip warn">no heartbeat</span>}
              {process.kind === 'worker' && (
                <span className="proc-flight">{process.in_flight} in flight</span>
              )}
              <span className="proc-age">{process.heartbeat_age_s.toFixed(1)}s</span>
              <button
                type="button"
                className="link danger"
                onClick={() => onKill(process.id)}
                disabled={killing.has(process.id)}
                title="Exit without draining, stranding whatever this process is holding"
              >
                {killing.has(process.id) ? 'killing…' : 'kill'}
              </button>
            </li>
          )
        })}
      </ul>
    </section>
  )
}
