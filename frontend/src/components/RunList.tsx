/**
 * Every run this browser has seen, with what each one currently is.
 *
 * The point of the page is the *comparison*: the submission's first claim is a
 * before/after, and before this existed the two halves of it were two URLs you
 * had to keep by hand. So the column that matters most is `scheduler` -- it is
 * what tells you which row is the naive arm and which is the fair one, months
 * after you stopped remembering.
 *
 * State is fetched once on mount rather than polled. A list is a place you pass
 * through on the way to a run, and N polling runs would multiply the load on
 * the system the app exists to measure -- the same reason the cold-landing
 * screen does not start a run for you.
 */
import { useCallback, useEffect, useState } from 'react'

import { ApiError, fetchRun, retireRun } from '../api/client'
import type { SimulationRead } from '../api/types'
import { PHASE_LABELS, formatVirtual } from '../scenario'
import { forgetRun, listRuns, type RunRecord } from '../runs'

/**
 * A remembered run, resolved against the server.
 *
 * `gone` is its own state rather than an error, because it is the expected one:
 * local history outlives any particular database, so a run the server has
 * dropped is a normal row to render, not a failure to report.
 */
type Row =
  | { state: 'loading'; record: RunRecord }
  | { state: 'ok'; record: RunRecord; simulation: SimulationRead }
  | { state: 'gone'; record: RunRecord }
  | { state: 'error'; record: RunRecord; message: string }

interface Props {
  currentSimId: string | null
  busy: boolean
  onOpen: (simId: string) => void
  onStart: () => void
  onReplay: () => void
  /** Called after a row is forgotten, so the header's count follows. */
  onHistoryChange: () => void
}

function ago(ms: number): string {
  const seconds = Math.max(0, Math.round((Date.now() - ms) / 1000))
  if (seconds < 60) return 'just now'
  const minutes = Math.round(seconds / 60)
  if (minutes < 60) return `${minutes} min ago`
  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.round(hours / 24)}d ago`
}

export function RunList({
  currentSimId,
  busy,
  onOpen,
  onStart,
  onReplay,
  onHistoryChange,
}: Props) {
  const [rows, setRows] = useState<Row[]>(() =>
    listRuns().map((record) => ({ state: 'loading', record })),
  )

  useEffect(() => {
    let cancelled = false
    const records = listRuns()

    // All at once: this is a handful of small reads against one process, and
    // resolving them in sequence would show a list filling in top to bottom for
    // no benefit.
    void Promise.all(
      records.map(async (record): Promise<Row> => {
        try {
          return { state: 'ok', record, simulation: await fetchRun(record.id) }
        } catch (err) {
          if (err instanceof ApiError && err.status === 404) return { state: 'gone', record }
          return {
            state: 'error',
            record,
            message: err instanceof Error ? err.message : String(err),
          }
        }
      }),
    ).then((resolved) => {
      if (!cancelled) setRows(resolved)
    })

    return () => {
      cancelled = true
    }
  }, [])

  const forget = useCallback(
    (id: string) => {
      forgetRun(id)
      setRows((current) => current.filter((row) => row.record.id !== id))
      onHistoryChange()
    },
    [onHistoryChange],
  )

  const retire = useCallback(async (id: string) => {
    const simulation = await retireRun(id)
    setRows((current) =>
      current.map((row) =>
        row.record.id === id ? { state: 'ok', record: row.record, simulation } : row,
      ),
    )
  }, [])

  const live = rows.filter(
    (row) => row.state === 'ok' && row.simulation.status !== 'done',
  ).length

  return (
    <section className="runs">
      <div className="runs-head">
        <h2>Your runs</h2>
        <div className="runs-actions">
          <button type="button" className="primary" onClick={onStart} disabled={busy}>
            Start a new run
          </button>
          <button type="button" onClick={onReplay} disabled={busy}>
            Replay the recorded run
          </button>
        </div>
      </div>

      <p className="caption">
        Kept in this browser, not on the server — runs themselves are permanent and addressable by
        URL, but nothing server-side knows they are <em>yours</em>. Clearing site data clears this
        list; the runs it named survive.
      </p>

      {rows.length === 0 ? (
        <p className="caption">No runs yet. Start one and it will appear here.</p>
      ) : (
        <table className="runs-table">
          <thead>
            <tr>
              <th>Started</th>
              <th>Scheduler</th>
              <th>Phase</th>
              <th>Virtual</th>
              <th>Status</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const id = row.record.id
              const current = id === currentSimId
              return (
                <tr key={id} className={current ? 'current' : undefined}>
                  {row.state === 'ok' ? (
                    <>
                      <td>
                        <button type="button" className="link" onClick={() => onOpen(id)}>
                          {ago(Date.parse(row.simulation.created_at_wall))}
                        </button>
                        {current && <span className="chip muted">open</span>}
                      </td>
                      <td>
                        {/*
                          The column the page exists for. Naming the arm on
                          every row is what turns a list of runs into a
                          before/after you can still read later.
                        */}
                        <span className={row.simulation.fair_drain_enabled ? 'ok' : 'muted'}>
                          {row.simulation.fair_drain_enabled ? 'Fair drain' : 'FIFO'}
                        </span>
                      </td>
                      <td>
                        <span className={`chip phase-${row.simulation.phase}`}>
                          {PHASE_LABELS[row.simulation.phase]}
                        </span>
                      </td>
                      <td>{formatVirtual(row.simulation.virtual_now_s)}</td>
                      <td className="muted">{row.simulation.status}</td>
                      <td className="runs-row-actions">
                        {/*
                          Retiring from here is not tidying. The producer feeds
                          *every* running simulation, so each run left running
                          goes on spending the shared attempt budget -- and the
                          cost lands on whichever run you are currently
                          watching. A list of runs is the first place that is
                          visible, so it is the right place to act on it.
                        */}
                        {row.simulation.status !== 'done' && (
                          <button type="button" className="link" onClick={() => void retire(id)}>
                            retire
                          </button>
                        )}
                      </td>
                    </>
                  ) : (
                    <>
                      <td>
                        <span className="muted">{ago(row.record.seenAt)}</span>
                      </td>
                      <td colSpan={3} className="muted">
                        {row.state === 'loading' && 'loading…'}
                        {row.state === 'gone' && 'no longer on the server'}
                        {row.state === 'error' && row.message}
                      </td>
                      <td className="muted">{row.state === 'loading' ? '' : '—'}</td>
                      <td className="runs-row-actions">
                        {row.state !== 'loading' && (
                          <button type="button" className="link" onClick={() => forget(id)}>
                            forget
                          </button>
                        )}
                      </td>
                    </>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      )}

      {live > 1 && (
        <p className="caption warn">
          {live} runs are still going. They share one provider budget, so each one slows the others
          down — retire the ones you are finished with.
        </p>
      )}
    </section>
  )
}
