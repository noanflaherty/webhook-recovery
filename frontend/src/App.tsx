/**
 * The instrument panel.
 *
 * Layout and run identity; everything that moves lives in `useRun`, and
 * everything the panels read comes through one `DataSource`.
 *
 * **Run identity lives in the URL.** `?sim=<uuid>` names the run, mirrored to
 * localStorage so a refresh resumes it, and `?source=replay` selects the
 * recorded fixtures instead. Keying off an explicit id rather than "whatever
 * the latest run is" is what makes a run a durable artifact: a naive run and a
 * fair one each keep their own URL, so the two can be compared side by side
 * rather than only through the toggle.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'

import './App.css'
import { LiveSource, createRun, retireRun } from './api/client'
import { ReplaySource } from './api/replay'
import type { DataSource } from './api/source'
import { BacklogChart } from './components/BacklogChart'
import { ConsumerCards } from './components/ConsumerCards'
import { ControlBar } from './components/ControlBar'
import { EmptyState } from './components/EmptyState'
import { RunList } from './components/RunList'
import { ShareChart } from './components/ShareChart'
import { useRun } from './hooks/useRun'
import { listRuns, rememberRun } from './runs'

const STORAGE_KEY = 'webhook-recovery:sim'

/**
 * What the page is pointed at.
 *
 * `runs` is a view rather than a data source -- it reads localStorage and a
 * handful of one-shot fetches, and has no simulation of its own -- but it lives
 * in `Target` anyway so that it is addressable, back/forward works across it,
 * and there is still exactly one function that decides what the page shows.
 */
type Target =
  | { kind: 'live'; simId: string }
  | { kind: 'replay' }
  | { kind: 'runs' }
  | null

function remembered(): string | null {
  // Private-mode browsers throw on access rather than returning null.
  try {
    return localStorage.getItem(STORAGE_KEY)
  } catch {
    return null
  }
}

function remember(simId: string | null): void {
  try {
    if (simId === null) localStorage.removeItem(STORAGE_KEY)
    else localStorage.setItem(STORAGE_KEY, simId)
  } catch {
    /* nothing to do -- the URL is still the source of truth */
  }
}

function readTarget(): Target {
  const params = new URLSearchParams(window.location.search)
  if (params.get('view') === 'runs') return { kind: 'runs' }
  if (params.get('source') === 'replay') return { kind: 'replay' }
  const fromUrl = params.get('sim')
  if (fromUrl) return { kind: 'live', simId: fromUrl }
  const fromStorage = remembered()
  return fromStorage ? { kind: 'live', simId: fromStorage } : null
}

function writeUrl(target: Target): void {
  const url = new URL(window.location.href)
  url.searchParams.delete('sim')
  url.searchParams.delete('source')
  url.searchParams.delete('view')
  if (target?.kind === 'live') url.searchParams.set('sim', target.simId)
  if (target?.kind === 'replay') url.searchParams.set('source', 'replay')
  if (target?.kind === 'runs') url.searchParams.set('view', 'runs')
  window.history.replaceState(null, '', url)
}

export default function App() {
  const [target, setTarget] = useState<Target>(readTarget)
  // Bumped to build a fresh source, which is how "restart replay" clears the
  // metrics buffer: `useRun` keys all of its state off source identity, so
  // there is no separate reset path to keep in step with the polling one.
  const [generation, setGeneration] = useState(0)
  const [busy, setBusy] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  // Held in state rather than read at render: this component re-renders at the
  // clock's 10Hz, and parsing the history out of localStorage that often to
  // print one number would be silly. Every path that changes it says so.
  const [runCount, setRunCount] = useState(() => listRuns().length)

  const source = useMemo<DataSource | null>(() => {
    if (!target || target.kind === 'runs') return null
    return target.kind === 'replay' ? new ReplaySource() : new LiveSource(target.simId)
    // `generation` is a deliberate cache-buster rather than an input.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, generation])

  const run = useRun(source)

  const go = useCallback((next: Target) => {
    writeUrl(next)
    setTarget(next)
    setActionError(null)
  }, [])

  /**
   * Mirror the target into localStorage: the resume slot, and the history.
   *
   * Keyed off the target rather than done inside `go`, because the first target
   * of a session does not come from `go` -- it is read out of the URL by
   * `useState(readTarget)`. Doing it in `go` alone meant that opening a run by
   * its link, which is the single most likely way to arrive here, was the one
   * path that neither joined the history nor became the run a refresh resumes.
   *
   * The runs view is excluded rather than treated as "no run": it is a page you
   * pass through, and clearing the resume slot on the way in would mean the
   * list could not say which run you came from, and `back` would have nowhere
   * to go.
   */
  useEffect(() => {
    if (target?.kind === 'live') {
      remember(target.simId)
      setRunCount(rememberRun(target.simId).length)
    } else if (target?.kind !== 'runs') {
      remember(null)
    }
  }, [target])

  const start = useCallback(async () => {
    setBusy(true)
    setActionError(null)
    try {
      const created = await createRun()
      go({ kind: 'live', simId: created.id })
    } catch (err) {
      setActionError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }, [go])

  const reset = useCallback(async () => {
    if (source instanceof ReplaySource) {
      setGeneration((n) => n + 1)
      return
    }
    setBusy(true)
    setActionError(null)
    try {
      // Retire before creating. The producer feeds every `running` simulation,
      // so a run left running is not merely clutter -- it goes on consuming the
      // provider's global attempt budget and distorting the next run's chart.
      if (target?.kind === 'live') {
        try {
          await retireRun(target.simId)
        } catch {
          /* already gone, or never existed -- either way, on to the new one */
        }
      }
      // No arm passed, here or on any other start path: fair drain is the
      // server's default and the system's actual behaviour, so a fresh run
      // shows what the thing does. The comparison is made by flipping *to*
      // FIFO, which is also the honest direction -- the naive arm is the
      // hypothetical, not the baseline this ships in.
      const created = await createRun()
      go({ kind: 'live', simId: created.id })
    } catch (err) {
      setActionError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }, [go, source, target])

  const replay = useCallback(() => go({ kind: 'replay' }), [go])
  const viewRuns = useCallback(() => go({ kind: 'runs' }), [go])

  // Leaving the list goes back to the run you came from if there still is one,
  // and to the cold-landing screen otherwise -- never to a dead end.
  const leaveRuns = useCallback(() => {
    const last = remembered()
    go(last ? { kind: 'live', simId: last } : null)
  }, [go])

  if (target?.kind === 'runs') {
    return (
      <main className="app">
        <div className="titlebar">
          <h1>webhook-recovery</h1>
          <button type="button" className="link" onClick={leaveRuns}>
            back
          </button>
        </div>
        {actionError && <p className="error">{actionError}</p>}
        <RunList
          currentSimId={remembered()}
          busy={busy}
          onOpen={(simId) => go({ kind: 'live', simId })}
          onStart={() => void start()}
          onReplay={replay}
          onHistoryChange={() => setRunCount(listRuns().length)}
        />
      </main>
    )
  }

  // No run selected, or one that could not be loaded -- a remembered id whose
  // simulation is gone lands here too, with the reason shown.
  if (!source || (run.loading && run.error)) {
    return (
      <main className="app">
        <EmptyState
          onStart={() => void start()}
          onReplay={replay}
          onViewRuns={runCount > 0 ? viewRuns : null}
          runCount={runCount}
          busy={busy}
          error={actionError ?? (source ? run.error : null)}
        />
      </main>
    )
  }

  if (!run.simulation) {
    return (
      <main className="app">
        <p className="caption">Loading run…</p>
      </main>
    )
  }

  return (
    <main className="app">
      <div className="titlebar">
        <h1>webhook-recovery</h1>
        <span className={`chip source-${source.kind}`}>
          {source.kind === 'replay' ? 'recorded run' : 'live'}
        </span>
        {source.kind === 'replay' ? (
          <button type="button" className="link" onClick={() => void start()} disabled={busy}>
            start a live run
          </button>
        ) : (
          <button type="button" className="link" onClick={replay}>
            view the recorded run
          </button>
        )}
        {runCount > 0 && (
          <button type="button" className="link" onClick={viewRuns}>
            your runs ({runCount})
          </button>
        )}
      </div>

      <ControlBar
        simulation={run.simulation}
        virtualNowS={run.virtualNowS}
        sourceKind={source.kind}
        busy={busy}
        onPatch={(body) => void run.patch(body)}
        onReset={() => void reset()}
      />

      {(actionError ?? run.error) && <p className="error">{actionError ?? run.error}</p>}

      {/*
        The cast comes first. The two charts are unreadable until you know who
        the three traces are and what each one is there to demonstrate -- that
        Acme's slow drain is the control rather than a defect, and that Clover's
        small backlog is the entire fairness case. Introduce the channels, then
        show what happens to them.
      */}
      <ConsumerCards consumers={run.consumers} refs={run.consumerRefs} buckets={run.buckets} />
      <BacklogChart buckets={run.buckets} consumers={run.consumerRefs} />
      <ShareChart
        buckets={run.buckets}
        consumers={run.consumerRefs}
        fairDrainFlips={run.fairDrainFlips}
      />
    </main>
  )
}
