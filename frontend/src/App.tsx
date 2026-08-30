/**
 * The instrument panel.
 *
 * Layout and run identity; everything that moves lives in `useRun`.
 *
 * **Run identity lives in the URL.** `?sim=<uuid>` names the run, mirrored to
 * localStorage so a refresh resumes it. Keying off an explicit id rather than
 * "whatever the latest run is" is what makes a run a durable artifact: a naive
 * run and a fair one each keep their own URL, so the two can be compared side
 * by side rather than only through the toggle.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'

import './App.css'
import { LiveSource, createRun, retireRun } from './api/client'
import { BacklogChart } from './components/BacklogChart'
import { ConsumerCards } from './components/ConsumerCards'
import { ControlBar } from './components/ControlBar'
import { EmptyState } from './components/EmptyState'
import { ProcessStrip } from './components/ProcessStrip'
import { RunList } from './components/RunList'
import { ShareChart } from './components/ShareChart'
import { useRun } from './hooks/useRun'
import { listRuns, rememberRun } from './runs'

const STORAGE_KEY = 'webhook-recovery:sim'

/**
 * What the page is pointed at.
 *
 * `runs` is a view rather than a run -- it reads localStorage and a handful of
 * one-shot fetches, and has no simulation of its own -- but it lives in `Target`
 * anyway so that it is addressable, back/forward works across it, and there is
 * still exactly one function that decides what the page shows.
 */
type Target = { kind: 'live'; simId: string } | { kind: 'runs' } | null

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
  const fromUrl = params.get('sim')
  if (fromUrl) return { kind: 'live', simId: fromUrl }
  const fromStorage = remembered()
  return fromStorage ? { kind: 'live', simId: fromStorage } : null
}

function writeUrl(target: Target): void {
  const url = new URL(window.location.href)
  url.searchParams.delete('sim')
  url.searchParams.delete('view')
  if (target?.kind === 'live') url.searchParams.set('sim', target.simId)
  if (target?.kind === 'runs') url.searchParams.set('view', 'runs')
  window.history.replaceState(null, '', url)
}

export default function App() {
  const [target, setTarget] = useState<Target>(readTarget)
  const [busy, setBusy] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  // Held in state rather than read at render: this component re-renders at the
  // clock's 10Hz, and parsing the history out of localStorage that often to
  // print one number would be silly. Every path that changes it says so.
  const [runCount, setRunCount] = useState(() => listRuns().length)

  // Rebuilt when the run changes, and `useRun` keys all of its state off source
  // identity -- so switching runs clears the metrics buffer with no separate
  // reset path to keep in step with the polling one.
  const source = useMemo<LiveSource | null>(
    () => (target?.kind === 'live' ? new LiveSource(target.simId) : null),
    [target],
  )

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
      // localStorage is the external system this synchronizes with, which is
      // what effects are for, and the count is what the write hands back --
      // deriving it at render would re-parse the history on every clock tick.
      // eslint-disable-next-line react/set-state-in-effect
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
  }, [go, target])

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
      {/*
        No "live" badge any more. It earned its place when a recorded run was
        the other possibility and the reader had to be told which one they were
        looking at; with one kind of run left it was a label that is true of
        every page load, which is a label that says nothing. Whether *this* run
        is still moving is the transport's job, and it already says so.
      */}
      <div className="titlebar">
        <h1>webhook-recovery</h1>
        {runCount > 0 && (
          <button type="button" className="link" onClick={viewRuns}>
            your runs ({runCount})
          </button>
        )}
      </div>

      <ControlBar
        simulation={run.simulation}
        virtualNowS={run.virtualNowS}
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

      {/*
        Last, and deliberately below the two claims. It is not part of either
        argument -- it is where you go to interfere with the run once you have
        read them, and where the consequence of interfering shows up.
      */}
      <ProcessStrip processes={run.processes} />
    </main>
  )
}
