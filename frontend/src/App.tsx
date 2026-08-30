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
import { useCallback, useMemo, useState } from 'react'

import './App.css'
import { LiveSource, createRun, retireRun } from './api/client'
import { ReplaySource } from './api/replay'
import type { DataSource } from './api/source'
import { BacklogChart } from './components/BacklogChart'
import { ConsumerCards } from './components/ConsumerCards'
import { ControlBar } from './components/ControlBar'
import { DecisionFeed } from './components/DecisionFeed'
import { EmptyState } from './components/EmptyState'
import { ProcessStrip } from './components/ProcessStrip'
import { ShareChart } from './components/ShareChart'
import { useRun } from './hooks/useRun'

const STORAGE_KEY = 'webhook-recovery:sim'

type Target = { kind: 'live'; simId: string } | { kind: 'replay' } | null

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
  if (target?.kind === 'live') url.searchParams.set('sim', target.simId)
  if (target?.kind === 'replay') url.searchParams.set('source', 'replay')
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

  const source = useMemo<DataSource | null>(() => {
    if (!target) return null
    return target.kind === 'replay' ? new ReplaySource() : new LiveSource(target.simId)
    // `generation` is a deliberate cache-buster rather than an input.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, generation])

  const run = useRun(source)

  const go = useCallback((next: Target) => {
    writeUrl(next)
    remember(next?.kind === 'live' ? next.simId : null)
    setTarget(next)
    setActionError(null)
  }, [])

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
      // Reset starts on the *naive* arm rather than inheriting the toggle or
      // the server's default. A fresh run is the "before" picture: you want to
      // watch FIFO starve the small consumer and then turn fairness on, which
      // means the interesting flip is on -> visible, not off -> nothing. It
      // also makes Reset mean one thing rather than "whatever it was last".
      const created = await createRun({ fair_drain_enabled: false })
      go({ kind: 'live', simId: created.id })
    } catch (err) {
      setActionError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }, [go, source, target])

  const replay = useCallback(() => go({ kind: 'replay' }), [go])

  // No run selected, or one that could not be loaded -- a remembered id whose
  // simulation is gone lands here too, with the reason shown.
  if (!source || (run.loading && run.error)) {
    return (
      <main className="app">
        <EmptyState
          onStart={() => void start()}
          onReplay={replay}
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

      <BacklogChart buckets={run.buckets} consumers={run.consumerRefs} />
      <ShareChart
        buckets={run.buckets}
        consumers={run.consumerRefs}
        fairDrainFlips={run.fairDrainFlips}
      />
      <ConsumerCards consumers={run.consumers} refs={run.consumerRefs} buckets={run.buckets} />
      <DecisionFeed decisions={run.decisions} sourceKind={source.kind} />
      <ProcessStrip processes={run.processes} sourceKind={source.kind} />
    </main>
  )
}
