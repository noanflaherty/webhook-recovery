/**
 * The cold-landing screen.
 *
 * Nothing here starts a run on its own, and that is the point. The producer
 * emits into *every* simulation whose status is `running`, so a page that
 * created one on load would have each visitor permanently adding ~13 deliveries
 * per virtual second to the system the page exists to measure. The instrument
 * would become the reading.
 *
 * So a run is started by a click, and "replay the recorded run" is offered
 * beside it — which is also the only thing that works once the platform has
 * spun the stack down.
 */
import { PhaseTrack } from './PhaseTrack'

interface Props {
  onStart: () => void
  onReplay: () => void
  /** Null when this browser has no history yet, which is the first-visit case. */
  onViewRuns: (() => void) | null
  runCount: number
  busy: boolean
  error: string | null
}

export function EmptyState({ onStart, onReplay, onViewRuns, runCount, busy, error }: Props) {
  return (
    <section className="empty">
      <h1>webhook-recovery</h1>
      <p className="subtitle">
        Webhook delivery built for the hour after a provider comes back up: burn the backlog down
        fairly across consumers, and let each consumer say which events are still worth replaying.
      </p>
      {/*
        The shape of a run, rather than a sentence describing the shape of a
        run. It is the same component the transport carries, minus the
        playhead -- so the first thing the page shows you is the thing it will
        be showing you a clock against thirty seconds later.
      */}
      <PhaseTrack virtualNowS={null} />
      <p className="caption">
        Every run plays this same fifteen minutes, so the only thing that changes between two runs
        is the scheduler you point at it.
      </p>

      {error && <p className="error">{error}</p>}

      <div className="empty-actions">
        <button type="button" className="primary" onClick={onStart} disabled={busy}>
          Start a run
        </button>
        <button type="button" onClick={onReplay} disabled={busy}>
          Replay the recorded run
        </button>
        {/*
          Only once there is something to show. On a first visit this is the
          screen that has to explain the project in two buttons, and a third one
          leading to an empty list would cost that for nothing.
        */}
        {onViewRuns && (
          <button type="button" onClick={onViewRuns} disabled={busy}>
            Your runs ({runCount})
          </button>
        )}
      </div>

      <p className="caption">
        About 32 real seconds at 20×. The replay is a run recorded to fixtures and needs no backend,
        which is also what you get if the deployment has gone to sleep.
      </p>
    </section>
  )
}
