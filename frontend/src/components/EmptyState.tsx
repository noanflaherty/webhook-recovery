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
interface Props {
  onStart: () => void
  onReplay: () => void
  busy: boolean
  error: string | null
}

export function EmptyState({ onStart, onReplay, busy, error }: Props) {
  return (
    <section className="empty">
      <h1>webhook-recovery</h1>
      <p className="subtitle">
        Webhook delivery built for the hour after a provider comes back up: burn the backlog down
        fairly across consumers, and let each consumer say which events are still worth replaying.
      </p>

      {error && <p className="error">{error}</p>}

      <div className="empty-actions">
        <button type="button" className="primary" onClick={onStart} disabled={busy}>
          Start a run
        </button>
        <button type="button" onClick={onReplay} disabled={busy}>
          Replay the recorded run
        </button>
      </div>

      <p className="caption">
        A run takes about 32 real seconds at 20×: two minutes of normal traffic, a five-minute
        provider outage, then the recovery. The replay is a recording of one, and needs no backend.
      </p>
    </section>
  )
}
