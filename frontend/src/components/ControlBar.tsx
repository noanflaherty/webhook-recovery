/**
 * The clock, the phase, and the four knobs that drive a run.
 *
 * Pause / resume / speed are all one `PATCH` on the simulation row, because on
 * the backend they are all the same thing: a rewrite of the virtual epoch. The
 * UI deliberately does not model them as separate operations either.
 */
import type { SimulationRead, SimStatus } from '../api/types'
import type { SourceKind } from '../api/source'
import { PHASE_LABELS, formatVirtual } from '../scenario'

const SPEEDS = [1, 5, 10, 20, 50]

interface Props {
  simulation: SimulationRead
  virtualNowS: number
  sourceKind: SourceKind
  busy: boolean
  onPatch: (body: { status?: SimStatus; speed_multiplier?: number; fair_drain_enabled?: boolean }) => void
  onReset: () => void
}

export function ControlBar({
  simulation,
  virtualNowS,
  sourceKind,
  busy,
  onPatch,
  onReset,
}: Props) {
  const running = simulation.status === 'running'
  const done = simulation.status === 'done'
  const replay = sourceKind === 'replay'

  return (
    <header className="controls">
      <div className="clock">
        <span className="clock-time">{formatVirtual(virtualNowS)}</span>
        <span className={`chip phase-${simulation.phase}`}>{PHASE_LABELS[simulation.phase]}</span>
        <span className="clock-rate">{simulation.speed_multiplier}× virtual</span>
      </div>

      <div className="knobs">
        <button
          type="button"
          onClick={() => onPatch({ status: running ? 'paused' : 'running' })}
          disabled={busy || done}
        >
          {running ? 'Pause' : 'Resume'}
        </button>

        <label className="knob">
          <span>speed</span>
          <select
            value={simulation.speed_multiplier}
            onChange={(e) => onPatch({ speed_multiplier: Number(e.target.value) })}
            disabled={busy || done}
          >
            {SPEEDS.map((speed) => (
              <option key={speed} value={speed}>
                {speed}×
              </option>
            ))}
          </select>
        </label>

        {/*
          The comparison the whole submission rests on, and it is live: the
          conductor re-reads `fair_drain_enabled` every pass, so flipping this
          mid-run changes the slope of the attempt-share chart within a tick
          rather than needing a restart. `ShareChart` marks where it happened.

          Off is not a strawman -- it is global FIFO under one shared pool with
          per-consumer rate caps still honoured, which is what most systems ship.
        */}
        <label
          className="knob"
          title="On: weighted round-robin across consumers. Off: global FIFO, the naive arm."
        >
          <input
            type="checkbox"
            checked={simulation.fair_drain_enabled}
            onChange={(e) => onPatch({ fair_drain_enabled: e.target.checked })}
            disabled={busy || replay}
          />
          <span>fair drain</span>
          {replay && <span className="chip muted">recorded</span>}
        </label>

        <button type="button" onClick={onReset} disabled={busy}>
          {replay ? 'Restart replay' : 'Reset'}
        </button>
      </div>
    </header>
  )
}
