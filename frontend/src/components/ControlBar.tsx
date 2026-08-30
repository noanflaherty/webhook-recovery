/**
 * The clock, the phase, and the four knobs a reviewer touches.
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
          Wired to the real field, and honestly labelled as doing nothing yet.
          Phase 3 replaces `select_candidates` in the conductor; this toggle is
          already the switch that will select between the two arms, so the seam
          gets exercised now rather than being written on the day it has to work.
        */}
        <label className="knob" title="Phase 3 wires this to the conductor's admission policy.">
          <input
            type="checkbox"
            checked={simulation.fair_drain_enabled}
            onChange={(e) => onPatch({ fair_drain_enabled: e.target.checked })}
            disabled={busy || replay}
          />
          <span>fair drain</span>
          <span className="chip muted">{replay ? 'recorded' : 'inert until Phase 3'}</span>
        </label>

        <button type="button" onClick={onReset} disabled={busy}>
          {replay ? 'Restart replay' : 'Reset'}
        </button>
      </div>
    </header>
  )
}
