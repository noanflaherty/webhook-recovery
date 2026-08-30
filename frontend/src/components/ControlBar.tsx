/**
 * The transport: the clock, where in the run that clock is, and the knobs that
 * move it.
 *
 * Pause / resume / speed are all one `PATCH` on the simulation row, because on
 * the backend they are all the same thing: a rewrite of the virtual epoch. The
 * UI deliberately does not model them as separate operations either.
 */
import type { SimulationRead, SimStatus } from '../api/types'
import type { SourceKind } from '../api/source'
import { formatVirtual } from '../scenario'
import { PhaseTrack } from './PhaseTrack'

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
  const fair = simulation.fair_drain_enabled

  return (
    <header className="transport">
      <div className="transport-top">
        <div className="clock">
          <span className="clock-time">{formatVirtual(virtualNowS)}</span>
          <span className="clock-rate">
            {done ? 'run complete' : `${simulation.speed_multiplier}× virtual`}
          </span>
        </div>

        <div className="knobs">
          {/*
            The comparison the whole submission rests on, so it leads the row and
            is named rather than checked: a checkbox called "fair drain" leaves the
            unchecked state unnamed, and the unchecked state is half the argument.
            Spelling both arms out says what the off position *is* -- global FIFO
            under one shared pool, with per-consumer rate caps still honoured,
            which is what most systems ship. Not a strawman, the plausible one.

            It is live. The conductor re-reads `fair_drain_enabled` every pass, so
            flipping this mid-run changes the slope of the attempt-share chart
            within a tick rather than needing a restart, and `ShareChart` marks
            where it happened.
          */}
          <div
            className="segmented"
            role="group"
            aria-label="Scheduler"
            title="FIFO: one shared pool, oldest first. Fair drain: weighted round-robin across consumers."
          >
            <button
              type="button"
              aria-pressed={!fair}
              onClick={() => onPatch({ fair_drain_enabled: false })}
              disabled={busy || replay || done}
            >
              FIFO
            </button>
            <button
              type="button"
              aria-pressed={fair}
              onClick={() => onPatch({ fair_drain_enabled: true })}
              disabled={busy || replay || done}
            >
              Fair drain
            </button>
          </div>
          {replay && <span className="chip muted">recorded</span>}

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

          <button
            type="button"
            onClick={() => onPatch({ status: running ? 'paused' : 'running' })}
            disabled={busy || done}
          >
            {running ? 'Pause' : 'Resume'}
          </button>

          <button type="button" onClick={onReset} disabled={busy}>
            {replay ? 'Restart replay' : 'Reset'}
          </button>
        </div>
      </div>

      <PhaseTrack virtualNowS={virtualNowS} />
    </header>
  )
}
