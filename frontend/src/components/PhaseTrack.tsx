/**
 * The scripted shape of a run, drawn to scale, with an optional playhead.
 *
 * The signature of the page, and the one place it spends any boldness.
 *
 * A run is a script: two minutes of normal traffic, a five-minute provider
 * outage, then recovery until the backlog is drained. The UI used to state the
 * current phase in a word and leave the shape to be inferred from the charts.
 * The word was the least useful part — "provider down" is already written
 * across both plots — and what a label cannot give you is *proportion*: that the
 * outage is a third of the run, that recovery is most of it, and how far
 * through the clock currently is.
 *
 * Drawn on the charts' own x-axis, so the playhead and the hatched span below
 * it line up. Hatched with the same treatment the charts give the same window,
 * so the two read as one instrument rather than two components that happen to
 * agree.
 *
 * Used twice: in the transport with a playhead, and on the cold-landing screen
 * without one, where it says what a run *is* better than the sentence it
 * replaced.
 */
import { OUTAGE_ENDS_AT_S, OUTAGE_STARTS_AT_S, formatVirtual } from '../scenario'

/**
 * How much run the track draws before it starts stretching.
 *
 * Recovery has no scripted end — it runs until the backlog is drained — so the
 * track needs a nominal length to lay the two scripted phases out against. A
 * run that outlasts it rescales rather than overflowing, which is the honest
 * behaviour: the phases stay in proportion to each other, and the whole track
 * goes on meaning "the run so far".
 */
const NOMINAL_RUN_S = 900

interface Segment {
  key: string
  label: string
  from: number
  to: number
  fault?: boolean
}

interface Props {
  /** Where the clock is, or null to draw the script with no playhead. */
  virtualNowS: number | null
}

export function PhaseTrack({ virtualNowS }: Props) {
  const end = Math.max(NOMINAL_RUN_S, virtualNowS ?? 0)
  const segments: Segment[] = [
    { key: 'normal', label: 'Normal', from: 0, to: OUTAGE_STARTS_AT_S },
    {
      key: 'outage',
      label: 'Provider down',
      from: OUTAGE_STARTS_AT_S,
      to: OUTAGE_ENDS_AT_S,
      fault: true,
    },
    { key: 'recovery', label: 'Recovery', from: OUTAGE_ENDS_AT_S, to: end },
  ]
  const live =
    virtualNowS === null
      ? undefined
      : segments.find((s) => virtualNowS >= s.from && virtualNowS < s.to)

  return (
    <div
      className="track"
      role="img"
      aria-label={
        virtualNowS === null
          ? 'A run: two minutes of normal traffic, a five-minute provider outage, then recovery until drained'
          : `${live ? live.label : 'Run complete'} at ${formatVirtual(virtualNowS)} of ${formatVirtual(end)}`
      }
    >
      {segments.map((segment) => (
        <div
          key={segment.key}
          className={['track-seg', segment.fault ? 'is-fault' : '', segment === live ? 'is-live' : '']
            .filter(Boolean)
            .join(' ')}
          style={{ flexGrow: segment.to - segment.from, flexBasis: 0 }}
        >
          {segment.label}
        </div>
      ))}
      {virtualNowS !== null && (
        <div
          className="track-head"
          style={{ left: `${Math.min(100, Math.max(0, (virtualNowS / end) * 100))}%` }}
        />
      )}
    </div>
  )
}
