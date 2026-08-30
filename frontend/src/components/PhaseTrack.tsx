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

/** Virtual seconds as whole minutes: the scripted phases are exact multiples. */
function minutes(seconds: number): string {
  return `${Math.round(seconds / 60)} min`
}

interface Segment {
  key: string
  label: string
  /**
   * How long this phase lasts, as copy.
   *
   * The first two are scripted and exact. Recovery is not: it runs until the
   * backlog is drained, which is both the variable the whole demo turns on and
   * the number that differs between the two schedulers. Printing the track's
   * own nominal length there would be inventing a duration the run does not
   * have, so it says what actually ends it instead.
   */
  note: string
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
    {
      key: 'normal',
      label: 'Normal',
      note: minutes(OUTAGE_STARTS_AT_S),
      from: 0,
      to: OUTAGE_STARTS_AT_S,
    },
    {
      key: 'outage',
      label: 'Provider down',
      note: minutes(OUTAGE_ENDS_AT_S - OUTAGE_STARTS_AT_S),
      from: OUTAGE_STARTS_AT_S,
      to: OUTAGE_ENDS_AT_S,
      fault: true,
    },
    {
      key: 'recovery',
      label: 'Recovery',
      note: 'until drained',
      from: OUTAGE_ENDS_AT_S,
      to: end,
    },
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
          ? `A run: ${segments.map((s) => `${s.label}, ${s.note}`).join('; then ')}`
          : `${live ? live.label : 'Run complete'} at ${formatVirtual(virtualNowS)} of ${formatVirtual(end)}`
      }
    >
      {segments.map((segment) => (
        <div
          key={segment.key}
          data-phase={segment.key}
          className={['track-seg', segment.fault ? 'is-fault' : '', segment === live ? 'is-live' : '']
            .filter(Boolean)
            .join(' ')}
          style={{ flexGrow: segment.to - segment.from, flexBasis: 0 }}
        >
          {segment.label}
          {/*
            Hidden by a container query when its own segment is too narrow to
            hold it -- see App.css. Each segment answers for itself, so `Normal`
            (a seventh of the track) drops its duration long before `Recovery`
            does, rather than all three vanishing at one page-level breakpoint.
          */}
          <span className="track-note">({segment.note})</span>
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
