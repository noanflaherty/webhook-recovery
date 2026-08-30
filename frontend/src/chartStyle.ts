/**
 * The bits of chart chrome both plots have to agree on.
 *
 * Recharts takes most of its styling as props rather than CSS, so anything two
 * charts must match on has to live somewhere shared or it drifts -- and these
 * two charts are read as one image, stacked and sharing an x-axis. A tooltip
 * that is a different grey on the lower one is the kind of thing nobody names
 * but everybody notices.
 */
import { createElement, type ReactElement } from 'react'

/** Floats above the recessed well, so it takes the panel plane, not the well. */
export const TOOLTIP_STYLE = {
  background: 'var(--panel)',
  border: '1px solid var(--rule)',
  borderRadius: 4,
  fontFamily: 'var(--face-data)',
  fontSize: 10,
  padding: '0.35rem 0.5rem',
} as const

export const LEGEND_STYLE = {
  fontFamily: 'var(--face-data)',
  fontSize: 9.5,
  letterSpacing: '0.04em',
  paddingTop: 4,
} as const

/**
 * The provider outage, drawn the way instruments draw an out-of-range span:
 * diagonal hatching rather than a colour wash.
 *
 * Not decoration. The share chart stacks translucent areas *over* this band, and
 * a flat tint under them just shifts every colour above it -- the reader sees
 * three slightly-wrong consumer colours instead of a marked region. Hatching
 * stays legible through the overlay because it varies, and it carries the right
 * meaning besides: this is a span where no reading was taken, not a span with a
 * different value.
 */
export function faultHatch(id: string): ReactElement {
  return createElement(
    'pattern',
    { id, width: 10, height: 10, patternUnits: 'userSpaceOnUse', patternTransform: 'rotate(45)' },
    createElement('rect', {
      width: 10,
      height: 10,
      fill: 'var(--alarm)',
      fillOpacity: 0.02,
      key: 'ground',
    }),
    createElement('line', {
      x1: 0,
      y1: 0,
      x2: 0,
      y2: 10,
      stroke: 'var(--alarm)',
      strokeWidth: 1,
      strokeOpacity: 0.11,
      key: 'rule',
    }),
  )
}
