/**
 * One colour per consumer, shared by both charts and the cards.
 *
 * Assigned by seeding order rather than by name, so a run with a renamed or
 * re-cast consumer still gets stable colours. The three are picked to stay
 * distinguishable in the common forms of colour blindness and to survive the
 * compression of a screen recording.
 *
 * Read them as instrument channels rather than as brand colours: cyan, sodium
 * amber and magenta are what a multichannel recorder puts on a dark field, they
 * separate at one-pixel stroke widths, and they leave green free to mean
 * *status* rather than "the third consumer" -- which matters on a page where
 * green already means a run is healthy.
 */
import type { ConsumerRef } from './transform/series'

/**
 * Returned as CSS custom properties rather than literal hex.
 *
 * The page has a light theme as well as a dark one, and a trace tuned to sit on
 * a near-black well is washed out on a near-white one -- sodium amber worst of
 * all. Hex baked in here could only ever be right for one of the two. Handing
 * back `var(--ch-n)` moves the actual values into App.css beside every other
 * token, where the media query can answer for them, and costs nothing: Recharts
 * passes `stroke` and `fill` through to SVG attributes that resolve custom
 * properties, and the cards spend theirs on a custom property of their own.
 */
const PALETTE = ['var(--ch-1)', 'var(--ch-2)', 'var(--ch-3)'] as const
const FALLBACK = 'var(--faint)'

export function colorFor(consumers: ConsumerRef[], consumerId: number): string {
  const index = consumers.findIndex((c) => c.id === consumerId)
  return index < 0 ? FALLBACK : (PALETTE[index % PALETTE.length] ?? FALLBACK)
}

export function colorByName(consumers: ConsumerRef[], name: string): string {
  const found = consumers.find((c) => c.name === name)
  return found ? colorFor(consumers, found.id) : FALLBACK
}
