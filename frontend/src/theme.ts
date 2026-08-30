/**
 * One colour per consumer, shared by both charts and the cards.
 *
 * Assigned by seeding order rather than by name, so a run with a renamed or
 * re-cast consumer still gets stable colours. The three are picked to stay
 * distinguishable in the common forms of colour blindness and to survive the
 * compression of a screen recording.
 */
import type { ConsumerRef } from './transform/series'

const PALETTE = ['#3b7dd8', '#e0902f', '#12a594'] as const
const FALLBACK = '#8b8b88'

export function colorFor(consumers: ConsumerRef[], consumerId: number): string {
  const index = consumers.findIndex((c) => c.id === consumerId)
  return index < 0 ? FALLBACK : (PALETTE[index % PALETTE.length] ?? FALLBACK)
}

export function colorByName(consumers: ConsumerRef[], name: string): string {
  const found = consumers.find((c) => c.name === name)
  return found ? colorFor(consumers, found.id) : FALLBACK
}
