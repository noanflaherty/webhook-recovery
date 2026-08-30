/**
 * The list of runs this browser has seen, in localStorage.
 *
 * Runs are already durable artifacts on the server -- every one keeps its rows
 * forever under its own `simulation_id`, and `?sim=<uuid>` addresses it. What
 * was missing is the index: nothing recorded *which* ids exist, so a run you
 * did not paste somewhere was unreachable the moment you started the next one.
 * That made the central comparison awkward in exactly the place it matters --
 * a FIFO run and a fair run are two URLs, and you had to keep both by hand.
 *
 * This is deliberately client-side rather than a `GET /api/simulation` listing.
 * The server has no notion of a user, so a global list would be every run by
 * everyone who ever opened the deployment, which is not what "my runs" means.
 * Per-browser history is both the honest scope and the useful one.
 *
 * The consequences of that choice, stated rather than hidden: the list does not
 * survive clearing site data, does not follow you to another browser, and can
 * name a run the server has since dropped. The last one is visible in the UI --
 * a run that 404s is shown as gone and can be removed -- rather than silently
 * filtered, because "I ran that and now it isn't there" is worth seeing.
 */

const KEY = 'webhook-recovery:runs'

/**
 * Cap on remembered runs. High enough that a session's worth of resets never
 * pushes out the run you actually wanted; low enough that the list page stays
 * one screen and does not fire fifty requests to render.
 */
const LIMIT = 30

export interface RunRecord {
  id: string
  /** Wall-clock ms when this browser first saw the run. */
  seenAt: number
}

function isRecord(value: unknown): value is RunRecord {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as RunRecord).id === 'string' &&
    typeof (value as RunRecord).seenAt === 'number'
  )
}

/** Newest first. Never throws: an unreadable or corrupt store reads as empty. */
export function listRuns(): RunRecord[] {
  let raw: string | null = null
  try {
    raw = localStorage.getItem(KEY)
  } catch {
    return [] // private-mode browsers throw on access rather than returning null
  }
  if (!raw) return []
  try {
    const parsed: unknown = JSON.parse(raw)
    // Filtered rather than trusted: this is parsed from storage a previous
    // version of this app wrote, which makes it the one input here that is not
    // under this code's control.
    return Array.isArray(parsed) ? parsed.filter(isRecord) : []
  } catch {
    return []
  }
}

function save(runs: RunRecord[]): RunRecord[] {
  const capped = runs.slice(0, LIMIT)
  try {
    localStorage.setItem(KEY, JSON.stringify(capped))
  } catch {
    /* nothing to do -- the URL still addresses the run, history is the extra */
  }
  return capped
}

/**
 * Record a run, or move it to the front if already known.
 *
 * Called for every live run the app *opens*, not only ones it creates, so a run
 * someone shared as a URL joins your history by being looked at. That is the
 * behaviour you want from a history: it answers "what have I had open", and a
 * pasted link is the case where you are least likely to still have the URL.
 */
export function rememberRun(id: string): RunRecord[] {
  const existing = listRuns()
  const seenAt = existing.find((run) => run.id === id)?.seenAt ?? Date.now()
  return save([{ id, seenAt }, ...existing.filter((run) => run.id !== id)])
}

export function forgetRun(id: string): RunRecord[] {
  return save(listRuns().filter((run) => run.id !== id))
}
