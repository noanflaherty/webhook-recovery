/**
 * The live data source: thin `fetch` wrappers over the routes in
 * `app/api/routes.py`.
 *
 * Relative URLs throughout. In production the FastAPI process serves this
 * bundle itself, and in development vite proxies `/api` to :8000 -- so the same
 * paths work in both and there is no base-URL configuration to get wrong.
 */
import type { DataSource } from './source'
import type {
  ConsumerRead,
  DecisionsPage,
  MetricsPage,
  ProcessRead,
  SimulationCreate,
  SimulationPatch,
  SimulationRead,
} from './types'

/** A failed request, carrying the status so callers can tell 404 from 500. */
export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: init?.body ? { 'content-type': 'application/json', ...init?.headers } : init?.headers,
  })
  if (!response.ok) {
    // FastAPI puts the useful half in `detail`; fall back to the status line
    // rather than rendering a raw HTML error page into the UI.
    let detail = response.statusText
    try {
      const body: unknown = await response.json()
      if (body && typeof body === 'object' && 'detail' in body) {
        detail = String((body as { detail: unknown }).detail)
      }
    } catch {
      /* non-JSON body -- the status line is the best we have */
    }
    throw new ApiError(response.status, `${path}: ${response.status} ${detail}`)
  }
  return (await response.json()) as T
}

export class LiveSource implements DataSource {
  readonly kind = 'live' as const
  readonly simulationId: string

  constructor(simulationId: string) {
    this.simulationId = simulationId
  }

  private get base(): string {
    return `/api/simulation/${this.simulationId}`
  }

  getSimulation(): Promise<SimulationRead> {
    return request<SimulationRead>(this.base)
  }

  getConsumers(): Promise<ConsumerRead[]> {
    return request<ConsumerRead[]>(`${this.base}/consumer`)
  }

  getMetrics(sinceBucket: number): Promise<MetricsPage> {
    return request<MetricsPage>(`${this.base}/metrics?since_bucket=${sinceBucket}`)
  }

  getDecisions(limit = 50): Promise<DecisionsPage> {
    return request<DecisionsPage>(`${this.base}/decisions?limit=${limit}`)
  }

  getProcesses(): Promise<ProcessRead[]> {
    // Not per-simulation: the process registry is global, because processes
    // outlive any one run.
    return request<ProcessRead[]>('/api/process')
  }

  patch(body: SimulationPatch): Promise<SimulationRead> {
    return request<SimulationRead>(this.base, {
      method: 'PATCH',
      body: JSON.stringify(body),
    })
  }
}

/**
 * Start a run.
 *
 * `POST /api/simulation` creates the row *and* seeds the cast in one
 * transaction, so this is the whole of "Reset" -- there is no separate reset
 * endpoint and there does not need to be, because everything is namespaced by
 * `simulation_id` and runs persist side by side.
 */
export function createRun(body: SimulationCreate = {}): Promise<SimulationRead> {
  return request<SimulationRead>('/api/simulation', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

/**
 * Retire a run by marking it `done`, which freezes its clock.
 *
 * Load-bearing rather than tidy: the producer emits into *every* simulation
 * whose status is `running` (`app/api/producer.py`), at ~13 deliveries per
 * virtual second each. Abandoning runs instead of retiring them multiplies the
 * load on the system the UI exists to measure, which would make the instrument
 * the cause of the reading.
 */
export function retireRun(simulationId: string): Promise<SimulationRead> {
  return request<SimulationRead>(`/api/simulation/${simulationId}`, {
    method: 'PATCH',
    body: JSON.stringify({ status: 'done' } satisfies SimulationPatch),
  })
}

/**
 * Read one run's simulation row, without standing up a `LiveSource` for it.
 *
 * The run list needs the current state of N runs once, not a polling source per
 * run -- and several of those ids may name runs the server no longer has, which
 * is a 404 the caller renders rather than an error the source should carry.
 */
export function fetchRun(simulationId: string): Promise<SimulationRead> {
  return request<SimulationRead>(`/api/simulation/${simulationId}`)
}

export function getHealth(): Promise<{ status: string; db: string }> {
  return request<{ status: string; db: string }>('/api/health')
}
