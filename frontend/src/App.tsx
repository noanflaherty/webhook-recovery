import { useEffect, useState } from 'react'
import './App.css'

/**
 * Phase 0 stub.
 *
 * Its only job is to prove the served-bundle path end to end: this file is
 * built by the node stage of the Dockerfile, copied into the python image, and
 * served by the same FastAPI process that answers the fetch below. If this
 * renders "db: ok" at http://localhost:8000, one container really does serve
 * the whole app.
 *
 * The real UI is Phase 3, built against the committed fixtures in
 * src/fixtures/.
 */

type Health = { status: string; db: string }
type ProcessRow = {
  id: string
  kind: string
  hostname: string
  pid: number
  is_leader: boolean
  heartbeat_age_s: number
}

const POLL_INTERVAL_MS = 2000

export default function App() {
  const [health, setHealth] = useState<Health | null>(null)
  const [processes, setProcesses] = useState<ProcessRow[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    async function poll() {
      try {
        const [healthRes, processRes] = await Promise.all([
          fetch('/api/health'),
          fetch('/api/process'),
        ])
        if (!healthRes.ok) throw new Error(`/api/health returned ${healthRes.status}`)
        if (!processRes.ok) throw new Error(`/api/process returned ${processRes.status}`)
        const nextHealth: Health = await healthRes.json()
        const nextProcesses: ProcessRow[] = await processRes.json()
        if (cancelled) return
        setHealth(nextHealth)
        setProcesses(nextProcesses)
        setError(null)
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err))
      }
    }

    void poll()
    const timer = setInterval(() => void poll(), POLL_INTERVAL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [])

  return (
    <main className="stub">
      <h1>webhook-recovery</h1>
      <p className="subtitle">Phase 0 skeleton — no business logic yet.</p>

      <section>
        <h2>API</h2>
        {error && <p className="error">{error}</p>}
        {health ? (
          <dl>
            <dt>status</dt>
            <dd>{health.status}</dd>
            <dt>db</dt>
            <dd className={health.db === 'ok' ? 'ok' : 'error'}>{health.db}</dd>
          </dl>
        ) : (
          !error && <p>checking…</p>
        )}
      </section>

      <section>
        <h2>Live processes ({processes.length})</h2>
        {processes.length === 0 ? (
          <p>none heartbeating inside the 15s liveness window.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>kind</th>
                <th>host</th>
                <th>pid</th>
                <th>heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {processes.map((p) => (
                <tr key={p.id}>
                  <td>
                    {p.kind}
                    {p.is_leader && <span className="badge">leader</span>}
                  </td>
                  <td>{p.hostname}</td>
                  <td>{p.pid}</td>
                  <td>{p.heartbeat_age_s.toFixed(1)}s ago</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  )
}
