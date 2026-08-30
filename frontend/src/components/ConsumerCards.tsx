/**
 * One card per consumer: where its backlog is now, and what its policies saved.
 *
 * `caught_up_after_s` is the headline of the fairness claim and the route
 * returns null for it unconditionally, so it is derived here from the metrics
 * buffer. The card says which of the two it is showing rather than quietly
 * presenting a client-side estimate as a server-side fact.
 */
import type { ConsumerRead, MetricsBucket } from '../api/types'
import { colorFor } from '../theme'
import { deriveCaughtUpAfter, peakBacklog, type ConsumerRef } from '../transform/series'

interface Props {
  consumers: ConsumerRead[]
  refs: ConsumerRef[]
  buckets: MetricsBucket[]
}

export function ConsumerCards({ consumers, refs, buckets }: Props) {
  return (
    <section className="panel">
      <h2>Consumers</h2>
      <div className="cards">
        {consumers.map((consumer) => {
          const derived = deriveCaughtUpAfter(buckets, consumer.id)
          const caughtUp = consumer.caught_up_after_s ?? derived
          const dropped = consumer.expired + consumer.superseded
          return (
            <article className="card" key={consumer.id}>
              <h3>
                <span className="swatch" style={{ background: colorFor(refs, consumer.id) }} />
                {consumer.name}
              </h3>

              <dl>
                <dt>backlog</dt>
                <dd className="big">{consumer.backlog.toLocaleString()}</dd>

                <dt>in flight</dt>
                <dd>
                  {consumer.in_flight} <span className="muted">/ {consumer.concurrency_cap} cap</span>
                </dd>

                <dt>delivered</dt>
                <dd>{consumer.delivered.toLocaleString()}</dd>

                <dt>peak backlog</dt>
                <dd>{peakBacklog(buckets, consumer.id).toLocaleString()}</dd>

                <dt>caught up</dt>
                <dd>
                  {caughtUp === null ? (
                    <span className="muted">still draining</span>
                  ) : (
                    <>
                      {Math.round(caughtUp)}s after outage
                      {consumer.caught_up_after_s === null && (
                        <span className="chip muted">derived</span>
                      )}
                    </>
                  )}
                </dd>
              </dl>

              {/*
                Zero here against a Phase 1 backend is the correct reading, not a
                bug: nothing evaluates policies yet. Showing the row anyway is
                what makes Phase 3 visible the moment it lands.
              */}
              <div className="policy-row">
                <span className={dropped > 0 ? '' : 'muted'}>
                  {dropped.toLocaleString()} never sent
                </span>
                <span className="muted">
                  {consumer.expired.toLocaleString()} stale · {consumer.superseded.toLocaleString()}{' '}
                  superseded
                  {consumer.failed > 0 && ` · ${consumer.failed.toLocaleString()} failed`}
                </span>
              </div>
            </article>
          )
        })}
      </div>
    </section>
  )
}
