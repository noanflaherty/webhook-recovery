/**
 * One card per consumer: what it represents, where its backlog is now, and what
 * its policies saved.
 *
 * `caught_up_after_s` is the headline of the fairness claim and the route
 * returns null for it unconditionally, so it is derived here from the metrics
 * buffer. The card says which of the two it is showing rather than quietly
 * presenting a client-side estimate as a server-side fact.
 */
import type { ConsumerRead, MetricsBucket } from '../api/types'
import { roleFor } from '../scenario'
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
          const role = roleFor(consumer.name)
          const derived = deriveCaughtUpAfter(buckets, consumer.id)
          const caughtUp = consumer.caught_up_after_s ?? derived
          const dropped = consumer.expired + consumer.superseded
          return (
            <article className="card" key={consumer.id}>
              <h3>
                <span className="swatch" style={{ background: colorFor(refs, consumer.id) }} />
                {consumer.name}
                {role && <span className="chip muted">{role.label}</span>}
              </h3>

              {/*
                What this consumer is demonstrating. Without it the three cards
                read as three customers who happen to be doing differently, and
                the reader is left to guess whether Acme draining last is the
                point or a defect. It is the point.
              */}
              {role && <p className="role">{role.blurb}</p>}

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
                Zero is the correct reading for a consumer that set no policies,
                not a missing number -- which is why Acme renders the row too.
                The comparison only lands if the baseline shows its own zero
                next to Bolt's thousands.
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
