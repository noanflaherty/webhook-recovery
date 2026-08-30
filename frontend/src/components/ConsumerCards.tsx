/**
 * One card per consumer: what it represents, how it fared, and what its
 * policies saved.
 *
 * Deliberately *not* a status dump. Backlog and peak backlog were both here and
 * are both gone, because the chart directly below draws them per consumer over
 * time -- a live number that duplicates a line on the next panel costs a glance
 * and adds nothing. What is left is the two things no chart shows, which are
 * also exactly the two claims: how long this consumer took to catch up, and how
 * much of its backlog its own policies made unnecessary to send.
 *
 * `caught_up_after_s` is the headline of the fairness claim and the route
 * returns null for it unconditionally, so in practice it is always derived here
 * from the metrics buffer -- the first bucket after the outage in which this
 * consumer's backlog reads zero. The server value is still preferred if it ever
 * starts arriving; the card no longer labels which of the two it got, because a
 * "derived" badge that is on every card in every run marks nothing.
 */
import type { CSSProperties } from 'react'

import type { ConsumerRead, MetricsBucket } from '../api/types'
import { roleFor } from '../scenario'
import { colorFor } from '../theme'
import { deriveCaughtUpAfter, type ConsumerRef } from '../transform/series'

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
          // The left rail carries this consumer's trace colour. It does the job
          // the old swatch did, but structurally rather than as an ornament
          // beside the name: the card *is* the channel, so the channel colour
          // is the edge of the card, and binding a card to its line in the
          // charts below costs no legend lookup.
          return (
            <article
              className="card"
              key={consumer.id}
              style={{ '--card-channel': colorFor(refs, consumer.id) } as CSSProperties}
            >
              <h3>
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
                <dt>caught up</dt>
                <dd className="big">
                  {caughtUp === null ? (
                    <span className="pending">still draining</span>
                  ) : (
                    <>
                      {Math.round(caughtUp)}s
                      <span className="muted unit">after outage</span>
                    </>
                  )}
                </dd>

                {/*
                  Kept only as the scale for the line below it: "1,593 never
                  sent" is either alarming or routine depending on what it is a
                  fraction of, and the fraction is the second claim.
                */}
                <dt>delivered</dt>
                <dd>{consumer.delivered.toLocaleString()}</dd>
              </dl>

              {/*
                Zero is the correct reading for a consumer that set no policies,
                not a missing number -- which is why Acme renders the row too.
                The comparison only lands if the baseline shows its own zero
                next to Bolt's thousands.
              */}
              <div className="policy-row">
                <span className={dropped > 0 ? '' : 'muted'}>
                  {dropped.toLocaleString()} excluded by policies
                </span>
                <span className="breakdown">
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
