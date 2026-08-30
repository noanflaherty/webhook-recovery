/**
 * The fairness exhibit: each consumer's share of delivery attempts.
 *
 * This is the chart the first claim is argued with, so it is the one most worth
 * being suspicious of. Two things are deliberate.
 *
 * **Shares are computed in `toShareWindows`, not by `stackOffset="expand"`.**
 * The expand offset divides by the row total, and the row total is legitimately
 * zero for the entire outage. That renders `NaN`, which Recharts draws as
 * nothing — indistinguishable from a consumer genuinely getting no share.
 *
 * **Five-second windows rather than per-second bars.** Five seconds is
 * `fairness_window_virtual_s`, the window the conductor's own rate term
 * averages over, so the picture is at the granularity the mechanism actually
 * controls. (`TECHNICAL_DESIGN.md` §UI says a per-virtual-second stacked bar;
 * over a 900-second run those are sub-pixel. Worth correcting the doc.)
 */
import { memo, useMemo } from 'react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { MetricsBucket } from '../api/types'
import {
  FAIRNESS_WINDOW_S,
  OUTAGE_ENDS_AT_S,
  OUTAGE_STARTS_AT_S,
  formatVirtual,
} from '../scenario'
import { colorByName } from '../theme'
import { toShareWindows, type ConsumerRef } from '../transform/series'

interface Props {
  buckets: MetricsBucket[]
  consumers: ConsumerRef[]
}

const percent = (value: number) => `${Math.round(value * 100)}%`

export const ShareChart = memo(function ShareChart({ buckets, consumers }: Props) {
  const data = useMemo(() => toShareWindows(buckets, consumers), [buckets, consumers])
  const equalShare = consumers.length > 0 ? 1 / consumers.length : null

  // Recharts drops a ReferenceArea whose `x2` falls outside the axis domain, so
  // an un-clamped band is invisible for the whole of the outage -- the one
  // stretch of the run where it is doing the most explaining. Clamp it to the
  // data, and omit it until the clock actually reaches the outage.
  const maxT = data.at(-1)?.t ?? 0
  const bandEnd = Math.min(OUTAGE_ENDS_AT_S, maxT)

  if (data.length === 0) {
    return (
      <section className="panel">
        <h2>Attempt share</h2>
        <div className="chart-empty">Waiting for the conductor&rsquo;s first metrics bucket…</div>
      </section>
    )
  }

  return (
    <section className="panel">
      <h2>Attempt share</h2>
      <p className="caption">
        Share of delivery attempts per {FAIRNESS_WINDOW_S}s window. Weights are equal, so a fair
        drain converges on equal thirds; a drained consumer's band correctly goes to zero, and the
        gap is the outage, when nobody attempted anything.
      </p>
      <ResponsiveContainer width="100%" height={220}>
        <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
          <CartesianGrid stroke="var(--line)" strokeDasharray="2 4" vertical={false} />
          <XAxis
            dataKey="t"
            type="number"
            domain={['dataMin', 'dataMax']}
            tickFormatter={formatVirtual}
            stroke="var(--muted)"
            fontSize={11}
          />
          <YAxis
            domain={[0, 1]}
            ticks={[0, 0.25, 0.5, 0.75, 1]}
            tickFormatter={percent}
            stroke="var(--muted)"
            fontSize={11}
            width={48}
          />
          <Tooltip
            labelFormatter={(label) => `t = ${formatVirtual(Number(label))}`}
            formatter={(value: unknown) => (typeof value === 'number' ? percent(value) : '—')}
            contentStyle={{
              background: 'var(--bg)',
              border: '1px solid var(--line)',
              borderRadius: 6,
              fontSize: 12,
            }}
          />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          {/*
            The same band as the backlog chart, so the hole in the areas reads
            as "the provider was down" rather than as missing data. Without it
            the most defensible thing this chart does -- refusing to draw a share
            of zero attempts -- looks like the chart failing to load.
          */}
          {bandEnd > OUTAGE_STARTS_AT_S && (
            <ReferenceArea
              x1={OUTAGE_STARTS_AT_S}
              x2={bandEnd}
              fill="var(--err)"
              fillOpacity={0.07}
              label={{ value: 'provider down', fill: 'var(--muted)', fontSize: 11 }}
            />
          )}
          {equalShare !== null && (
            <ReferenceLine
              y={equalShare}
              stroke="var(--muted)"
              strokeDasharray="4 4"
              label={{
                value: 'equal share',
                position: 'insideTopRight',
                fill: 'var(--muted)',
                fontSize: 10,
              }}
            />
          )}
          {consumers.map((consumer) => (
            <Area
              key={consumer.id}
              type="monotone"
              dataKey={consumer.name}
              stackId="share"
              stroke={colorByName(consumers, consumer.name)}
              fill={colorByName(consumers, consumer.name)}
              fillOpacity={0.55}
              strokeWidth={1}
              isAnimationActive={false}
              connectNulls={false}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
    </section>
  )
})
