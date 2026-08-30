/**
 * Backlog depth per consumer over virtual time.
 *
 * The narrative chart: three lines climbing together through the shaded outage,
 * then the shape of the drain after it. Whether the small consumer's line comes
 * down early or waits behind the big ones is the entire fairness claim, drawn
 * once.
 */
import { memo, useMemo } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { MetricsBucket } from '../api/types'
import { OUTAGE_ENDS_AT_S, OUTAGE_STARTS_AT_S, formatVirtual } from '../scenario'
import { colorByName } from '../theme'
import { toWideSeries, type ConsumerRef } from '../transform/series'

interface Props {
  buckets: MetricsBucket[]
  consumers: ConsumerRef[]
}

export const BacklogChart = memo(function BacklogChart({ buckets, consumers }: Props) {
  // A full run is ~900 x 3 points and this recomputes on every metrics poll, so
  // both the transform and the component are memoized. Animation is off for the
  // same reason: a re-render mid-transition redraws from the old values.
  const data = useMemo(() => toWideSeries(buckets, consumers), [buckets, consumers])

  // Recharts drops a ReferenceArea whose `x2` falls outside the axis domain, so
  // an un-clamped band is invisible for the whole of the outage -- the one
  // stretch of the run where it is doing the most explaining. Clamp it to the
  // data, and omit it until the clock actually reaches the outage.
  const maxT = data.at(-1)?.t ?? 0
  const bandEnd = Math.min(OUTAGE_ENDS_AT_S, maxT)

  // An empty chart with bare axes reads as broken. It is in fact the correct
  // rendering of the first seconds of every live run -- the conductor lags two
  // buckets before it writes anything -- and of a run whose conductor has died,
  // which is precisely the state this panel exists to make visible.
  if (data.length === 0) {
    return (
      <section className="panel">
        <h2>Backlog</h2>
        <div className="chart-empty">Waiting for the conductor&rsquo;s first metrics bucket…</div>
      </section>
    )
  }

  return (
    <section className="panel">
      <h2>Backlog</h2>
      <p className="caption">
        Undelivered work per consumer. The band is the provider outage — nothing is attempted
        inside it, so every line climbs; what matters is the order they come down.
      </p>
      <ResponsiveContainer width="100%" height={260}>
        <LineChart data={data} margin={{ top: 8, right: 12, bottom: 4, left: 4 }}>
          <CartesianGrid stroke="var(--line)" strokeDasharray="2 4" vertical={false} />
          {bandEnd > OUTAGE_STARTS_AT_S && (
            <ReferenceArea
              x1={OUTAGE_STARTS_AT_S}
              x2={bandEnd}
              fill="var(--err)"
              fillOpacity={0.07}
              label={{
                value: 'provider down',
                position: 'insideTop',
                fill: 'var(--muted)',
                fontSize: 11,
              }}
            />
          )}
          <XAxis
            dataKey="t"
            type="number"
            domain={['dataMin', 'dataMax']}
            tickFormatter={formatVirtual}
            stroke="var(--muted)"
            fontSize={11}
          />
          <YAxis stroke="var(--muted)" fontSize={11} width={48} allowDecimals={false} />
          <Tooltip
            labelFormatter={(label) => `t = ${formatVirtual(Number(label))}`}
            contentStyle={{
              background: 'var(--bg)',
              border: '1px solid var(--line)',
              borderRadius: 6,
              fontSize: 12,
            }}
          />
          <Legend iconType="plainline" wrapperStyle={{ fontSize: 12 }} />
          {consumers.map((consumer) => (
            <Line
              key={consumer.id}
              type="monotone"
              dataKey={consumer.name}
              stroke={colorByName(consumers, consumer.name)}
              strokeWidth={1.75}
              dot={false}
              isAnimationActive={false}
              connectNulls={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </section>
  )
})
