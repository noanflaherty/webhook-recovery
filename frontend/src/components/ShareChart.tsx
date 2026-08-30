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
import { LEGEND_STYLE, TOOLTIP_STYLE, faultHatch } from '../chartStyle'
import { colorByName } from '../theme'
import type { FairDrainFlip } from '../hooks/useRun'
import { toShareWindows, type ConsumerRef } from '../transform/series'

interface Props {
  buckets: MetricsBucket[]
  consumers: ConsumerRef[]
  /** Where the fair-drain toggle moved, and which way. See `useRun`. */
  fairDrainFlips?: FairDrainFlip[]
}

const percent = (value: number) => `${Math.round(value * 100)}%`

const FAULT_HATCH_ID = 'fault-hatch-share'
const FAULT_HATCH = faultHatch(FAULT_HATCH_ID)

export const ShareChart = memo(function ShareChart({
  buckets,
  consumers,
  fairDrainFlips = [],
}: Props) {
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
        <div className="well">
          <div className="chart-empty">Waiting for the conductor&rsquo;s first metrics bucket…</div>
        </div>
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
      <div className="well">
        <ResponsiveContainer width="100%" height={214}>
          <AreaChart data={data} margin={{ top: 10, right: 14, bottom: 2, left: 0 }}>
            <defs>{FAULT_HATCH}</defs>
            <CartesianGrid stroke="var(--rule)" strokeDasharray="1 5" vertical={false} />
            <XAxis
              dataKey="t"
              type="number"
              domain={['dataMin', 'dataMax']}
              tickFormatter={formatVirtual}
              stroke="var(--rule)"
              tick={{ fill: 'var(--faint)', fontSize: 9 }}
              tickLine={{ stroke: 'var(--rule)' }}
              minTickGap={44}
            />
            <YAxis
              domain={[0, 1]}
              ticks={[0, 0.25, 0.5, 0.75, 1]}
              tickFormatter={percent}
              stroke="var(--rule)"
              tick={{ fill: 'var(--faint)', fontSize: 9 }}
              tickLine={{ stroke: 'var(--rule)' }}
              width={46}
            />
            <Tooltip
              labelFormatter={(label) => `t = ${formatVirtual(Number(label))}`}
              formatter={(value: unknown) => (typeof value === 'number' ? percent(value) : '\u2014')}
              cursor={{ stroke: 'var(--dim)', strokeDasharray: '2 3' }}
              contentStyle={TOOLTIP_STYLE}
            />
            <Legend iconType="plainline" wrapperStyle={LEGEND_STYLE} />
            {/*
              The same span as the backlog chart, so the hole in the areas reads
              as "the provider was down" rather than as missing data. Without it
              the most defensible thing this chart does -- refusing to draw a
              share of zero attempts -- looks like the chart failing to load.

              Hatched rather than tinted because the areas stack *over* it: a
              flat wash under three translucent fills just shifts every consumer
              colour inside the window, so the reader sees three slightly-wrong
              colours instead of a marked region.
            */}
            {bandEnd > OUTAGE_STARTS_AT_S && (
              <ReferenceArea
                x1={OUTAGE_STARTS_AT_S}
                x2={bandEnd}
                fill={`url(#${FAULT_HATCH_ID})`}
                fillOpacity={1}
                label={{
                  value: 'PROVIDER DOWN',
                  fill: 'var(--alarm)',
                  fillOpacity: 0.75,
                  fontSize: 8.5,
                  letterSpacing: '0.14em',
                }}
              />
            )}
            {equalShare !== null && (
              <ReferenceLine
                y={equalShare}
                stroke="var(--dim)"
                strokeDasharray="3 3"
                label={{
                  value: 'EQUAL SHARE',
                  position: 'insideTopRight',
                  fill: 'var(--dim)',
                  fontSize: 8.5,
                  letterSpacing: '0.12em',
                }}
              />
            )}
            {/*
              Where the scheduler changed underneath the data. The chart is the
              argument, and an argument that needs narrating over is weaker than
              one you can point at: with the marker, the re-balance is a single
              image rather than a claim about what happened off-screen.

              Each marker names the state it moved *into*, and is coloured to
              match. "toggled" alone would say that something changed without
              saying which way -- and on a run with more than one flip, that
              leaves the reader to infer the direction by alternating from a
              starting state the chart never showed them.
            */}
            {fairDrainFlips.map((flip) => (
              <ReferenceLine
                key={`${flip.t}:${flip.enabled}`}
                x={flip.t}
                stroke={flip.enabled ? 'var(--ok)' : 'var(--signal)'}
                strokeWidth={1.25}
                label={{
                  value: flip.enabled ? 'FAIR DRAIN ON' : 'FAIR DRAIN OFF',
                  position: 'insideTopLeft',
                  fill: flip.enabled ? 'var(--ok)' : 'var(--signal)',
                  fontSize: 8.5,
                  letterSpacing: '0.12em',
                }}
              />
            ))}
            {consumers.map((consumer) => (
              <Area
                key={consumer.id}
                type="monotone"
                dataKey={consumer.name}
                stackId="share"
                stroke={colorByName(consumers, consumer.name)}
                fill={colorByName(consumers, consumer.name)}
                fillOpacity={0.42}
                strokeWidth={1}
                isAnimationActive={false}
                connectNulls={false}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </section>
  )
})
