import { useState } from "react";
import * as Tabs from "@radix-ui/react-tabs";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ErrorBar,
} from "recharts";
import { ChartNoAxesCombined, ClipboardList, Fingerprint } from "lucide-react";
import type { CampaignEntry } from "../../api/reports";
import type { CampaignView } from "../../api/generated/campaign-view";
import { formatNumber as number } from "../../lib/format";
import "./campaigns.css";

type Session = CampaignView["sessions"][number];
export type SeriesMetric = Session["metrics"][number];
const compact = (value: number) =>
  new Intl.NumberFormat("en", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);

export function chartPoints(
  metric: SeriesMetric,
  visible: number[],
  fitted: boolean,
) {
  return metric.points
    .filter((point) => visible.includes(point.rows))
    .map((point) => ({ ...point, fitted: fitted ? point.fitted : null }));
}

function LoadSeries({ view }: { view: CampaignView }) {
  const [sessionId, setSessionId] = useState(view.sessions[0]?.id ?? "");
  const session =
    view.sessions.find((item) => item.id === sessionId) ?? view.sessions[0];
  const [metricId, setMetricId] = useState(session?.metrics[0]?.id ?? "");
  const metric =
    session?.metrics.find((item) => item.id === metricId) ??
    session?.metrics[0];
  const [hidden, setHidden] = useState<number[]>([]);
  const [showMad, setShowMad] = useState(true);
  const [showFit, setShowFit] = useState(false);
  if (!session || !metric)
    return <p className="empty-line">No load-series evidence attached.</p>;
  const rows = metric.points.map((point) => point.rows);
  const visible = rows.filter((row) => !hidden.includes(row));
  const fitted = showFit && metric.fit.status === "reported_descriptive";
  const data = chartPoints(metric, visible, fitted);
  const maxRows = Math.max(...rows);
  return (
    <>
      <div className="campaign-toolbar">
        <label>
          Session
          <select
            aria-label="Scaling session"
            value={session.id}
            onChange={(event) => {
              setSessionId(event.target.value);
              setHidden([]);
            }}
          >
            {view.sessions.map((item) => (
              <option key={item.id} value={item.id}>
                {item.label}
                {item.started_at ? ` / ${item.started_at.slice(0, 10)}` : ""}
              </option>
            ))}
          </select>
        </label>
        <label>
          Metric
          <select
            aria-label="Scaling metric"
            value={metric.id}
            onChange={(event) => setMetricId(event.target.value)}
          >
            {session.metrics.map((item) => (
              <option key={item.id} value={item.id}>
                {item.label}
              </option>
            ))}
          </select>
        </label>
        <span className="campaign-environment">
          {session.engine} / {session.environment.replaceAll("_", " ")}
        </span>
      </div>
      <fieldset className="load-controls">
        <legend>Visible loads</legend>
        {rows.map((row) => (
          <label key={row}>
            <input
              type="checkbox"
              checked={visible.includes(row)}
              onChange={(event) =>
                setHidden((previous) =>
                  event.target.checked
                    ? previous.filter((value) => value !== row)
                    : [...previous, row],
                )
              }
            />
            {number(row, 0)}
          </label>
        ))}
      </fieldset>
      <div className="chart-heading">
        <div>
          <h3>{metric.label}</h3>
          <span className="muted">
            {metric.scope.replaceAll("_", " ")} / {metric.unit}
          </span>
        </div>
        <div className="chart-options">
          <label>
            <input
              type="checkbox"
              checked={showMad}
              onChange={(event) => setShowMad(event.target.checked)}
            />
            Show MAD
          </label>
          <label>
            <input
              type="checkbox"
              checked={fitted}
              disabled={metric.fit.status !== "reported_descriptive"}
              onChange={(event) => setShowFit(event.target.checked)}
            />
            Source fit
          </label>
        </div>
      </div>
      <figure
        className="load-figure"
        aria-label={`${metric.label} by total input rows`}
      >
        <div className="load-chart" data-testid="load-chart">
          {visible.length ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart
                data={data}
                margin={{ top: 18, right: 20, bottom: 30, left: 4 }}
                accessibilityLayer
              >
                <CartesianGrid stroke="#e3e9e5" vertical={false} />
                <XAxis
                  dataKey="rows"
                  type="number"
                  domain={[0, maxRows]}
                  ticks={[0, maxRows / 2, maxRows]}
                  tickFormatter={compact}
                  tick={{ fontSize: 12 }}
                  label={{
                    value: "Total input rows",
                    position: "bottom",
                    offset: 8,
                  }}
                />
                <YAxis
                  width={58}
                  domain={[(minimum: number) => Math.min(0, minimum), "auto"]}
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => number(value, 2)}
                />
                <Tooltip
                  labelFormatter={(value) =>
                    `${number(Number(value), 0)} total rows`
                  }
                  formatter={(value, name) => [
                    `${number(Number(value))} ${metric.unit}`,
                    name,
                  ]}
                  contentStyle={{
                    fontSize: 13,
                    borderColor: "#dce3e0",
                    borderRadius: 4,
                  }}
                />
                <Line
                  dataKey="median"
                  name="Median"
                  type="linear"
                  stroke="#0c7164"
                  strokeWidth={2}
                  dot={{ r: 4, strokeWidth: 2, fill: "#fff" }}
                  activeDot={{ r: 6 }}
                  connectNulls={false}
                  isAnimationActive={false}
                >
                  {showMad && (
                    <ErrorBar
                      dataKey="mad"
                      width={5}
                      stroke="#0c7164"
                      direction="y"
                    />
                  )}
                </Line>
                {fitted && (
                  <Line
                    dataKey="fitted"
                    name="Source descriptive fit"
                    type="linear"
                    stroke="#9a6332"
                    strokeWidth={1.5}
                    strokeDasharray="5 4"
                    dot={false}
                    connectNulls={false}
                    isAnimationActive={false}
                  />
                )}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="empty-line">No loads selected.</p>
          )}
        </div>
        <figcaption>
          <span className="series-key">
            <span />
            Reported medians
          </span>
          {showMad && <span>MAD whiskers; not confidence intervals</span>}
          {fitted && <span className="fit-key">Source descriptive fit</span>}
        </figcaption>
      </figure>
      <div className="fit-readout">
        {metric.fit.status === "reported_descriptive" ? (
          <>
            <span>
              Source slope{" "}
              <strong>
                {number(metric.fit.slope_per_100k_rows, 4)} {metric.unit} / 100k
                rows
              </strong>
            </span>
            <span>
              Intercept{" "}
              <strong>
                {number(metric.fit.intercept, 4)} {metric.unit}
              </strong>
            </span>
            <span>
              Descriptive R² <strong>{number(metric.fit.r_squared, 4)}</strong>
            </span>
          </>
        ) : (
          <span>
            Fit unavailable: <code>{metric.fit.reason}</code>
          </span>
        )}
      </div>
      <p className="notice">
        {metric.definition} Source fits cover the complete planned load set, not
        a filtered subset. No linearity or accuracy claim.
      </p>
      <p className="load-table-caption">
        {session.label}: {visible.length} of {rows.length} planned loads shown;{" "}
        {session.planned_repetitions ?? "unknown"} planned attempts per load
      </p>
      <div className="load-table-wrap">
        <table
          className="load-table"
          aria-label={`${session.label} load readings`}
        >
          <thead>
            <tr>
              <th>Total rows</th>
              <th className="numeric">Median ({metric.unit})</th>
              <th className="numeric">MAD</th>
              <th>Coverage</th>
              <th>Verified / planned</th>
            </tr>
          </thead>
          <tbody>
            {data.map((point) => (
              <tr key={point.rows}>
                <td>
                  {number(point.rows, 0)}
                  {point.missing_reason && (
                    <span className="reason">{point.missing_reason}</span>
                  )}
                </td>
                <td className="numeric">{number(point.median)}</td>
                <td className="numeric">{number(point.mad)}</td>
                <td>
                  {point.coverage === null
                    ? "\u2014"
                    : `${number(point.coverage * 100, 1)}%`}
                </td>
                <td>
                  {point.verified ?? "?"} / {point.planned ?? "?"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function SuiteInventory({ view }: { view: CampaignView }) {
  const [pipeline, setPipeline] = useState("all");
  const [caseName, setCaseName] = useState("all");
  const inventory = view.inventory.filter(
    (row) =>
      (pipeline === "all" || row.pipeline === pipeline) &&
      (caseName === "all" || row.case === caseName),
  );
  const totalPlanned = view.inventory.reduce(
    (sum, row) => sum + row.planned,
    0,
  );
  return (
    <>
      <div className="campaign-toolbar">
        <label>
          Pipeline
          <select
            aria-label="Inventory pipeline"
            value={pipeline}
            onChange={(event) => setPipeline(event.target.value)}
          >
            <option value="all">All pipelines</option>
            {Array.from(new Set(view.inventory.map((row) => row.pipeline))).map(
              (value) => (
                <option key={value}>{value}</option>
              ),
            )}
          </select>
        </label>
        <label>
          Case
          <select
            aria-label="Inventory case"
            value={caseName}
            onChange={(event) => setCaseName(event.target.value)}
          >
            <option value="all">All cases</option>
            {Array.from(new Set(view.inventory.map((row) => row.case))).map(
              (value) => (
                <option key={value}>{value}</option>
              ),
            )}
          </select>
        </label>
        <span className="muted">
          {totalPlanned} planned attempts across the full suite
        </span>
      </div>
      <div className="load-table-wrap">
        <table>
          <thead>
            <tr>
              <th>Pipeline / case</th>
              <th>Attempted / planned</th>
              <th>Expectation matched</th>
              <th>Outputs verified</th>
            </tr>
          </thead>
          <tbody>
            {inventory.map((row) => (
              <tr key={row.pointer}>
                <td>
                  {row.pipeline.replaceAll("_", " ")}
                  <span className="reason">
                    {row.case.replaceAll("_", " ")}
                  </span>
                </td>
                <td>
                  {row.attempted} / {row.planned}
                </td>
                <td>
                  {row.expected_behavior_matched} / {row.planned}
                </td>
                <td>
                  {row.verified_outputs} / {row.planned}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="notice">
        Expected rejection of a negative fixture is separate from verified
        useful work. Filters do not change the planned denominator.
      </p>
    </>
  );
}

export default function CampaignWorkspace({ entry }: { entry: CampaignEntry }) {
  const { view } = entry;
  return (
    <Tabs.Root
      className="campaign-workspace"
      defaultValue={view.sessions.length ? "loads" : "inventory"}
    >
      <Tabs.List className="tabs" aria-label="Campaign view">
        {view.sessions.length > 0 && (
          <Tabs.Trigger className="tab" value="loads">
            <ChartNoAxesCombined />
            Load progression
          </Tabs.Trigger>
        )}
        {view.inventory.length > 0 && (
          <Tabs.Trigger className="tab" value="inventory">
            <ClipboardList />
            Inventory
          </Tabs.Trigger>
        )}
        <Tabs.Trigger className="tab" value="evidence">
          <Fingerprint />
          Evidence
        </Tabs.Trigger>
      </Tabs.List>
      <Tabs.Content value="loads" className="campaign-content">
        <LoadSeries view={view} />
      </Tabs.Content>
      <Tabs.Content value="inventory" className="campaign-content">
        <SuiteInventory view={view} />
      </Tabs.Content>
      <Tabs.Content value="evidence" className="campaign-content">
        <h3>Campaign provenance</h3>
        <dl className="provenance">
          <dt>Origin</dt>
          <dd>
            {entry.origin === "example"
              ? "Saved example; not re-executed"
              : entry.origin === "local-api"
                ? "Local Python structural admission"
                : "Imported view; not revalidated by Python"}
          </dd>
          <dt>Format</dt>
          <dd>{view.report_kind}</dd>
          <dt>Reported status</dt>
          <dd>{view.status}</dd>
          <dt>View schema</dt>
          <dd>{view.view_schema_version}</dd>
        </dl>
        {view.sessions.map((session) => (
          <section className="evidence-section" key={session.id}>
            <h3>{session.label}</h3>
            <dl className="provenance">
              <dt>Started</dt>
              <dd>{session.started_at ?? "Not reported"}</dd>
              <dt>Profile ID</dt>
              <dd>
                <code>{session.profile_id ?? "Not reported"}</code>
              </dd>
              <dt>Environment</dt>
              <dd>{session.environment}</dd>
            </dl>
          </section>
        ))}
        <section className="evidence-section">
          <h3>Limitations</h3>
          <ul className="warning-list">
            {view.warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        </section>
      </Tabs.Content>
    </Tabs.Root>
  );
}
