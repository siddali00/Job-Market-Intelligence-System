import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { fetchSalaries, type SalarySummary } from "../api/client";
import MetricCard from "../components/MetricCard";
import { FilterBar, FilterInput, FilterSelect } from "../components/FilterBar";

const DEFAULT_COUNTRY = "US";

const COUNTRY_OPTIONS = [
  { value: "all", label: "All countries" },
  { value: "US", label: "United States" },
  { value: "GB", label: "United Kingdom" },
  { value: "CA", label: "Canada" },
  { value: "AU", label: "Australia" },
  { value: "DE", label: "Germany" },
  { value: "IN", label: "India" },
];

const fmt = (n: number | null) =>
  n != null ? `$${Math.round(n).toLocaleString()}` : "—";

/** Matches gold-layer salary_summary (HAVING COUNT(*) >= 3); client-side filter kept for clarity. */
const MIN_SAMPLE = 3;

/** Vertical bar chart: pixels per row + axis padding (scrolls when tall). */
const CHART_PX_PER_ROW = 26;
const CHART_MIN_HEIGHT = 260;
const CHART_MAX_HEIGHT = 3600;

export default function SalaryExplorer() {
  const [searchParams, setSearchParams] = useSearchParams();
  const roleFilter = searchParams.get("q") ?? "";
  const countryRaw = searchParams.get("country");
  const countryFilter =
    countryRaw === null || countryRaw === "" ? DEFAULT_COUNTRY : countryRaw;

  const setParam = (key: string, value: string) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        if (value) p.set(key, value);
        else p.delete(key);
        return p;
      },
      { replace: true }
    );
  };

  const [salaries, setSalaries] = useState<SalarySummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchSalaries(
      roleFilter || undefined,
      countryFilter === "all" ? undefined : countryFilter
    )
      .then((d) => {
        setSalaries(d.data);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [roleFilter, countryFilter]);

  const processed = useMemo(() => {
    let rows = salaries.filter((r) => r.sample_size >= MIN_SAMPLE);
    rows = [...rows].sort((a, b) => (b.salary_median ?? 0) - (a.salary_median ?? 0));
    return rows;
  }, [salaries]);

  const topMedian = processed.reduce(
    (best, row) => (row.salary_median ?? 0) > (best?.salary_median ?? 0) ? row : best,
    processed[0]
  );

  const chartRows = processed.filter((r) => r.salary_median != null);

  const chartLabel = (r: SalarySummary) =>
    countryFilter === "all" ? `${r.role} (${r.country})` : r.role;

  const chartData = chartRows.map((r) => ({
    name: chartLabel(r),
    p25: r.salary_p25 ?? 0,
    median: r.salary_median ?? 0,
    p75: r.salary_p75 ?? 0,
  }));

  const chartHeight = Math.min(
    CHART_MAX_HEIGHT,
    Math.max(CHART_MIN_HEIGHT, chartData.length * CHART_PX_PER_ROW + 72)
  );

  return (
    <div>
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90">Salaries</p>
      <h1 className="mb-0.5 text-lg font-semibold text-slate-100">Salary explorer</h1>

      {error && (
        <div className="mb-4 rounded-lg border border-rose-800/50 bg-rose-950/30 px-3 py-2 text-xs text-rose-200">
          {error}
        </div>
      )}

      <FilterBar>
        <FilterInput
          label="Role"
          value={roleFilter}
          placeholder="e.g. Engineer"
          onChange={(v) => setParam("q", v)}
        />
        <FilterSelect
          label="Country"
          value={countryFilter}
          options={COUNTRY_OPTIONS}
          onChange={(v) => setParam("country", v)}
        />
      </FilterBar>

      <div className="mb-3 grid grid-cols-2 gap-1.5 sm:grid-cols-4">
        <MetricCard size="sm" label="Rows" value={processed.length} />
        <MetricCard
          size="sm"
          label="Top median"
          value={fmt(topMedian?.salary_median ?? null)}
          subtitle={topMedian ? `${topMedian.role} · ${topMedian.country}` : ""}
          highlight
        />
        <MetricCard
          size="sm"
          label="Total sample"
          value={processed.reduce((s, r) => s + r.sample_size, 0).toLocaleString()}
        />
        <MetricCard
          size="sm"
          label="Markets"
          value={new Set(processed.map((r) => r.country)).size}
        />
      </div>

      <div className="mb-3 rounded-lg border border-slate-800/80 bg-slate-900/30 p-3">
        <div className="mb-2 flex items-baseline justify-between gap-2">
          <h2 className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">
            Median by role × country
          </h2>
          <span className="text-[10px] text-slate-400">Values in USD as reported by source APIs</span>
        </div>
        {loading ? (
          <div className="flex h-72 items-center justify-center text-sm text-slate-500">Loading…</div>
        ) : chartData.length === 0 ? (
          <div className="flex h-72 items-center justify-center text-sm text-slate-500">
            No rows match your filters.
          </div>
        ) : (
          <div className="max-h-[min(85vh,720px)] overflow-y-auto pr-1">
            <ResponsiveContainer width="100%" height={chartHeight}>
              <BarChart data={chartData} layout="vertical" margin={{ left: 4, right: 8, top: 4, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
                <XAxis
                  type="number"
                  tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
                  tick={{ fontSize: 10, fill: "#94a3b8" }}
                />
                <YAxis
                  dataKey="name"
                  type="category"
                  tick={{ fontSize: 9, fill: "#94a3b8" }}
                  width={countryFilter === "all" ? 168 : 140}
                />
                <Tooltip
                  formatter={(v: number) => fmt(v)}
                  contentStyle={{
                    background: "#0f172a",
                    border: "1px solid #334155",
                    borderRadius: 6,
                    fontSize: 11,
                  }}
                  labelStyle={{ color: "#e2e8f0" }}
                  itemStyle={{ color: "#e2e8f0" }}
                />
                <Bar dataKey="median" name="Median" radius={[0, 3, 3, 0]}>
                  {chartData.map((_, i) => (
                    <Cell key={i} fill={i === 0 ? "#10b981" : "#0ea5e9"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-800/80 bg-slate-900/30">
        <div className="border-b border-slate-800/80 px-2.5 py-1.5">
          <h2 className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">Full table</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-[11px]">
            <thead>
              <tr className="border-b border-slate-800 text-left text-[10px] font-medium uppercase text-slate-500">
                <th className="px-2 py-1.5">Role</th>
                <th className="px-2 py-1.5">Country</th>
                <th className="px-2 py-1.5 text-right">P25</th>
                <th className="px-2 py-1.5 text-right">Median</th>
                <th className="px-2 py-1.5 text-right">P75</th>
                <th className="px-2 py-1.5 text-right">P90</th>
                <th className="px-2 py-1.5 text-right">n</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={7} className="px-2 py-6 text-center text-slate-500">
                    Loading…
                  </td>
                </tr>
              ) : processed.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-2 py-6 text-center text-slate-500">
                    No data yet.
                  </td>
                </tr>
              ) : (
                processed.map((row, i) => (
                  <tr
                    key={i}
                    className="border-b border-slate-800/30 transition-colors hover:bg-slate-800/20"
                  >
                    <td className="px-2 py-1 font-medium text-slate-100">{row.role}</td>
                    <td className="px-2 py-1 text-slate-500">{row.country}</td>
                    <td className="px-2 py-1 text-right text-slate-400">{fmt(row.salary_p25)}</td>
                    <td className="px-2 py-1 text-right font-medium text-emerald-400/90">
                      {fmt(row.salary_median)}
                    </td>
                    <td className="px-2 py-1 text-right text-slate-400">{fmt(row.salary_p75)}</td>
                    <td className="px-2 py-1 text-right text-slate-400">{fmt(row.salary_p90)}</td>
                    <td className="px-2 py-1 text-right text-slate-500">{row.sample_size}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
