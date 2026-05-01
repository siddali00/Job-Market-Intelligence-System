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

const COUNTRY_OPTIONS = [
  { value: "", label: "All countries" },
  { value: "US", label: "United States" },
  { value: "GB", label: "United Kingdom" },
  { value: "CA", label: "Canada" },
  { value: "AU", label: "Australia" },
  { value: "DE", label: "Germany" },
  { value: "IN", label: "India" },
];

const SORT_OPTIONS = [
  { value: "median", label: "Median (high first)" },
  { value: "sample", label: "Sample size" },
  { value: "role", label: "Role A–Z" },
];

const fmt = (n: number | null) =>
  n != null ? `$${Math.round(n).toLocaleString()}` : "—";

export default function SalaryExplorer() {
  const [searchParams, setSearchParams] = useSearchParams();
  const roleFilter = searchParams.get("q") ?? "";
  const countryFilter = searchParams.get("country") ?? "";
  const minSample = searchParams.get("min_n") ?? "3";
  const sortBy = searchParams.get("sort") ?? "median";

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
    fetchSalaries(roleFilter || undefined, countryFilter || undefined)
      .then((d) => {
        setSalaries(d.data);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [roleFilter, countryFilter]);

  const minN = Math.max(1, parseInt(minSample, 10) || 3);

  const processed = useMemo(() => {
    let rows = salaries.filter((r) => r.sample_size >= minN);
    if (sortBy === "median") {
      rows = [...rows].sort((a, b) => (b.salary_median ?? 0) - (a.salary_median ?? 0));
    } else if (sortBy === "sample") {
      rows = [...rows].sort((a, b) => b.sample_size - a.sample_size);
    } else {
      rows = [...rows].sort((a, b) => a.role.localeCompare(b.role));
    }
    return rows;
  }, [salaries, minN, sortBy]);

  const topMedian = processed.reduce(
    (best, row) => (row.salary_median ?? 0) > (best?.salary_median ?? 0) ? row : best,
    processed[0]
  );

  const chartData = processed
    .filter((r) => r.salary_median != null)
    .slice(0, 12)
    .map((r) => ({
      name: `${r.role} (${r.country})`,
      p25: r.salary_p25 ?? 0,
      median: r.salary_median ?? 0,
      p75: r.salary_p75 ?? 0,
    }));

  return (
    <div>
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90"> Compensation </p>
      <h1 className="mb-0.5 text-lg font-semibold text-slate-100">Salary explorer</h1>
      <p className="mb-3 text-xs text-slate-500">
        Filter roles and countries, set a minimum sample for reliability, and sort to match how you compare
        offers.
      </p>

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
        <FilterSelect
          label="Min n"
          value={minSample}
          options={[
            { value: "1", label: "n ≥ 1" },
            { value: "3", label: "n ≥ 3" },
            { value: "5", label: "n ≥ 5" },
            { value: "10", label: "n ≥ 10" },
            { value: "20", label: "n ≥ 20" },
          ]}
          onChange={(v) => setParam("min_n", v)}
        />
        <FilterSelect
          label="Sort"
          value={sortBy}
          options={SORT_OPTIONS}
          onChange={(v) => setParam("sort", v)}
        />
      </FilterBar>

      <div className="mb-3 grid grid-cols-2 gap-1.5 sm:grid-cols-4">
        <MetricCard size="sm" label="Rows (after n)" value={processed.length} />
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
        <h2 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
          Median by role × country (top 12, filtered)
        </h2>
        {loading ? (
          <div className="flex h-72 items-center justify-center text-sm text-slate-500">Loading…</div>
        ) : chartData.length === 0 ? (
          <div className="flex h-72 items-center justify-center text-sm text-slate-500">
            No rows match your filters.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData} layout="vertical">
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
                width={150}
              />
              <Tooltip
                formatter={(v: number) => fmt(v)}
                contentStyle={{
                  background: "#0f172a",
                  border: "1px solid #334155",
                  borderRadius: 6,
                  fontSize: 11,
                }}
              />
              <Bar dataKey="median" name="Median" radius={[0, 3, 3, 0]}>
                {chartData.map((_, i) => (
                  <Cell key={i} fill={i === 0 ? "#10b981" : "#0ea5e9"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
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
