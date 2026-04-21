import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell,
} from "recharts";
import { fetchSalaries, type SalarySummary } from "../api/client";
import MetricCard from "../components/MetricCard";
import { FilterBar, FilterInput, FilterSelect } from "../components/FilterBar";

const COUNTRY_OPTIONS = [
  { value: "", label: "All Countries" },
  { value: "US", label: "United States" },
  { value: "GB", label: "United Kingdom" },
  { value: "CA", label: "Canada" },
  { value: "AU", label: "Australia" },
  { value: "IN", label: "India" },
];

const fmt = (n: number | null) =>
  n != null ? `$${Math.round(n).toLocaleString()}` : "—";

export default function SalaryExplorer() {
  const [salaries, setSalaries] = useState<SalarySummary[]>([]);
  const [roleFilter, setRoleFilter] = useState("");
  const [countryFilter, setCountryFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchSalaries(roleFilter || undefined, countryFilter || undefined)
      .then((d) => { setSalaries(d.data); setError(null); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [roleFilter, countryFilter]);

  const topMedian = salaries.reduce(
    (best, row) => (row.salary_median ?? 0) > (best?.salary_median ?? 0) ? row : best,
    salaries[0]
  );

  const chartData = salaries
    .filter((r) => r.salary_median != null)
    .sort((a, b) => (b.salary_median ?? 0) - (a.salary_median ?? 0))
    .slice(0, 12)
    .map((r) => ({
      name: `${r.role} (${r.country})`,
      p25: r.salary_p25 ?? 0,
      median: r.salary_median ?? 0,
      p75: r.salary_p75 ?? 0,
    }));

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-1">Salary Explorer</h1>
      <p className="text-gray-400 text-sm mb-6">Compare salary distributions across roles and locations.</p>

      {error && (
        <div className="bg-red-900/30 border border-red-700 text-red-300 rounded-lg px-4 py-3 mb-6 text-sm">
          {error}
        </div>
      )}

      <FilterBar>
        <FilterInput label="Role" value={roleFilter} placeholder="e.g. Data Engineer" onChange={setRoleFilter} />
        <FilterSelect label="Country" value={countryFilter} options={COUNTRY_OPTIONS} onChange={setCountryFilter} />
      </FilterBar>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
        <MetricCard label="Roles with salary data" value={salaries.length} />
        <MetricCard
          label="Highest median"
          value={fmt(topMedian?.salary_median ?? null)}
          subtitle={topMedian ? `${topMedian.role} — ${topMedian.country}` : ""}
          highlight
        />
        <MetricCard label="Sample size (total)" value={salaries.reduce((s, r) => s + r.sample_size, 0).toLocaleString()} />
        <MetricCard label="Countries" value={new Set(salaries.map((r) => r.country)).size} />
      </div>

      {/* Median salary bar chart */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Median Salary by Role × Country (Top 12)</h2>
        {loading ? (
          <div className="h-72 flex items-center justify-center text-gray-500">Loading…</div>
        ) : chartData.length === 0 ? (
          <div className="h-72 flex items-center justify-center text-gray-500">No salary data yet.</div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={chartData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
              <XAxis type="number" tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`} tick={{ fontSize: 11, fill: "#9ca3af" }} />
              <YAxis dataKey="name" type="category" tick={{ fontSize: 10, fill: "#9ca3af" }} width={160} />
              <Tooltip
                formatter={(v: number) => fmt(v)}
                contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }}
              />
              <Bar dataKey="median" name="Median" radius={[0, 4, 4, 0]}>
                {chartData.map((_, i) => (
                  <Cell key={i} fill={i === 0 ? "#10b981" : "#3b82f6"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Data table */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-800">
          <h2 className="text-sm font-semibold text-gray-300">Full Salary Table</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-gray-500 uppercase border-b border-gray-800">
                <th className="px-4 py-2 text-left">Role</th>
                <th className="px-4 py-2 text-left">Country</th>
                <th className="px-4 py-2 text-right">P25</th>
                <th className="px-4 py-2 text-right">Median</th>
                <th className="px-4 py-2 text-right">P75</th>
                <th className="px-4 py-2 text-right">P90</th>
                <th className="px-4 py-2 text-right">n</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-500">Loading…</td></tr>
              ) : salaries.length === 0 ? (
                <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-500">No data yet.</td></tr>
              ) : (
                salaries.map((row, i) => (
                  <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                    <td className="px-4 py-2 text-white font-medium">{row.role}</td>
                    <td className="px-4 py-2 text-gray-400">{row.country}</td>
                    <td className="px-4 py-2 text-right text-gray-300">{fmt(row.salary_p25)}</td>
                    <td className="px-4 py-2 text-right text-green-400 font-medium">{fmt(row.salary_median)}</td>
                    <td className="px-4 py-2 text-right text-gray-300">{fmt(row.salary_p75)}</td>
                    <td className="px-4 py-2 text-right text-gray-300">{fmt(row.salary_p90)}</td>
                    <td className="px-4 py-2 text-right text-gray-500">{row.sample_size}</td>
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
