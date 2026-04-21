import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend, BarChart, Bar,
} from "recharts";
import { fetchTrendingSkills, fetchSkillTimeseries, type TrendingSkill, type SkillTimeseries } from "../api/client";
import MetricCard from "../components/MetricCard";
import { FilterBar, FilterSelect } from "../components/FilterBar";

const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"];
const TOP_N_OPTIONS = [
  { value: "10", label: "Top 10" },
  { value: "20", label: "Top 20" },
  { value: "30", label: "Top 30" },
];
const DEFAULT_SKILLS = ["python", "sql", "spark", "aws", "docker"];

export default function SkillTrends() {
  const [trending, setTrending] = useState<TrendingSkill[]>([]);
  const [timeseries, setTimeseries] = useState<SkillTimeseries[]>([]);
  const [topN, setTopN] = useState("20");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchTrendingSkills(parseInt(topN)),
      fetchSkillTimeseries(DEFAULT_SKILLS),
    ])
      .then(([trendData, tsData]) => {
        setTrending(trendData.data);
        setTimeseries(tsData.data);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [topN]);

  // Reshape timeseries for recharts: [{date, python: 12, sql: 9, ...}]
  const timeseriesMap: Record<string, Record<string, number>> = {};
  for (const row of timeseries) {
    if (!timeseriesMap[row.date]) timeseriesMap[row.date] = { date: row.date as unknown as number };
    timeseriesMap[row.date][row.skill] = row.job_count;
  }
  const chartData = Object.values(timeseriesMap).sort((a, b) =>
    String(a.date).localeCompare(String(b.date))
  );

  const skills = [...new Set(timeseries.map((r) => r.skill))];

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-1">Skill Trends</h1>
      <p className="text-gray-400 text-sm mb-6">Track which technical skills are growing fastest in demand.</p>

      {error && (
        <div className="bg-red-900/30 border border-red-700 text-red-300 rounded-lg px-4 py-3 mb-6 text-sm">
          {error} — make sure the API is running and data has been ingested.
        </div>
      )}

      <FilterBar>
        <FilterSelect label="Show" value={topN} options={TOP_N_OPTIONS} onChange={setTopN} />
      </FilterBar>

      {/* Summary metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
        <MetricCard label="Skills tracked" value={trending.length} />
        <MetricCard
          label="Top skill"
          value={trending[0]?.skill ?? "—"}
          subtitle={`${trending[0]?.total_jobs ?? 0} jobs`}
          highlight
        />
        <MetricCard label="7d avg (top)" value={trending[0]?.peak_7d_avg?.toFixed(1) ?? "—"} subtitle="postings/day" />
        <MetricCard label="Data points" value={timeseries.length} />
      </div>

      {/* Line chart: skill demand over time */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 mb-6">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Daily Demand Over Time</h2>
        {loading ? (
          <div className="h-64 flex items-center justify-center text-gray-500">Loading…</div>
        ) : chartData.length === 0 ? (
          <div className="h-64 flex items-center justify-center text-gray-500">No data yet — run the pipeline first.</div>
        ) : (
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: "#9ca3af" }} />
              <YAxis tick={{ fontSize: 11, fill: "#9ca3af" }} />
              <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }} />
              <Legend />
              {skills.map((skill, i) => (
                <Line
                  key={skill}
                  type="monotone"
                  dataKey={skill}
                  stroke={COLORS[i % COLORS.length]}
                  dot={false}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Bar chart: top N skills by total jobs */}
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
        <h2 className="text-sm font-semibold text-gray-300 mb-4">Top Skills by Total Job Postings</h2>
        {loading ? (
          <div className="h-64 flex items-center justify-center text-gray-500">Loading…</div>
        ) : trending.length === 0 ? (
          <div className="h-64 flex items-center justify-center text-gray-500">No data yet.</div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={trending.slice(0, 15)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11, fill: "#9ca3af" }} />
              <YAxis dataKey="skill" type="category" tick={{ fontSize: 11, fill: "#9ca3af" }} width={90} />
              <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }} />
              <Bar dataKey="total_jobs" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
