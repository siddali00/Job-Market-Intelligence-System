import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  BarChart,
  Bar,
} from "recharts";
import { fetchSkillsYearly, type SkillsYearlyResponse } from "../api/client";
import { FilterBar, FilterSelect } from "../components/FilterBar";

const fmtSkill = (s: string) => s.toLowerCase();

const COLORS = [
  "#0ea5e9",
  "#10b981",
  "#f59e0b",
  "#f43f5e",
  "#8b5cf6",
  "#06b6d4",
  "#ec4899",
  "#a3e635",
  "#94a3b8",
  "#f97316",
  "#22d3ee",
  "#c084fc",
  "#4ade80",
];

const COMPARE_OPTIONS = [
  { value: "top10", label: "Compare top 10 skills" },
  { value: "all", label: "Compare all skills" },
];

export default function SkillTrends() {
  const [compare, setCompare] = useState<"top10" | "all">("top10");
  const [yearly, setYearly] = useState<SkillsYearlyResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchSkillsYearly(compare)
      .then((d) => {
        setYearly(d);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [compare]);

  const chart = yearly?.chart ?? [];
  const skills = yearly?.skills ?? [];
  const ranking = yearly?.ranking ?? [];
  const mode = yearly?.compare ?? compare;

  const barData = useMemo(
    () => ranking.map((r) => ({ skill: r.skill, total: r.total_jobs })),
    [ranking]
  );

  const manyLines = skills.length > 24;
  const lineStroke = manyLines ? 1 : 2;
  const showDots = skills.length <= 15;

  const chartTitle =
    mode === "all"
      ? `Skill Trends (YoY)`
      : "Skill Trends (Top 10, YoY)";

  const barTitle =
    mode === "all" ? "All-time totals (every skill)" : "All-time totals (top 10 skills)";

  return (
    <div>
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90"> Demand </p>
      <h1 className="mb-0.5 text-lg font-semibold text-slate-100">Skill trends</h1>

      {error && (
        <div className="mb-4 rounded-lg border border-rose-800/50 bg-rose-950/30 px-3 py-2 text-xs text-rose-200">
          {error} — is the API running, and has silver→gold built skill demand?
        </div>
      )}

      <FilterBar>
        <FilterSelect
          label="View"
          value={compare}
          options={COMPARE_OPTIONS}
          onChange={(v) => setCompare(v as "top10" | "all")}
        />
      </FilterBar>

      <div className="mb-3 rounded-lg border border-slate-800/80 bg-slate-900/30 p-3">
        <h2 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
          {chartTitle}
        </h2>
        {loading ? (
          <div className="flex h-64 items-center justify-center text-sm text-slate-500">Loading…</div>
        ) : chart.length === 0 ? (
          <div className="flex flex-col items-center justify-center gap-1 px-4 py-12 text-center text-sm text-slate-500">
            <span>No yearly data yet.</span>
            <span className="text-xs text-slate-400">Run the pipeline to populate daily_skill_demand.</span>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={mode === "all" ? 340 : 300}>
            <LineChart data={chart} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis
                dataKey="year"
                type="number"
                domain={["dataMin", "dataMax"]}
                allowDecimals={false}
                tick={{ fontSize: 10, fill: "#94a3b8" }}
              />
              <YAxis tick={{ fontSize: 10, fill: "#94a3b8" }} />
              <Tooltip
                contentStyle={{
                  background: "#0f172a",
                  border: "1px solid #334155",
                  borderRadius: 6,
                  fontSize: 11,
                  maxHeight: 280,
                  overflowY: "auto",
                }}
                labelStyle={{ color: "#e2e8f0" }}
                itemStyle={{ color: "#e2e8f0" }}
              />
              <Legend
                layout="vertical"
                align="right"
                verticalAlign="middle"
                wrapperStyle={{
                  maxHeight: mode === "all" ? 300 : 200,
                  overflowY: "auto",
                  fontSize: manyLines ? 9 : 10,
                  paddingLeft: 8,
                }}
                iconSize={manyLines ? 6 : 8}
              />
              {skills.map((skill, i) => (
                <Line
                  key={skill}
                  type="monotone"
                  dataKey={skill}
                  name={fmtSkill(skill)}
                  stroke={COLORS[i % COLORS.length]}
                  dot={showDots ? { r: 2 } : false}
                  strokeWidth={lineStroke}
                  isAnimationActive={skills.length < 80}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      <div className="rounded-lg border border-slate-800/80 bg-slate-900/30 p-3">
        <h2 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
          {barTitle}
        </h2>
        {loading ? (
          <div className="flex h-72 items-center justify-center text-sm text-slate-500">Loading…</div>
        ) : barData.length === 0 ? (
          <div className="flex h-64 items-center justify-center text-sm text-slate-500">No data.</div>
        ) : (
          <div className={mode === "all" ? "max-h-[28rem] overflow-y-auto pr-1" : ""}>
            <ResponsiveContainer
              width="100%"
              height={mode === "all" ? Math.min(1200, Math.max(360, barData.length * 22)) : 320}
            >
              <BarChart data={barData} layout="vertical" margin={{ left: 4, right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 10, fill: "#94a3b8" }} />
                <YAxis
                  dataKey="skill"
                  type="category"
                  tickFormatter={fmtSkill}
                  tick={{ fontSize: mode === "all" ? 8 : 9, fill: "#94a3b8" }}
                  width={mode === "all" ? 100 : 110}
                />
                <Tooltip
                  contentStyle={{
                    background: "#0f172a",
                    border: "1px solid #334155",
                    borderRadius: 6,
                    fontSize: 11,
                  }}
                  labelStyle={{ color: "#e2e8f0" }}
                  itemStyle={{ color: "#e2e8f0" }}
                  formatter={(v: number) => [v.toLocaleString(), "postings"]}
                />
                <Bar dataKey="total" fill="#0ea5e9" radius={[0, 3, 3, 0]} name="Total postings" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}
        {mode === "all" && barData.length > 0 && (
          <p className="mt-2 text-center text-[10px] text-slate-400">
            {barData.length} skills — scroll the chart if needed
          </p>
        )}
      </div>

      {yearly?.data_freshness && (
        <p className="mt-2 text-center text-[10px] text-slate-400">
          API: {new Date(yearly.data_freshness).toLocaleString()}
        </p>
      )}
    </div>
  );
}
