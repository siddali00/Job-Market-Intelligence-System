import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  Brain,
  Briefcase,
  Globe2,
  LineChart,
  Sparkles,
} from "lucide-react";
import {
  fetchCooccurrence,
  fetchOverviewMetrics,
  fetchSalaries,
  fetchTopRoles,
  fetchTrendingSkills,
  type CooccurrenceRow,
  type OverviewMetrics,
  type SalarySummary,
  type TopRole,
  type TrendingSkill,
} from "../api/client";
import { useDateRange } from "../hooks/useDateRange";
import DateRangeToolbar from "../components/DateRangeToolbar";
import MetricCard from "../components/MetricCard";

function Panel({
  title,
  action,
  children,
  className = "",
}: {
  title: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`rounded-lg border border-slate-800/80 bg-slate-900/30 overflow-hidden ${className}`}
    >
      <div className="flex items-center justify-between gap-2 border-b border-slate-800/80 px-2.5 py-1.5">
        <h2 className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
          {title}
        </h2>
        {action}
      </div>
      <div className="p-2.5">{children}</div>
    </div>
  );
}

const fmtMoney = (n: number | null) =>
  n != null ? `$${Math.round(n).toLocaleString()}` : "—";

export default function Dashboard() {
  const { start, end, setStart, setEnd, activePreset, applyPreset } = useDateRange("90d");
  const [metrics, setMetrics] = useState<OverviewMetrics | null>(null);
  const [trending, setTrending] = useState<TrendingSkill[]>([]);
  const [topRoles, setTopRoles] = useState<TopRole[]>([]);
  const [cooc, setCooc] = useState<CooccurrenceRow[]>([]);
  const [salaryTop, setSalaryTop] = useState<SalarySummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const m = await fetchOverviewMetrics(start, end);
        if (cancelled) return;
        setMetrics(m);
        const [trend, roles, pairs, sal] = await Promise.all([
          fetchTrendingSkills(15, start, end),
          fetchTopRoles(10, start, end),
          fetchCooccurrence(12),
          fetchSalaries(),
        ]);
        if (cancelled) return;
        setTrending(trend.data);
        setTopRoles(roles.data);
        setCooc(pairs.data);
        const byMed = [...sal.data]
          .filter((r) => r.salary_median != null)
          .sort((a, b) => (b.salary_median ?? 0) - (a.salary_median ?? 0));
        setSalaryTop(byMed.slice(0, 8));
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load dashboard");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [start, end]);

  const skillsLink = "/skills";

  const remotePct = metrics?.remote?.remote_pct;

  return (
    <div className="space-y-3 -mt-0.5">
      <header className="space-y-1.5">
        <div className="flex flex-wrap items-end justify-between gap-2">
          <div>
            <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-sky-500/90">
              Your snapshot
            </p>
            <h1 className="text-lg font-semibold text-slate-100 tracking-tight sm:text-xl">
              Job market intelligence
            </h1>
            <p className="text-xs text-slate-500 max-w-2xl leading-snug">
              Explore demand, pay, and remote mix in one place. Adjust the window and open a section for
              detail.
            </p>
          </div>
        </div>
        <DateRangeToolbar
          start={start}
          end={end}
          onStartChange={setStart}
          onEndChange={setEnd}
          activePreset={activePreset}
          onPreset={applyPreset}
        />
      </header>

      {error && (
        <div className="rounded-md border border-rose-800/60 bg-rose-950/30 px-3 py-2 text-xs text-rose-200">
          {error} — check that the API is reachable (production uses same-origin{" "}
          <code className="rounded bg-slate-900 px-1">/api</code> via Vercel → EC2).
        </div>
      )}

      {/* KPI row */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-1.5">
        <MetricCard
          size="sm"
          label="Postings in window"
          value={loading ? "…" : (metrics?.jobs_in_window ?? 0).toLocaleString()}
          subtitle="Indexed in range"
        />
        <MetricCard
          size="sm"
          label="Index coverage"
          value={loading ? "…" : (metrics?.jobs_total ?? 0).toLocaleString()}
          subtitle="All-time in DB"
        />
        <MetricCard
          size="sm"
          label="Remote share"
          value={loading || remotePct == null ? (loading ? "…" : "—") : `${remotePct}%`}
          subtitle="Of remote+onsite"
        />
        <MetricCard
          size="sm"
          label="Salary bands"
          value={loading ? "…" : (metrics?.salary_summary_rows ?? 0).toLocaleString()}
          subtitle={`${metrics?.distinct_countries_salary ?? 0} countries`}
        />
        <MetricCard
          size="sm"
          label="Data through"
          value={
            loading
              ? "…"
              : metrics?.demand_date_range?.max
                ? metrics.demand_date_range.max
                : "—"
          }
          subtitle="Gold demand"
          highlight
        />
      </div>

      <div className="grid grid-cols-1 gap-2 xl:grid-cols-2">
        <Panel
          title="In-demand skills"
          action={
            <Link
              to={skillsLink}
              className="inline-flex items-center gap-0.5 text-[10px] font-medium text-sky-400 hover:text-sky-300"
            >
              Trends <ArrowRight className="h-3 w-3" />
            </Link>
          }
        >
          {loading ? (
            <p className="text-xs text-slate-500 py-2">Loading…</p>
          ) : (
            <div className="overflow-x-auto -mx-0.5">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-left text-slate-500">
                    <th className="px-1 py-0.5 font-medium">#</th>
                    <th className="px-1 py-0.5 font-medium">Skill</th>
                    <th className="px-1 py-0.5 text-right font-medium">Jobs</th>
                    <th className="px-1 py-0.5 text-right font-medium">7d avg</th>
                  </tr>
                </thead>
                <tbody>
                  {trending.slice(0, 10).map((r, i) => (
                    <tr key={r.skill} className="border-t border-slate-800/50 text-slate-200">
                      <td className="px-1 py-0.5 text-slate-500">{i + 1}</td>
                      <td className="px-1 py-0.5 font-medium text-slate-100">{r.skill}</td>
                      <td className="px-1 py-0.5 text-right text-slate-300">{r.total_jobs}</td>
                      <td className="px-1 py-0.5 text-right text-sky-300/90">
                        {r.peak_7d_avg != null ? r.peak_7d_avg.toFixed(1) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>

        <Panel
          title="Top roles"
          action={
            <Link
              to={skillsLink}
              className="inline-flex items-center gap-0.5 text-[10px] font-medium text-sky-400 hover:text-sky-300"
            >
              Skills view <ArrowRight className="h-3 w-3" />
            </Link>
          }
        >
          {loading ? (
            <p className="text-xs text-slate-500 py-2">Loading…</p>
          ) : (
            <ul className="space-y-0.5">
              {topRoles.map((r, i) => (
                <li
                  key={r.role}
                  className="flex items-center justify-between gap-2 text-[11px] border-b border-slate-800/40 last:border-0 py-0.5"
                >
                  <span className="text-slate-500 w-3 shrink-0">{i + 1}</span>
                  <span className="flex-1 min-w-0 truncate font-medium text-slate-200">{r.role}</span>
                  <span className="text-slate-400 tabular-nums shrink-0">{r.total_jobs.toLocaleString()}</span>
                </li>
              ))}
            </ul>
          )}
        </Panel>

        <Panel title="Top salaries (median)">
          {loading ? (
            <p className="text-xs text-slate-500 py-2">Loading…</p>
          ) : (
            <ul className="space-y-0.5">
              {salaryTop.map((r) => (
                <li
                  key={`${r.role}-${r.country}`}
                  className="flex items-center justify-between gap-1 text-[11px] border-b border-slate-800/40 last:border-0 py-0.5"
                >
                  <span className="min-w-0 flex-1 truncate text-slate-200">{r.role}</span>
                  <span className="text-slate-500 shrink-0 text-[10px]">{r.country}</span>
                  <span className="text-emerald-400/90 font-medium tabular-nums shrink-0">
                    {fmtMoney(r.salary_median)}
                  </span>
                </li>
              ))}
            </ul>
          )}
          <Link
            to="/salaries"
            className="mt-2 inline-flex items-center gap-0.5 text-[10px] font-medium text-sky-400 hover:text-sky-300"
          >
            Open salary explorer <ArrowRight className="h-3 w-3" />
          </Link>
        </Panel>

        <Panel
          title="Skill pairs (co-occurrence)"
        >
          {loading ? (
            <p className="text-xs text-slate-500 py-2">Loading…</p>
          ) : (
            <ul className="space-y-0.5 max-h-48 overflow-y-auto pr-0.5">
              {cooc.map((c) => (
                <li
                  key={`${c.skill_a}-${c.skill_b}`}
                  className="flex items-center justify-between gap-1 text-[10px] text-slate-300 border-b border-slate-800/30 last:border-0 py-0.5"
                >
                  <span className="min-w-0 truncate">
                    <span className="text-slate-200">{c.skill_a}</span>
                    <span className="text-slate-600"> + </span>
                    <span className="text-slate-200">{c.skill_b}</span>
                  </span>
                  <span className="text-slate-500 tabular-nums shrink-0">{c.co_count}</span>
                </li>
              ))}
            </ul>
          )}
        </Panel>
      </div>

      <div className="rounded-lg border border-slate-800/80 bg-slate-900/20 p-3">
          <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500 mb-2">
            Quick tools
          </p>
          <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3">
            <Link
              to={skillsLink}
              className="flex items-center gap-1.5 rounded border border-slate-800/80 bg-slate-900/50 px-2 py-1.5 text-[11px] text-slate-200 hover:border-sky-700/50 hover:bg-slate-800/50"
            >
              <LineChart className="h-3.5 w-3.5 text-sky-500 shrink-0" />
              Skill trends
            </Link>
            <Link
              to="/salaries"
              className="flex items-center gap-1.5 rounded border border-slate-800/80 bg-slate-900/50 px-2 py-1.5 text-[11px] text-slate-200 hover:border-sky-700/50 hover:bg-slate-800/50"
            >
              <Briefcase className="h-3.5 w-3.5 text-emerald-500 shrink-0" />
              Salaries
            </Link>
            <Link
              to="/predictions"
              className="flex items-center gap-1.5 rounded border border-slate-800/80 bg-slate-900/50 px-2 py-1.5 text-[11px] text-slate-200 hover:border-sky-700/50 hover:bg-slate-800/50"
            >
              <Brain className="h-3.5 w-3.5 text-violet-400 shrink-0" />
              Predict
            </Link>
            <div className="col-span-2 sm:col-span-1 flex items-center gap-1.5 rounded border border-slate-800/40 bg-slate-950/30 px-2 py-1.5 text-[10px] text-slate-500">
              <Globe2 className="h-3.5 w-3.5 shrink-0" />
              {metrics ? `${metrics.distinct_skills} skills · ${metrics.distinct_roles} roles` : "…"}
            </div>
            <div className="flex items-center gap-1.5 rounded border border-slate-800/40 bg-slate-950/30 px-2 py-1.5 text-[10px] text-slate-500">
              <Sparkles className="h-3.5 w-3.5 text-slate-600 shrink-0" />
              <span className="truncate">
                {metrics?.data_freshness
                  ? `Refreshed ${new Date(metrics.data_freshness).toLocaleString()}`
                  : "—"}
              </span>
            </div>
          </div>
      </div>
    </div>
  );
}
