import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchAlerts, type MarketAlert } from "../api/client";
import MetricCard from "../components/MetricCard";
import { FilterBar, FilterDate, FilterSelect } from "../components/FilterBar";
import { AlertTriangle } from "lucide-react";

const TYPE_OPTIONS = [
  { value: "", label: "All types" },
  { value: "role", label: "Roles only" },
  { value: "skill", label: "Skills only" },
];

const spikeColor = (ratio: number | null) => {
  if (ratio == null) return "text-slate-400";
  if (ratio >= 5) return "text-rose-400";
  if (ratio >= 3) return "text-amber-400";
  return "text-yellow-400";
};

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

export default function MarketAlerts() {
  const [searchParams, setSearchParams] = useSearchParams();
  const alertDate = searchParams.get("date") ?? todayISO();
  const entityType = (searchParams.get("type") as "" | "role" | "skill") || "";

  const setAlertDate = (d: string) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        p.set("date", d);
        if (entityType) p.set("type", entityType);
        else p.delete("type");
        return p;
      },
      { replace: true }
    );
  };

  const setEntityType = (t: string) => {
    setSearchParams(
      (prev) => {
        const p = new URLSearchParams(prev);
        p.set("date", alertDate);
        if (t) p.set("type", t);
        else p.delete("type");
        return p;
      },
      { replace: true }
    );
  };

  const [alerts, setAlerts] = useState<MarketAlert[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchAlerts(alertDate, entityType || undefined)
      .then((d) => {
        setAlerts(d.data);
        setError(null);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [alertDate, entityType]);

  const roleAlerts = alerts.filter((a) => a.entity_type === "role");
  const skillAlerts = alerts.filter((a) => a.entity_type === "skill");
  const topSpike = alerts.reduce(
    (best, a) => (a.spike_ratio ?? 0) > (best?.spike_ratio ?? 0) ? a : best,
    alerts[0]
  );

  return (
    <div>
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90"> Signals </p>
      <h1 className="mb-0.5 text-lg font-semibold text-slate-100">Market alerts</h1>
      <p className="mb-3 text-xs text-slate-500">
        Surging demand: 7-day average at least double the 30-day baseline. Pick a run date to inspect
        historical spikes.
      </p>

      {error && (
        <div className="mb-4 rounded-lg border border-rose-800/50 bg-rose-950/30 px-3 py-2 text-xs text-rose-200">
          {error}
        </div>
      )}

      <FilterBar>
        <FilterDate label="Alert date" value={alertDate} onChange={setAlertDate} />
        <FilterSelect
          label="Filter"
          value={entityType}
          options={TYPE_OPTIONS}
          onChange={setEntityType}
        />
      </FilterBar>

      <div className="mb-3 grid grid-cols-2 gap-1.5 sm:grid-cols-4">
        <MetricCard size="sm" label="Alerts (date)" value={loading ? "…" : alerts.length} />
        <MetricCard size="sm" label="Role spikes" value={loading ? "…" : roleAlerts.length} />
        <MetricCard size="sm" label="Skill spikes" value={loading ? "…" : skillAlerts.length} />
        <MetricCard
          size="sm"
          label="Strongest spike"
          value={topSpike ? `${topSpike.spike_ratio?.toFixed(1)}×` : "—"}
          subtitle={topSpike?.entity_name}
          highlight
        />
      </div>

      {alerts.length === 0 && !loading ? (
        <div className="rounded-lg border border-slate-800/80 bg-slate-900/30 p-8 text-center">
          <AlertTriangle className="mx-auto mb-2 h-9 w-9 text-slate-600" />
          <p className="text-sm text-slate-400">No alerts for this date.</p>
          <p className="mt-1 text-xs text-slate-600">
            Try another date when the pipeline produced gold, or widen your research in Skills.
          </p>
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-slate-800/80 bg-slate-900/30">
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="border-b border-slate-800 text-left text-[10px] font-medium uppercase tracking-wide text-slate-500">
                  <th className="px-2 py-2">Type</th>
                  <th className="px-2 py-2">Name</th>
                  <th className="px-2 py-2 text-right">7d Avg</th>
                  <th className="px-2 py-2 text-right">30d Avg</th>
                  <th className="px-2 py-2 text-right">Spike</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={5} className="px-2 py-8 text-center text-slate-500">
                      Loading…
                    </td>
                  </tr>
                ) : (
                  alerts.map((alert, i) => (
                    <tr
                      key={i}
                      className="border-b border-slate-800/40 transition-colors hover:bg-slate-800/20"
                    >
                      <td className="px-2 py-1.5">
                        <span
                          className={`inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium ${
                            alert.entity_type === "skill"
                              ? "bg-sky-950/80 text-sky-300"
                              : "bg-violet-950/80 text-violet-300"
                          }`}
                        >
                          {alert.entity_type}
                        </span>
                      </td>
                      <td className="px-2 py-1.5 font-medium text-slate-100">{alert.entity_name}</td>
                      <td className="px-2 py-1.5 text-right text-slate-400">
                        {alert.demand_7d_avg?.toFixed(1) ?? "—"}
                      </td>
                      <td className="px-2 py-1.5 text-right text-slate-400">
                        {alert.demand_30d_avg?.toFixed(1) ?? "—"}
                      </td>
                      <td className={`px-2 py-1.5 text-right font-semibold ${spikeColor(alert.spike_ratio)}`}>
                        {alert.spike_ratio != null ? `${alert.spike_ratio.toFixed(1)}×` : "—"}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
