import { useEffect, useState } from "react";
import { fetchAlerts, type MarketAlert } from "../api/client";
import MetricCard from "../components/MetricCard";
import { FilterBar, FilterSelect } from "../components/FilterBar";
import { AlertTriangle } from "lucide-react";

const TYPE_OPTIONS = [
  { value: "", label: "All" },
  { value: "role", label: "Roles" },
  { value: "skill", label: "Skills" },
];

const spikeColor = (ratio: number | null) => {
  if (ratio == null) return "text-gray-400";
  if (ratio >= 5) return "text-red-400";
  if (ratio >= 3) return "text-orange-400";
  return "text-yellow-400";
};

export default function MarketAlerts() {
  const [alerts, setAlerts] = useState<MarketAlert[]>([]);
  const [entityType, setEntityType] = useState<"" | "role" | "skill">("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchAlerts(undefined, entityType || undefined)
      .then((d) => { setAlerts(d.data); setError(null); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [entityType]);

  const roleAlerts = alerts.filter((a) => a.entity_type === "role");
  const skillAlerts = alerts.filter((a) => a.entity_type === "skill");
  const topSpike = alerts.reduce(
    (best, a) => (a.spike_ratio ?? 0) > (best?.spike_ratio ?? 0) ? a : best,
    alerts[0]
  );

  return (
    <div>
      <h1 className="text-2xl font-bold text-white mb-1">Market Alerts</h1>
      <p className="text-gray-400 text-sm mb-6">
        Roles and skills whose 7-day demand exceeds 2× their 30-day average — early signals of surging demand.
      </p>

      {error && (
        <div className="bg-red-900/30 border border-red-700 text-red-300 rounded-lg px-4 py-3 mb-6 text-sm">
          {error}
        </div>
      )}

      <FilterBar>
        <FilterSelect
          label="Type"
          value={entityType}
          options={TYPE_OPTIONS}
          onChange={(v) => setEntityType(v as "" | "role" | "skill")}
        />
      </FilterBar>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
        <MetricCard label="Total alerts" value={alerts.length} />
        <MetricCard label="Role spikes" value={roleAlerts.length} />
        <MetricCard label="Skill spikes" value={skillAlerts.length} />
        <MetricCard
          label="Highest spike"
          value={topSpike ? `${topSpike.spike_ratio?.toFixed(1)}×` : "—"}
          subtitle={topSpike?.entity_name}
          highlight
        />
      </div>

      {alerts.length === 0 && !loading ? (
        <div className="bg-gray-900 border border-gray-800 rounded-lg p-12 text-center">
          <AlertTriangle className="w-10 h-10 text-gray-600 mx-auto mb-3" />
          <p className="text-gray-400">No alerts for today.</p>
          <p className="text-gray-600 text-sm mt-1">Alerts appear when demand spikes above 2× the 30-day average.</p>
        </div>
      ) : (
        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 uppercase border-b border-gray-800">
                  <th className="px-4 py-3 text-left">Type</th>
                  <th className="px-4 py-3 text-left">Name</th>
                  <th className="px-4 py-3 text-right">7d Avg</th>
                  <th className="px-4 py-3 text-right">30d Avg</th>
                  <th className="px-4 py-3 text-right">Spike Ratio</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-500">Loading…</td></tr>
                ) : (
                  alerts.map((alert, i) => (
                    <tr key={i} className="border-b border-gray-800/50 hover:bg-gray-800/30 transition-colors">
                      <td className="px-4 py-3">
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                          alert.entity_type === "skill"
                            ? "bg-blue-900/50 text-blue-300"
                            : "bg-purple-900/50 text-purple-300"
                        }`}>
                          {alert.entity_type}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-white font-medium">{alert.entity_name}</td>
                      <td className="px-4 py-3 text-right text-gray-300">{alert.demand_7d_avg?.toFixed(1) ?? "—"}</td>
                      <td className="px-4 py-3 text-right text-gray-300">{alert.demand_30d_avg?.toFixed(1) ?? "—"}</td>
                      <td className={`px-4 py-3 text-right font-bold ${spikeColor(alert.spike_ratio)}`}>
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
