import { useEffect, useState } from "react";
import {
  fetchPredictOptions,
  predictSalary,
  type PredictResponse,
  type PredictUiOptions,
} from "../api/client";
import { Brain, Loader2 } from "lucide-react";

export default function Predictions() {
  const [options, setOptions] = useState<PredictUiOptions | null>(null);
  const [optionsError, setOptionsError] = useState<string | null>(null);

  const [jobTitle, setJobTitle] = useState("");
  const [industry, setIndustry] = useState("");
  const [years, setYears] = useState(3);
  const [remoteStatus, setRemoteStatus] = useState<"Remote" | "On-site">("On-site");
  const [skills, setSkills] = useState<string[]>([]);

  const [result, setResult] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchPredictOptions()
      .then((o) => {
        setOptions(o);
        setOptionsError(null);
        if (o.job_titles.length) setJobTitle(o.job_titles[0]);
        if (o.industries.length) setIndustry(o.industries[0]);
        if (o.remote_options.length) {
          const r = o.remote_options[0];
          setRemoteStatus(r === "Remote" || r === "On-site" ? r : "On-site");
        }
      })
      .catch((e: unknown) =>
        setOptionsError(e instanceof Error ? e.message : "Could not load form options")
      );
  }, []);

  const toggleSkill = (s: string) => {
    setSkills((prev) => (prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s]));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!jobTitle || !industry) return;
    setLoading(true);
    setError(null);
    try {
      const res = await predictSalary({
        years_of_experience: years,
        job_title: jobTitle,
        industry,
        remote_status: remoteStatus,
        selected_skills: skills,
      });
      setResult(res);
      if (res.status !== "ok") setError(res.message);
      else setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Request failed");
    } finally {
      setLoading(false);
    }
  };

  const fmt = (n: number | null) => (n != null ? `$${Math.round(n).toLocaleString()}` : "—");

  const selectCls =
    "w-full rounded-md border border-slate-700/90 bg-slate-950/60 px-3 py-2 text-sm text-slate-100 focus:outline-none focus:ring-1 focus:ring-sky-500/50";

  return (
    <div className="max-w-2xl">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90">Estimator</p>
      <h1 className="mb-4 text-lg font-semibold text-slate-100">Salary prediction</h1>

      {optionsError && (
        <div className="mb-4 rounded-lg border border-red-800/80 bg-red-950/30 px-3 py-2 text-sm text-red-300">
          {optionsError}
        </div>
      )}

      {!options && !optionsError && (
        <div className="mb-4 flex items-center gap-2 text-sm text-slate-500">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading form options…
        </div>
      )}

      <form
        onSubmit={handleSubmit}
        className="space-y-3 rounded-lg border border-slate-800/80 bg-slate-900/30 p-4"
      >
        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Job title *</label>
          <select
            required
            className={selectCls}
            value={jobTitle}
            onChange={(e) => setJobTitle(e.target.value)}
            disabled={!options}
          >
            {(options?.job_titles ?? []).map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Industry *</label>
          <select
            required
            className={selectCls}
            value={industry}
            onChange={(e) => setIndustry(e.target.value)}
            disabled={!options}
          >
            {(options?.industries ?? []).map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="mb-1 block text-[11px] text-slate-500">
            Years of experience: <span className="text-slate-300">{years.toFixed(1)}</span>
          </label>
          <input
            type="range"
            min={options?.experience_range?.[0] ?? 0}
            max={options?.experience_range?.[1] ?? 30}
            step={0.5}
            value={years}
            onChange={(e) => setYears(parseFloat(e.target.value))}
            disabled={!options}
            className="w-full accent-sky-500"
          />
        </div>

        <div>
          <span className="mb-1 block text-[11px] text-slate-500">Workplace</span>
          <div className="flex flex-wrap gap-3">
            {(options?.remote_options ?? ["Remote", "On-site"]).map((r) => (
              <label key={r} className="flex items-center gap-2 text-sm text-slate-300">
                <input
                  type="radio"
                  name="remote"
                  value={r}
                  checked={remoteStatus === r}
                  onChange={() => setRemoteStatus(r as "Remote" | "On-site")}
                  disabled={!options}
                  className="accent-sky-500"
                />
                {r}
              </label>
            ))}
          </div>
        </div>

        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Skills (multi-select)</label>
          <div className="flex max-h-40 flex-wrap gap-1.5 overflow-y-auto rounded-md border border-slate-800/80 bg-slate-950/40 p-2">
            {(options?.available_skills ?? []).map((s) => {
              const on = skills.includes(s);
              return (
                <button
                  key={s}
                  type="button"
                  disabled={!options}
                  onClick={() => toggleSkill(s)}
                  className={`rounded px-2 py-0.5 text-xs transition-colors ${
                    on
                      ? "bg-sky-600 text-white"
                      : "bg-slate-800/80 text-slate-400 hover:bg-slate-700 hover:text-slate-200"
                  }`}
                >
                  {s}
                </button>
              );
            })}
          </div>
        </div>

        <button
          type="submit"
          disabled={loading || !options}
          className="flex w-full items-center justify-center gap-2 rounded-md bg-sky-600 py-2 font-medium text-white transition-colors hover:bg-sky-700 disabled:opacity-50"
        >
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Brain className="h-4 w-4" />}
          {loading ? "Predicting…" : "Predict salary"}
        </button>
      </form>

      {error && (
        <div className="mt-4 rounded-lg border border-amber-800/80 bg-amber-950/20 px-4 py-3 text-sm text-amber-200/90">
          {error}
        </div>
      )}

      {result && result.status === "ok" && (
        <div className="mt-4 rounded-lg border border-green-800/80 bg-green-950/20 p-6">
          <h2 className="mb-4 text-sm font-semibold text-slate-300">Prediction result</h2>
          <div className="space-y-3">
            {result.predicted_point != null && (
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-400">Point estimate (annual)</span>
                <span className="text-xl font-bold text-green-400">{fmt(result.predicted_point)}</span>
              </div>
            )}
            <div className="flex items-center justify-between">
              <span className="text-sm text-slate-400">Approximate range (±12%)</span>
              <span className="text-lg font-semibold text-green-300/90">
                {fmt(result.predicted_salary_min)} – {fmt(result.predicted_salary_max)}
              </span>
            </div>
            {result.model_version && (
              <div className="flex justify-between text-xs text-slate-500">
                <span>Model</span>
                <span className="font-mono">{result.model_version}</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
