import { useState } from "react";
import { predictSalary, type PredictResponse } from "../api/client";
import { Brain, Loader2 } from "lucide-react";

const SKILL_SUGGESTIONS = [
  "python", "sql", "spark", "aws", "docker", "kubernetes",
  "fastapi", "react", "tensorflow", "pytorch",
];

export default function Predictions() {
  const [title, setTitle] = useState("");
  const [location, setLocation] = useState("");
  const [skillInput, setSkillInput] = useState("");
  const [skills, setSkills] = useState<string[]>([]);
  const [remote, setRemote] = useState<boolean>(false);
  const [result, setResult] = useState<PredictResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const addSkill = (skill: string) => {
    const s = skill.trim().toLowerCase();
    if (s && !skills.includes(s)) setSkills((prev) => [...prev, s]);
    setSkillInput("");
  };

  const removeSkill = (skill: string) => setSkills((prev) => prev.filter((s) => s !== skill));

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!title || !location) return;
    setLoading(true);
    setError(null);
    try {
      const res = await predictSalary({ title, location, skills, remote });
      setResult(res);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Request failed");
    } finally {
      setLoading(false);
    }
  };

  const fmt = (n: number | null) => n != null ? `$${Math.round(n).toLocaleString()}` : "—";

  return (
    <div className="max-w-2xl">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-sky-500/90"> What-if </p>
      <h1 className="mb-0.5 text-lg font-semibold text-slate-100">Salary prediction</h1>
      <p className="mb-4 text-xs text-slate-500">
        Describe a role to estimate a range. The model activates after enough data and a training run.
      </p>

      <form
        onSubmit={handleSubmit}
        className="space-y-3 rounded-lg border border-slate-800/80 bg-slate-900/30 p-4"
      >
        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Job title *</label>
          <input
            required
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            placeholder="e.g. Data Engineer"
            className="w-full rounded-md border border-slate-700/90 bg-slate-950/60 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
          />
        </div>

        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Location *</label>
          <input
            required
            value={location}
            onChange={(e) => setLocation(e.target.value)}
            placeholder="e.g. San Francisco, CA or Remote"
            className="w-full rounded-md border border-slate-700/90 bg-slate-950/60 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
          />
        </div>

        <div>
          <label className="mb-1 block text-[11px] text-slate-500">Skills</label>
          <div className="flex gap-2 mb-2 flex-wrap">
            {skills.map((s) => (
              <span
                key={s}
                className="inline-flex items-center gap-1 rounded-full bg-sky-950/80 px-2 py-0.5 text-xs text-sky-300"
              >
                {s}
                <button type="button" onClick={() => removeSkill(s)} className="hover:text-white">×</button>
              </span>
            ))}
          </div>
          <div className="flex gap-2">
            <input
              value={skillInput}
              onChange={(e) => setSkillInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && (e.preventDefault(), addSkill(skillInput))}
              placeholder="Type a skill and press Enter"
              className="flex-1 rounded-md border border-slate-700/90 bg-slate-950/60 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
            />
          </div>
          <div className="flex flex-wrap gap-1.5 mt-2">
            {SKILL_SUGGESTIONS.filter((s) => !skills.includes(s)).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => addSkill(s)}
                className="rounded px-2 py-0.5 text-xs text-slate-500 transition-colors hover:bg-slate-800 hover:text-slate-200"
              >
                + {s}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="remote"
            checked={remote}
            onChange={(e) => setRemote(e.target.checked)}
            className="h-4 w-4 accent-sky-500"
          />
          <label htmlFor="remote" className="text-sm text-slate-300">Remote position</label>
        </div>

        <button
          type="submit"
          disabled={loading}
          className="flex w-full items-center justify-center gap-2 rounded-md bg-sky-600 py-2 font-medium text-white transition-colors hover:bg-sky-700 disabled:opacity-50"
        >
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Brain className="w-4 h-4" />}
          {loading ? "Predicting…" : "Predict Salary"}
        </button>
      </form>

      {error && (
        <div className="mt-4 bg-red-900/30 border border-red-700 text-red-300 rounded-lg px-4 py-3 text-sm">{error}</div>
      )}

      {result && (
        <div className={`mt-4 rounded-lg border p-6 ${
          result.status === "ok"
            ? "bg-green-900/20 border-green-700"
            : "bg-gray-900 border-gray-700"
        }`}>
          <h2 className="text-sm font-semibold text-gray-300 mb-4">Prediction Result</h2>
          {result.status === "ok" ? (
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-gray-400 text-sm">Predicted Range</span>
                <span className="text-2xl font-bold text-green-400">
                  {fmt(result.predicted_salary_min)} – {fmt(result.predicted_salary_max)}
                </span>
              </div>
              {result.confidence != null && (
                <div className="flex justify-between items-center">
                  <span className="text-gray-400 text-sm">Confidence</span>
                  <span className="text-white">{(result.confidence * 100).toFixed(0)}%</span>
                </div>
              )}
              {result.model_version && (
                <div className="flex justify-between items-center">
                  <span className="text-gray-400 text-sm">Model Version</span>
                  <span className="text-gray-300 text-xs font-mono">{result.model_version}</span>
                </div>
              )}
            </div>
          ) : (
            <div className="flex items-start gap-3">
              <Brain className="w-5 h-5 text-gray-500 mt-0.5 shrink-0" />
              <div>
                <p className="text-gray-300 text-sm font-medium">Model not yet trained</p>
                <p className="text-gray-500 text-sm mt-1">{result.message}</p>
                <p className="text-gray-600 text-xs mt-2">
                  Run <code className="bg-gray-800 px-1 rounded">python -m ml.train</code> after ingesting sufficient data.
                </p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
