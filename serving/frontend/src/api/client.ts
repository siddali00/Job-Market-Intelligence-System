/**
 * Typed axios client for the FastAPI backend.
 * Base URL is read from the VITE_API_URL env var (set in .env).
 */

import axios from "axios";

/**
 * API base URL for axios.
 *
 * **Production (`vite build`):** `baseURL` is always `""` so the browser calls
 * same-origin `/api/*` only. Vercel `vercel.json` rewrites those to your EC2 backend.
 * We intentionally ignore `VITE_API_URL` in production so a mistaken
 * `http://localhost:8000` from CI/env can never be baked into the bundle.
 *
 * **Development (`vite dev`):** use optional `VITE_API_URL` (e.g. `http://localhost:8000`)
 * or leave unset and use the Vite `/api` proxy (see `vite.config.ts`).
 */
const raw = import.meta.env.VITE_API_URL;
const BASE_URL = import.meta.env.PROD
  ? ""
  : typeof raw === "string" && raw.trim() !== ""
    ? raw.trim()
    : "";

export const api = axios.create({
  baseURL: BASE_URL,
  timeout: 15_000,
  headers: { "Content-Type": "application/json" },
});

// ── Types matching FastAPI response shapes ─────────────────────────────────

export interface TrendingSkill {
  skill: string;
  total_jobs: number;
  peak_7d_avg: number | null;
  peak_30d_avg: number | null;
}

export interface SkillTimeseries {
  date: string;
  skill: string;
  job_count: number;
  moving_avg_7d: number | null;
}

export interface SalarySummary {
  role: string;
  country: string;
  sample_size: number;
  salary_median: number | null;
  salary_p25: number | null;
  salary_p75: number | null;
  salary_p90: number | null;
}

export interface RoleDemand {
  date: string;
  role: string;
  job_count: number;
  moving_avg_7d: number | null;
  moving_avg_30d: number | null;
}

export interface RemoteData {
  date: string;
  role: string;
  remote_count: number;
  onsite_count: number;
  remote_ratio: number | null;
}

export interface MarketAlert {
  alert_date: string;
  entity_type: "role" | "skill";
  entity_name: string;
  demand_7d_avg: number | null;
  demand_30d_avg: number | null;
  spike_ratio: number | null;
}

export interface PredictRequest {
  title: string;
  location: string;
  skills: string[];
  remote?: boolean;
}

export interface PredictResponse {
  predicted_salary_min: number | null;
  predicted_salary_max: number | null;
  confidence: number | null;
  model_version: string | null;
  status: string;
  message: string;
}

export interface OverviewMetrics {
  window: { start_date: string; end_date: string };
  jobs_in_window: number;
  jobs_total: number;
  demand_date_range: { min: string | null; max: string | null };
  distinct_roles: number;
  distinct_skills: number;
  salary_summary_rows: number;
  distinct_countries_salary: number;
  cooccurrence_rows: number;
  alerts_today: number;
  alerts_last_7d: number;
  last_alert_date: string | null;
  remote: {
    total_remote: number;
    total_onsite: number;
    remote_pct: number | null;
  };
  data_freshness: string;
}

export interface TopRole {
  role: string;
  total_jobs: number;
}

export interface RemoteSummary {
  total_remote: number;
  total_onsite: number;
  remote_pct: number | null;
  data_freshness: string;
}

export interface CooccurrenceRow {
  skill_a: string;
  skill_b: string;
  co_count: number;
}

/** Row for year-over-year chart: one object per year with numeric keys per skill + year */
export type YearlyChartRow = { year: number } & Record<string, number | string>;

export interface SkillsYearlyResponse {
  compare: "top10" | "all";
  years: number[];
  /** One entry per skill line in the year-over-year chart */
  skills: string[];
  chart: YearlyChartRow[];
  ranking: { skill: string; total_jobs: number }[];
  data_freshness: string;
}

// ── API helper functions ────────────────────────────────────────────────────

export async function fetchTrendingSkills(topN = 20, startDate?: string, endDate?: string) {
  const params: Record<string, string | number> = { top_n: topN };
  if (startDate) params.start_date = startDate;
  if (endDate) params.end_date = endDate;
  const { data } = await api.get<{ data: TrendingSkill[]; data_freshness: string }>("/api/skills/trending", { params });
  return data;
}

export async function fetchSkillTimeseries(skills: string[], startDate?: string, endDate?: string) {
  const params: Record<string, string | string[]> = { skills };
  if (startDate) params.start_date = startDate;
  if (endDate) params.end_date = endDate;
  const { data } = await api.get<{ data: SkillTimeseries[] }>("/api/skills/timeseries", { params });
  return data;
}

export async function fetchSalaries(role?: string, country?: string) {
  const params: Record<string, string> = {};
  if (role) params.role = role;
  if (country) params.country = country;
  const { data } = await api.get<{ data: SalarySummary[]; data_freshness: string }>("/api/salaries", { params });
  return data;
}

export async function fetchRoleDemand(role?: string, startDate?: string, endDate?: string) {
  const params: Record<string, string> = {};
  if (role) params.role = role;
  if (startDate) params.start_date = startDate;
  if (endDate) params.end_date = endDate;
  const { data } = await api.get<{ data: RoleDemand[] }>("/api/roles/demand", { params });
  return data;
}

export async function fetchAlerts(alertDate?: string, entityType?: "role" | "skill") {
  const params: Record<string, string> = {};
  if (alertDate) params.alert_date = alertDate;
  if (entityType) params.entity_type = entityType;
  const { data } = await api.get<{ data: MarketAlert[]; alert_date: string }>("/api/alerts", { params });
  return data;
}

export async function predictSalary(request: PredictRequest) {
  const { data } = await api.post<PredictResponse>("/api/predict/salary", request);
  return data;
}

export async function fetchOverviewMetrics(startDate?: string, endDate?: string) {
  const params: Record<string, string> = {};
  if (startDate) params.start_date = startDate;
  if (endDate) params.end_date = endDate;
  const { data } = await api.get<OverviewMetrics>("/api/overview/metrics", { params });
  return data;
}

export async function fetchTopRoles(
  topN = 10,
  startDate?: string,
  endDate?: string
) {
  const params: Record<string, string | number> = { top_n: topN };
  if (startDate) params.start_date = startDate;
  if (endDate) params.end_date = endDate;
  const { data } = await api.get<{ data: TopRole[] }>("/api/roles/top", { params });
  return data;
}

export async function fetchRemoteSummary() {
  const { data } = await api.get<RemoteSummary>("/api/remote/summary");
  return data;
}

export async function fetchCooccurrence(topN = 15) {
  const { data } = await api.get<{ data: CooccurrenceRow[] }>("/api/skills/cooccurrence", {
    params: { top_n: topN },
  });
  return data;
}

/** Full-history yearly aggregates; no date filter. */
export async function fetchSkillsYearly(compare: "top10" | "all" = "top10") {
  const { data } = await api.get<SkillsYearlyResponse>("/api/skills/yearly", {
    params: { compare },
  });
  return data;
}
