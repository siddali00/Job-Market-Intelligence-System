/**
 * Typed axios client for the FastAPI backend.
 * Base URL is read from the VITE_API_URL env var (set in .env).
 */

import axios from "axios";

const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

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
