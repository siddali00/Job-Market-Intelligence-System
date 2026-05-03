import clsx from "clsx";
import { CalendarRange } from "lucide-react";
import type { DatePreset } from "../hooks/useDateRange";

const PRESETS: { id: DatePreset; label: string }[] = [
  { id: "7d", label: "7d" },
  { id: "30d", label: "30d" },
  { id: "90d", label: "90d" },
  { id: "ytd", label: "YTD" },
  { id: "all", label: "All" },
];

interface DateRangeToolbarProps {
  start: string;
  end: string;
  onStartChange: (v: string) => void;
  onEndChange: (v: string) => void;
  activePreset: DatePreset | null;
  onPreset: (p: DatePreset) => void;
  compact?: boolean;
}

export default function DateRangeToolbar({
  start,
  end,
  onStartChange,
  onEndChange,
  activePreset,
  onPreset,
  compact = false,
}: DateRangeToolbarProps) {
  return (
    <div
      className={clsx(
        "flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between",
        "rounded-lg border border-slate-800/90 bg-slate-900/40",
        compact ? "px-2 py-1.5" : "px-3 py-2"
      )}
    >
      <div className="flex flex-col gap-1.5 min-w-0 sm:flex-row sm:items-center sm:gap-3">
        <div className="flex items-center gap-1.5">
          <CalendarRange className="h-3.5 w-3.5 text-slate-500 shrink-0" />
          <span className="text-[10px] font-medium uppercase tracking-wider text-slate-500">
            Window
          </span>
        </div>
        <div className="flex flex-wrap gap-0.5">
          {PRESETS.map(({ id, label }) => (
            <button
              key={id}
              type="button"
              onClick={() => onPreset(id)}
              className={clsx(
                "rounded px-1.5 py-0.5 text-[11px] font-medium transition-colors",
                activePreset === id
                  ? "bg-sky-600 text-white"
                  : "text-slate-400 hover:bg-slate-800 hover:text-slate-200"
              )}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-1.5 sm:gap-2 pl-0 sm:pl-2">
        <label className="sr-only">From</label>
        <input
          type="date"
          value={start}
          onChange={(e) => onStartChange(e.target.value)}
          className="rounded border border-slate-700/90 bg-slate-950/80 px-1.5 py-0.5 text-[11px] text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500/60"
        />
        <span className="text-slate-400 text-[10px]">→</span>
        <label className="sr-only">To</label>
        <input
          type="date"
          value={end}
          onChange={(e) => onEndChange(e.target.value)}
          className="rounded border border-slate-700/90 bg-slate-950/80 px-1.5 py-0.5 text-[11px] text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500/60"
        />
      </div>
    </div>
  );
}
