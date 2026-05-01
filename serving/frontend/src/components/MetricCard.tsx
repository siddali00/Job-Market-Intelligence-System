interface MetricCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  highlight?: boolean;
  /** Compact layout for dense dashboards */
  size?: "md" | "sm";
}

export default function MetricCard({
  label,
  value,
  subtitle,
  highlight,
  size = "md",
}: MetricCardProps) {
  const isSm = size === "sm";
  return (
    <div
      className={`rounded-md border ${
        highlight
          ? "bg-sky-950/40 border-sky-800/60"
          : "bg-slate-900/50 border-slate-800/80"
      } ${isSm ? "p-1.5" : "p-4"}`}
    >
      <p
        className={`text-slate-500 uppercase tracking-wide ${isSm ? "text-[9px] leading-tight" : "text-xs"}`}
      >
        {label}
      </p>
      <p
        className={`font-bold tabular-nums ${isSm ? "text-sm text-slate-100 mt-0.5" : "text-2xl text-white mt-1"} ${
          highlight && !isSm ? "text-sky-400" : ""
        } ${highlight && isSm ? "text-sky-300" : ""}`}
      >
        {value}
      </p>
      {subtitle && (
        <p className={`text-slate-500 ${isSm ? "text-[9px] mt-0.5 leading-tight" : "text-xs mt-1"}`}>
          {subtitle}
        </p>
      )}
    </div>
  );
}
