interface MetricCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  highlight?: boolean;
}

export default function MetricCard({ label, value, subtitle, highlight }: MetricCardProps) {
  return (
    <div className={`rounded-lg p-4 border ${highlight ? "bg-brand-900/30 border-brand-700" : "bg-gray-900 border-gray-800"}`}>
      <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${highlight ? "text-brand-400" : "text-white"}`}>{value}</p>
      {subtitle && <p className="text-xs text-gray-500 mt-1">{subtitle}</p>}
    </div>
  );
}
