interface FilterBarProps {
  children: React.ReactNode;
  className?: string;
}

export function FilterBar({ children, className = "" }: FilterBarProps) {
  return (
    <div
      className={`mb-4 flex flex-wrap items-center gap-2 rounded-lg border border-slate-800/80 bg-slate-900/40 px-2.5 py-2 sm:gap-3 sm:px-3 ${className}`}
    >
      {children}
    </div>
  );
}

interface FilterSelectProps {
  label: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
}

export function FilterSelect({ label, value, options, onChange }: FilterSelectProps) {
  return (
    <div className="flex items-center gap-2">
      <label className="whitespace-nowrap text-[11px] text-slate-500">{label}</label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="rounded-md border border-slate-700/90 bg-slate-950/60 px-2 py-1 text-sm text-slate-100 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </div>
  );
}

interface FilterInputProps {
  label: string;
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}

export function FilterInput({ label, value, placeholder, onChange }: FilterInputProps) {
  return (
    <div className="flex items-center gap-2">
      <label className="whitespace-nowrap text-[11px] text-slate-500">{label}</label>
      <input
        type="text"
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className="w-36 rounded-md border border-slate-700/90 bg-slate-950/60 px-2 py-1 text-sm text-slate-100 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
      />
    </div>
  );
}

interface FilterDateProps {
  label: string;
  value: string;
  onChange: (v: string) => void;
}

export function FilterDate({ label, value, onChange }: FilterDateProps) {
  return (
    <div className="flex items-center gap-1.5">
      <label className="whitespace-nowrap text-[11px] text-slate-500">{label}</label>
      <input
        type="date"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="rounded border border-slate-700/90 bg-slate-950/60 px-1.5 py-0.5 text-[11px] text-slate-200 focus:outline-none focus:ring-1 focus:ring-sky-500/50"
      />
    </div>
  );
}
