import { useCallback, useState } from "react";

function toYMD(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export type DatePreset = "7d" | "30d" | "90d" | "ytd" | "all";

export function useDateRange(initialPreset: DatePreset = "90d") {
  const computeRange = useCallback((p: DatePreset) => {
    const end = new Date();
    end.setHours(0, 0, 0, 0);
    const start = new Date(end);
    switch (p) {
      case "7d":
        start.setDate(start.getDate() - 6);
        break;
      case "30d":
        start.setDate(start.getDate() - 29);
        break;
      case "90d":
        start.setDate(start.getDate() - 89);
        break;
      case "ytd":
        start.setTime(new Date(end.getFullYear(), 0, 1).getTime());
        break;
      case "all":
        start.setTime(new Date(2020, 0, 1).getTime());
        break;
      default:
        start.setDate(start.getDate() - 89);
    }
    return { start: toYMD(start), end: toYMD(end) };
  }, []);

  const [activePreset, setActivePreset] = useState<DatePreset | null>(initialPreset);
  const [start, setStart] = useState(() => computeRange(initialPreset).start);
  const [end, setEnd] = useState(() => computeRange(initialPreset).end);

  const applyPreset = useCallback(
    (p: DatePreset) => {
      setActivePreset(p);
      const r = computeRange(p);
      setStart(r.start);
      setEnd(r.end);
    },
    [computeRange]
  );

  const setStartCustom = useCallback((v: string) => {
    setStart(v);
    setActivePreset(null);
  }, []);

  const setEndCustom = useCallback((v: string) => {
    setEnd(v);
    setActivePreset(null);
  }, []);

  return {
    start,
    end,
    setStart: setStartCustom,
    setEnd: setEndCustom,
    activePreset,
    applyPreset,
  };
}
