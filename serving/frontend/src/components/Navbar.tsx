import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  TrendingUp,
  DollarSign,
  Bell,
  Brain,
} from "lucide-react";
import clsx from "clsx";

const navItems: {
  to: string;
  label: string;
  icon: typeof LayoutDashboard;
  end?: boolean;
}[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard, end: true },
  { to: "/skills", label: "Skills", icon: TrendingUp },
  { to: "/salaries", label: "Salaries", icon: DollarSign },
  { to: "/alerts", label: "Alerts", icon: Bell },
  { to: "/predictions", label: "Predict", icon: Brain },
];

export default function Navbar() {
  return (
    <header className="sticky top-0 z-50 border-b border-slate-800/90 bg-slate-950/95 backdrop-blur-sm">
      <div className="container mx-auto max-w-7xl px-3 sm:px-4">
        <div className="flex h-11 items-center justify-between gap-2 sm:h-12">
          <NavLink
            to="/"
            className="flex min-w-0 items-center gap-1.5 rounded-md py-1 pr-2 text-slate-100 hover:text-white"
          >
            <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-sky-600/90">
              <LayoutDashboard className="h-3.5 w-3.5 text-white" />
            </div>
            <div className="min-w-0 leading-tight">
              <span className="block truncate text-xs font-semibold tracking-tight sm:text-sm">
                Job Market Intelligence
              </span>
              <span className="hidden text-[10px] text-slate-500 sm:block">
                Workforce analytics
              </span>
            </div>
          </NavLink>
          <nav className="flex flex-wrap justify-end gap-0.5 sm:gap-1">
            {navItems.map(({ to, label, icon: Icon, end }) => (
              <NavLink
                key={to}
                to={to}
                title={label}
                end={end === true}
                className={({ isActive }) =>
                  clsx(
                    "flex items-center gap-1 rounded-md px-2 py-1 text-[11px] font-medium transition-colors sm:px-2.5 sm:py-1.5 sm:text-xs",
                    isActive
                      ? "bg-sky-600 text-white shadow-sm"
                      : "text-slate-400 hover:bg-slate-800/80 hover:text-slate-100"
                  )
                }
              >
                <Icon className="h-3.5 w-3.5 shrink-0 sm:h-4 sm:w-4" />
                <span className="hidden sm:inline">{label}</span>
              </NavLink>
            ))}
          </nav>
        </div>
      </div>
    </header>
  );
}
