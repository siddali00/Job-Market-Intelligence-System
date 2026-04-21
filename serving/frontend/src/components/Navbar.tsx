import { NavLink } from "react-router-dom";
import { TrendingUp, DollarSign, Bell, Brain } from "lucide-react";
import clsx from "clsx";

const navItems = [
  { to: "/skills", label: "Skill Trends", icon: TrendingUp },
  { to: "/salaries", label: "Salary Explorer", icon: DollarSign },
  { to: "/alerts", label: "Market Alerts", icon: Bell },
  { to: "/predictions", label: "Predictions", icon: Brain },
];

export default function Navbar() {
  return (
    <header className="bg-gray-900 border-b border-gray-800 sticky top-0 z-50">
      <div className="container mx-auto px-4 max-w-7xl">
        <div className="flex items-center justify-between h-14">
          <div className="flex items-center gap-2">
            <TrendingUp className="text-brand-500 w-5 h-5" />
            <span className="font-semibold text-white text-sm">Job Market Intelligence</span>
          </div>
          <nav className="flex gap-1">
            {navItems.map(({ to, label, icon: Icon }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) =>
                  clsx(
                    "flex items-center gap-1.5 px-3 py-2 rounded-md text-sm font-medium transition-colors",
                    isActive
                      ? "bg-brand-600 text-white"
                      : "text-gray-400 hover:text-white hover:bg-gray-800"
                  )
                }
              >
                <Icon className="w-4 h-4" />
                <span className="hidden sm:inline">{label}</span>
              </NavLink>
            ))}
          </nav>
        </div>
      </div>
    </header>
  );
}
