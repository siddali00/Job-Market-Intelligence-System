import { Routes, Route, Navigate } from "react-router-dom";
import Navbar from "./components/Navbar";
import SkillTrends from "./pages/SkillTrends";
import SalaryExplorer from "./pages/SalaryExplorer";
import MarketAlerts from "./pages/MarketAlerts";
import Predictions from "./pages/Predictions";

export default function App() {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1 container mx-auto px-4 py-6 max-w-7xl">
        <Routes>
          <Route path="/" element={<Navigate to="/skills" replace />} />
          <Route path="/skills" element={<SkillTrends />} />
          <Route path="/salaries" element={<SalaryExplorer />} />
          <Route path="/alerts" element={<MarketAlerts />} />
          <Route path="/predictions" element={<Predictions />} />
        </Routes>
      </main>
    </div>
  );
}
