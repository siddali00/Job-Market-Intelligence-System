import { Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import Dashboard from "./pages/Dashboard";
import SkillTrends from "./pages/SkillTrends";
import SalaryExplorer from "./pages/SalaryExplorer";
import Predictions from "./pages/Predictions";

export default function App() {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar />
      <main className="flex-1 container mx-auto px-3 py-4 sm:px-4 sm:py-5 max-w-7xl">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/skills" element={<SkillTrends />} />
          <Route path="/salaries" element={<SalaryExplorer />} />
          <Route path="/predictions" element={<Predictions />} />
        </Routes>
      </main>
    </div>
  );
}
