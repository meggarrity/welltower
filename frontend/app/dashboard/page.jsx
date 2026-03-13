// app/dashboard/page.jsx
"use client";
import DataTable from "@/components/DataTable";
import { useState } from "react";

const TABS = [
  { label: "Rent Roll",       endpoint: "rentroll" },
  { label: "Properties",      endpoint: "properties" },
  { label: "Units",           endpoint: "units" },
  { label: "Residents",       endpoint: "residents" },
  { label: "Property SCD2",   endpoint: "property_scd" },
  { label: "Unit SCD2",       endpoint: "unit_scd" },
  { label: "Resident SCD2",   endpoint: "resident_scd" },
  { label: "KPIs",            endpoint: "kpis" }
];

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

function firstOfMonthISO() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-01`;
}

export default function Dashboard() {
  const [activeTab, setActiveTab]   = useState(0);
  const [startDate, setStartDate]   = useState(firstOfMonthISO());
  const [endDate,   setEndDate]     = useState(todayISO());

  const isKPIs     = TABS[activeTab].endpoint === "kpis";
  const isRentRoll = TABS[activeTab].endpoint === "rentroll";
  const showDateRange = isKPIs || isRentRoll;

  const endpoint = isKPIs
    ? `kpis/${startDate}/${endDate}`
    : isRentRoll
      ? `rentroll?start_date=${startDate}&end_date=${endDate}`
      : TABS[activeTab].endpoint;

  return (
    <main className="p-8">
      <h1 className="text-2xl font-bold mb-6">Property Management</h1>

      {/* Tab bar */}
      <div className="flex gap-2 border-b border-gray-200 mb-6 flex-wrap">
        {TABS.map((tab, i) => (
          <button
            key={tab.endpoint}
            onClick={() => setActiveTab(i)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === i
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Date range picker — KPIs and Rent Roll tabs */}
      {showDateRange && (
        <div className="flex items-center gap-4 mb-6">
          <label className="flex items-center gap-2 text-sm text-gray-600">
            Start
            <input
              type="date"
              value={startDate}
              onChange={e => setStartDate(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            />
          </label>
          <label className="flex items-center gap-2 text-sm text-gray-600">
            End
            <input
              type="date"
              value={endDate}
              onChange={e => setEndDate(e.target.value)}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            />
          </label>
        </div>
      )}

      {/* Active table */}
      <DataTable
        endpoint={endpoint}
        title={TABS[activeTab].label}
      />
    </main>
  );
}