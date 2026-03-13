// components/DataTable.jsx
"use client";
import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// --- Cell renderers keyed by column name ---
const RENDERERS = {
  is_current: v => (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
      v ? "bg-green-100 text-green-800" : "bg-gray-100 text-gray-500"
    }`}>
      {v ? "Current" : "Expired"}
    </span>
  ),

  occupied: v => (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
      v ? "bg-blue-100 text-blue-800" : "bg-gray-100 text-gray-500"
    }`}>
      {v ? "Occupied" : "Vacant"}
    </span>
  ),

  unit_status: v => {
    const styles = {
      active:   "bg-green-100 text-green-800",
      inactive: "bg-gray-100  text-gray-500",
      vacant:   "bg-yellow-100 text-yellow-800",
      occupied: "bg-blue-100  text-blue-800",
      notice:   "bg-orange-100 text-orange-800",
      eviction: "bg-red-100   text-red-800",
    };
    return (
      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
        styles[v] ?? "bg-gray-100 text-gray-600"
      }`}>
        {v ?? "—"}
      </span>
    );
  },

  rent: v => v != null
    ? <span className="font-mono">${Number(v).toLocaleString("en-US", { minimumFractionDigits: 2 })}</span>
    : <span className="text-gray-300">—</span>,

  // Any column ending in _date renders in a neutral style
};

function renderCell(col, value) {
  if (RENDERERS[col])              return RENDERERS[col](value);
  if (col.endsWith("_date") && value) return <span className="font-mono text-xs">{value}</span>;
  if (col === "effective_date" || col === "expiration_date")
                                   return <span className="font-mono text-xs">{value}</span>;
  if (value === null || value === undefined) return <span className="text-gray-300">—</span>;
  return String(value);
}

// Nicer column header labels
function formatHeader(col) {
  return col
    .replace(/_/g, " ")
    .replace(/\bid\b/gi, "ID")
    .replace(/\bscd2\b/gi, "SCD2")
    .replace(/\b\w/g, c => c.toUpperCase());
}

export default function DataTable({ endpoint, title, propertyId }) {
  const [rows, setRows]       = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);

    // Support /rentroll/{property_id} style endpoints
    const url = propertyId
      ? `${API_BASE}/${endpoint}/${propertyId}`
      : `${API_BASE}/${endpoint}`;

    fetch(url)
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(data => { setRows(data); setLoading(false); })
      .catch(err => { setError(err.message); setLoading(false); });
  }, [endpoint, propertyId]);

  if (loading) return (
    <div className="flex items-center gap-2 p-4 text-gray-500">
      <div className="w-4 h-4 border-2 border-gray-300 border-t-blue-500 rounded-full animate-spin" />
      Loading...
    </div>
  );

  if (error) return (
    <div className="p-4 rounded-lg bg-red-50 text-red-700 text-sm">
      Error fetching <code>{endpoint}</code>: {error}
    </div>
  );

  if (!rows.length) return (
    <p className="p-4 text-gray-400 italic">No records found.</p>
  );

  const columns = Object.keys(rows[0]);

  return (
    <div>
      {title && (
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-gray-800">{title}</h2>
          <span className="text-sm text-gray-400">{rows.length} rows</span>
        </div>
      )}

      <div className="overflow-x-auto rounded-lg border border-gray-200 shadow-sm">
        <table className="min-w-full divide-y divide-gray-200 text-sm">

          <thead className="bg-gray-50">
            <tr>
              {columns.map(col => (
                <th
                  key={col}
                  className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap"
                >
                  {formatHeader(col)}
                </th>
              ))}
            </tr>
          </thead>

          <tbody className="divide-y divide-gray-100 bg-white">
            {rows.map((row, i) => (
              <tr key={i} className="hover:bg-gray-50 transition-colors">
                {columns.map(col => (
                  <td key={col} className="px-4 py-2.5 text-gray-700 whitespace-nowrap">
                    {renderCell(col, row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>

        </table>
      </div>
    </div>
  );
}