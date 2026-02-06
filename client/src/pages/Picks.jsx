import React from 'react';
import PerformanceReportNCAAM from '../components/PerformanceReportNCAAM';

export default function Picks() {
  return (
    <div className="space-y-6">
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <h1 className="text-2xl font-black text-white">Picks</h1>
        <p className="text-slate-400 text-sm mt-1">Daily recommended bets + recent model performance.</p>
      </div>

      <PerformanceReportNCAAM />
    </div>
  );
}
