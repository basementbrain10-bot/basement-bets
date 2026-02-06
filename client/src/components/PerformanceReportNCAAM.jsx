import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { RefreshCw } from 'lucide-react';

const fmtPct = (x) => (x === null || x === undefined) ? '—' : `${x >= 0 ? '+' : ''}${Number(x).toFixed(1)}%`;
const fmtOdds = (p) => (p === null || p === undefined) ? '—' : `${Number(p) > 0 ? '+' : ''}${p}`;

export default function PerformanceReportNCAAM() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      const res = await api.get('/api/ncaam/performance-report', { params: { days: 30 } });
      setData(res.data);
    } catch (e) {
      setErr(e?.response?.data?.detail || e?.message || 'Failed to load report');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const rows = (() => {
    const out = [];
    (data?.daily_recommended_bets || []).forEach((d) => {
      (d.picks || []).forEach((p) => out.push({ day: d.day, ...p }));
    });
    return out;
  })();

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
      <div className="flex items-center gap-3">
        <div>
          <h2 className="text-lg font-bold text-white">NCAAM Picks & Performance</h2>
          <div className="text-xs text-slate-500">Generated: {data?.generated_at || '—'}</div>
        </div>
        <button
          onClick={load}
          className="ml-auto px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-bold flex items-center gap-2"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {err && (
        <div className="mt-4 p-3 rounded-lg bg-red-900/20 border border-red-800 text-red-200 text-sm">{err}</div>
      )}

      {!data ? (
        <div className="mt-6 text-slate-500 text-sm">{loading ? 'Loading…' : 'No data yet.'}</div>
      ) : (
        <>
          <div className="mt-5 grid grid-cols-1 md:grid-cols-2 gap-4">
            {(['7d','30d']).map((k) => {
              const w = data?.windows?.[k];
              if (!w) return null;
              const rec = w.record || {};
              return (
                <div key={k} className="bg-slate-800/40 border border-slate-700 rounded-xl p-4">
                  <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-2">Last {w.days} days</div>
                  <div className="flex flex-wrap gap-4 text-sm">
                    <div>
                      <div className="text-slate-400 text-xs">Record</div>
                      <div className="text-white font-bold">{rec.won}-{rec.lost}-{rec.push}</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">Win Rate</div>
                      <div className="text-white font-bold">{w.win_rate}%</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">ROI</div>
                      <div className={`font-bold ${w.roi_pct >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmtPct(w.roi_pct)}</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">Avg EV/u</div>
                      <div className="text-white font-mono font-bold">{w.avg_ev_per_unit}</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">Avg CLV</div>
                      <div className="text-white font-mono font-bold">{w.avg_clv_points ?? '—'}</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          <div className="mt-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-bold text-slate-200 uppercase tracking-wider">Daily recommended bets</h3>
              <div className="text-xs text-slate-500">(table; includes finals when graded)</div>
            </div>

            <div className="overflow-x-auto border border-slate-800 rounded-xl">
              <table className="min-w-full text-left text-sm">
                <thead className="bg-slate-900/60 border-b border-slate-800">
                  <tr className="text-[10px] uppercase tracking-wider text-slate-500">
                    <th className="py-2 px-3">Day</th>
                    <th className="py-2 px-3">Matchup</th>
                    <th className="py-2 px-3">Recommended bet</th>
                    <th className="py-2 px-3">Odds</th>
                    <th className="py-2 px-3">EV%</th>
                    <th className="py-2 px-3">Conf</th>
                    <th className="py-2 px-3">Outcome</th>
                    <th className="py-2 px-3">Final</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {rows.length === 0 ? (
                    <tr><td className="py-4 px-3 text-slate-500" colSpan={8}>No picks in range.</td></tr>
                  ) : rows.map((p) => {
                    const evPct = Math.round((p.ev_per_unit || 0) * 1000) / 10;
                    const out = (p.outcome || '').toUpperCase();
                    const outCls = out === 'WON' ? 'text-green-400' : out === 'LOST' ? 'text-red-400' : out === 'PUSH' ? 'text-yellow-400' : 'text-slate-400';
                    return (
                      <tr key={`${p.day}-${p.event_id}-${p.selection}`} className="hover:bg-slate-800/30">
                        <td className="py-2 px-3 text-slate-400 font-mono text-xs">{p.day}</td>
                        <td className="py-2 px-3 text-slate-200">{p.matchup}</td>
                        <td className="py-2 px-3 text-white font-bold">{p.selection}</td>
                        <td className="py-2 px-3 text-slate-300 font-mono">{fmtOdds(p.price)}</td>
                        <td className="py-2 px-3 text-green-300 font-mono font-bold">{evPct}%</td>
                        <td className="py-2 px-3 text-slate-400">{p.confidence_0_100}</td>
                        <td className={`py-2 px-3 font-bold ${outCls}`}>{out || 'PENDING'}</td>
                        <td className="py-2 px-3 text-slate-300 font-mono">{p.final_score || '—'}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
