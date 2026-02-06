import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { RefreshCw } from 'lucide-react';

const fmtPct = (x) => (x === null || x === undefined) ? '—' : `${x >= 0 ? '+' : ''}${Number(x).toFixed(1)}%`;

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

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
      <div className="flex items-center gap-3">
        <div>
          <h2 className="text-lg font-bold text-white">NCAAM Performance Report</h2>
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
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-bold text-slate-200 uppercase tracking-wider">Daily recommended bets</h3>
              <div className="text-xs text-slate-500">(last 30 days)</div>
            </div>

            <div className="mt-3 space-y-3">
              {(data.daily_recommended_bets || []).slice(0, 10).map((d) => (
                <div key={d.day} className="bg-slate-800/30 border border-slate-700 rounded-xl p-4">
                  <div className="text-xs text-slate-400 font-bold mb-2">{d.day}</div>
                  {(!d.picks || d.picks.length === 0) ? (
                    <div className="text-slate-500 text-sm">No picks.</div>
                  ) : (
                    <ul className="space-y-1 text-sm">
                      {d.picks.slice(0, 12).map((p) => (
                        <li key={p.event_id} className="flex flex-wrap gap-2 items-center">
                          <span className="text-slate-300">{p.matchup}</span>
                          <span className="text-white font-bold">— {p.selection}</span>
                          {p.price !== null && p.price !== undefined ? (
                            <span className="text-slate-400 font-mono">({p.price > 0 ? '+' : ''}{p.price})</span>
                          ) : null}
                          <span className="text-green-300 font-mono font-bold">EV {Math.round((p.ev_per_unit || 0) * 1000) / 10}%</span>
                          <span className="text-slate-500">conf {p.confidence_0_100}</span>
                          {p.outcome ? (
                            <span className={`text-xs font-bold ${String(p.outcome).toUpperCase() === 'WON' ? 'text-green-400' : String(p.outcome).toUpperCase() === 'LOST' ? 'text-red-400' : 'text-slate-400'}`}>{p.outcome}</span>
                          ) : null}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
