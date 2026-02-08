import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { RefreshCw } from 'lucide-react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

const fmtPct = (x) => (x === null || x === undefined) ? '—' : `${x >= 0 ? '+' : ''}${Number(x).toFixed(1)}%`;
const fmtOdds = (p) => (p === null || p === undefined) ? '—' : `${Number(p) > 0 ? '+' : ''}${p}`;
const confLabel = (c0) => {
  const n = Number(c0 || 0);
  if (n >= 80) return 'High';
  if (n >= 50) return 'Medium';
  return 'Low';
};
const fmtNum = (x, d = 2) => (x === null || x === undefined) ? '—' : Number(x).toFixed(d);

export default function PerformanceReportNCAAM() {
  const [data, setData] = useState(null);
  const [series, setSeries] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      const [rep, ser] = await Promise.all([
        api.get('/api/ncaam/performance-report', { params: { days: 30 } }),
        api.get('/api/ncaam/model-performance/series', { params: { days: 30, min_ev_per_unit: 0.02 } }),
      ]);
      setData(rep.data);
      setSeries(ser.data);
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
                      <div className="text-white font-mono font-bold">{fmtNum((w.avg_ev_per_unit ?? 0) * 100, 1)}%</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">Avg CLV (pts)</div>
                      <div className="text-white font-mono font-bold">{w.avg_clv_points ?? '—'}</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">+CLV Rate</div>
                      <div className="text-white font-mono font-bold">{w.pos_clv_rate ?? '—'}</div>
                    </div>
                    <div>
                      <div className="text-slate-400 text-xs">Decided</div>
                      <div className="text-white font-mono font-bold">{w.decided ?? 0}</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          <div className="mt-4 text-xs text-slate-500">
            Coverage (last 30d): {data?.coverage?.decided ?? 0} decided • {data?.coverage?.pending ?? 0} pending
            {Number(data?.coverage?.pending_but_final_available || 0) > 0 ? (
              <span className="text-yellow-300"> • {data.coverage.pending_but_final_available} pending but finals exist (grading lag)</span>
            ) : null}
          </div>

          {(data?.confidence_breakdown || []).length > 0 && (
            <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
              {data.confidence_breakdown.map((b) => (
                <div key={b.bucket} className="bg-slate-950/30 border border-slate-800 rounded-xl p-4">
                  <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black">{b.bucket} confidence</div>
                  <div className="mt-1 text-white font-black text-lg">{b.record.won}-{b.record.lost}-{b.record.push}</div>
                  <div className="mt-1 text-slate-400 text-xs">Win% (W/L only): <span className="text-slate-200 font-bold">{b.win_rate}%</span> • N={b.decided}</div>
                </div>
              ))}
            </div>
          )}

          <div className="mt-6">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-bold text-slate-200 uppercase tracking-wider">Model performance (recommended bets only)</h3>
              <div className="text-xs text-slate-500">EV gate: ≥2% EV/u • cumulative units</div>
            </div>

            <div className="bg-slate-800/40 border border-slate-800 rounded-xl p-4">
              {(!series || !(series.series || []).length) ? (
                <div className="text-slate-500 text-sm">{loading ? 'Loading…' : 'No graded recommended bets in range yet.'}</div>
              ) : (
                <div className="h-[220px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={series.series} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                      <XAxis dataKey="day" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                      <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                      <Tooltip
                        contentStyle={{ background: '#0b1220', border: '1px solid #334155', color: '#e2e8f0' }}
                        labelStyle={{ color: '#94a3b8' }}
                        formatter={(v, name) => [v, name === 'cum_units' ? 'Cum Units' : name]}
                      />
                      <Line type="monotone" dataKey="cum_units" stroke="#22c55e" strokeWidth={2} dot={false} name="All" />
                      <Line type="monotone" dataKey="cum_units_high" stroke="#60a5fa" strokeWidth={2} dot={false} name="High" />
                      <Line type="monotone" dataKey="cum_units_medium" stroke="#fbbf24" strokeWidth={2} dot={false} name="Medium" />
                      <Line type="monotone" dataKey="cum_units_low" stroke="#a78bfa" strokeWidth={2} dot={false} name="Low" />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              )}
            </div>

            <div className="mt-6 flex items-center justify-between mb-2">
              <h3 className="text-sm font-bold text-slate-200 uppercase tracking-wider">Daily recommended bets (graded)</h3>
              <div className="text-xs text-slate-500">(table; shows outcome/ROI when finals ingested)</div>
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
                    <th className="py-2 px-3">CLV</th>
                    <th className="py-2 px-3">ROI/u</th>
                    <th className="py-2 px-3">Outcome</th>
                    <th className="py-2 px-3">Final</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {rows.length === 0 ? (
                    <tr><td className="py-4 px-3 text-slate-500" colSpan={10}>No picks in range.</td></tr>
                  ) : rows.map((p) => {
                    const evPct = Math.round((p.ev_per_unit || 0) * 1000) / 10;
                    const out = (p.outcome || '').toUpperCase();
                    const outCls = out === 'WON' ? 'text-green-400' : out === 'LOST' ? 'text-red-400' : out === 'PUSH' ? 'text-yellow-400' : 'text-slate-400';
                    return (
                      <tr key={`${p.day}-${p.event_id}-${p.selection}`} className="hover:bg-slate-800/30">
                        <td className="py-2 px-3 text-slate-400 font-mono text-xs">{(() => {
                          try {
                            // p.day is YYYY-MM-DD (ET). Show as MM/DD/YYYY.
                            const [yy, mm, dd] = String(p.day || '').split('-');
                            if (yy && mm && dd) return `${mm}/${dd}/${yy}`;
                          } catch (e) {}
                          return p.day;
                        })()}</td>
                        <td className="py-2 px-3 text-slate-200">{p.matchup}</td>
                        <td className="py-2 px-3 text-white font-bold">{p.selection}</td>
                        <td className="py-2 px-3 text-slate-300 font-mono">{fmtOdds(p.price)}</td>
                        <td className="py-2 px-3 text-green-300 font-mono font-bold">{evPct}%</td>
                        <td className="py-2 px-3 text-slate-200 font-bold">{confLabel(p.confidence_0_100)}</td>
                        <td className="py-2 px-3 text-slate-300 font-mono">{p.clv_points === null || p.clv_points === undefined ? '—' : fmtNum(p.clv_points, 2)}</td>
                        <td className="py-2 px-3 text-slate-300 font-mono">{p.roi_per_unit === null || p.roi_per_unit === undefined ? '—' : fmtNum(p.roi_per_unit, 2)}</td>
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
