import React, { useEffect, useMemo, useState } from 'react';
import api from '../api/axios';
import { RefreshCw, BarChart3 } from 'lucide-react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, Legend } from 'recharts';
import ModelPerformanceAnalytics from '../components/ModelPerformanceAnalytics';

const etDay = (dt) => {
  if (!dt) return null;
  try {
    const d = new Date(dt);
    return d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  } catch (e) {
    return null;
  }
};

const isGraded = (x) => {
  const s = String(x || '').toUpperCase();
  return s === 'WON' || s === 'WIN' || s === 'LOST' || s === 'LOSS' || s === 'PUSH';
};

export default function Picks() {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      // Primary source: recommended model history (all leagues)
      const res = await api.get('/api/research/history');
      const rows = res.data || [];
      if (Array.isArray(rows) && rows.length > 0) {
        setHistory(rows);
      } else {
        // Fallback (UI-only): NCAAM history endpoint
        const n = await api.get('/api/ncaam/history', { params: { limit: 2000 } }).catch(() => ({ data: [] }));
        setHistory(n.data || []);
      }
    } catch (e) {
      // Match other pages: if auth fails, prompt for Basement password.
      if (e?.response?.status === 403) {
        const pass = prompt('Authentication failed. Please enter the Basement Password:');
        if (pass) {
          try { localStorage.setItem('basement_password', pass); } catch (err) { }
          window.location.reload();
          return;
        }
      }
      setErr(e?.response?.data?.detail || e?.response?.data?.message || e?.message || 'Failed to load model performance');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const yesterdayEt = useMemo(() => {
    const d = new Date();
    // Convert to ET day string by forcing timezone formatting after subtracting 1 day
    d.setDate(d.getDate() - 1);
    return d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  }, []);

  const graded = useMemo(() => {
    return (history || []).filter((h) => isGraded(h.graded_result || h.outcome || h.result));
  }, [history]);

  const gradedYesterday = useMemo(() => {
    return graded.filter((h) => etDay(h.analyzed_at) === yesterdayEt);
  }, [graded, yesterdayEt]);

  const yRecord = useMemo(() => {
    const res = (h) => String(h.graded_result || h.outcome || h.result || '').toUpperCase();
    const w = gradedYesterday.filter((h) => res(h) === 'WON' || res(h) === 'WIN').length;
    const l = gradedYesterday.filter((h) => res(h) === 'LOST' || res(h) === 'LOSS').length;
    const p = gradedYesterday.filter((h) => res(h) === 'PUSH').length;
    const decided = w + l;
    const winRate = decided > 0 ? (w / decided) * 100 : 0;
    return { w, l, p, decided, winRate };
  }, [gradedYesterday]);

  const dailyPerformance = useMemo(() => {
    // Daily net units based on graded recommended picks.
    // Convention: +1u for win, -1u for loss, 0u for push.
    const res = (h) => String(h.graded_result || h.outcome || h.result || '').toUpperCase();
    const unit = (h) => {
      const r = res(h);
      if (r === 'WON' || r === 'WIN') return 1;
      if (r === 'LOST' || r === 'LOSS') return -1;
      return 0;
    };

    const byDay = {};
    graded.forEach((h) => {
      const day = etDay(h.analyzed_at) || '—';
      byDay[day] = byDay[day] || { day, units: 0, wins: 0, losses: 0, pushes: 0, picks: 0 };
      const r = res(h);
      byDay[day].picks += 1;
      byDay[day].units += unit(h);
      if (r === 'WON' || r === 'WIN') byDay[day].wins += 1;
      else if (r === 'LOST' || r === 'LOSS') byDay[day].losses += 1;
      else if (r === 'PUSH') byDay[day].pushes += 1;
    });

    return Object.values(byDay)
      .filter((x) => x.day && x.day !== '—')
      .sort((a, b) => String(a.day).localeCompare(String(b.day)))
      .slice(-30);
  }, [graded, yesterdayEt]);

  const confidenceBreakdown = useMemo(() => {
    const normRes = (h) => String(h.graded_result || h.outcome || h.result || '').toUpperCase();
    const isW = (r) => r === 'WON' || r === 'WIN';
    const isL = (r) => r === 'LOST' || r === 'LOSS';

    const bucket = (h) => {
      const c = Number(h?.confidence_0_100 ?? h?.confidence ?? 0);
      if (c >= 80) return 'High';
      if (c >= 50) return 'Medium';
      return 'Low';
    };

    const base = { High: [], Medium: [], Low: [] };
    graded.forEach((h) => {
      base[bucket(h)].push(h);
    });

    const calc = (rows) => {
      const w = rows.filter((h) => isW(normRes(h))).length;
      const l = rows.filter((h) => isL(normRes(h))).length;
      const p = rows.filter((h) => normRes(h) === 'PUSH').length;
      const decided = w + l;
      const winRate = decided > 0 ? (w / decided) * 100 : null;
      // Same simplifying assumption used elsewhere in UI: $10 stake, -110 style
      const roi = rows.length > 0 ? ((w * 9.09 - l * 10) / (rows.length * 10)) * 100 : null;
      return { w, l, p, decided, winRate, roi, n: rows.length };
    };

    const out = ['High', 'Medium', 'Low'].map((k) => {
      const s = calc(base[k]);
      return {
        bucket: k,
        picks: s.n,
        wins: s.w,
        losses: s.l,
        pushes: s.p,
        winRate: s.winRate === null ? null : Number(s.winRate.toFixed(1)),
        roi: s.roi === null ? null : Number(s.roi.toFixed(1)),
      };
    }).filter((x) => x.picks > 0);

    return out;
  }, [graded]);

  const edgeBandChart = useMemo(() => {
    // EV/u decimal bands
    const bands = [
      { lo: 0.0, hi: 0.05, label: '0–5%' },
      { lo: 0.05, hi: 0.1, label: '5–10%' },
      { lo: 0.1, hi: 0.15, label: '10–15%' },
      { lo: 0.15, hi: 0.2, label: '15–20%' },
      { lo: 0.2, hi: 0.25, label: '20–25%' },
      { lo: 0.25, hi: 0.3, label: '25–30%' },
      { lo: 0.3, hi: null, label: '30%+' },
    ];

    const res = (h) => String(h.graded_result || h.outcome || h.result || '').toUpperCase();
    const ev = (h) => {
      const n = Number(h?.ev_per_unit ?? h?.ev);
      return Number.isFinite(n) ? n : null;
    };

    return bands
      .map((b) => {
        const rows = graded.filter((h) => {
          const e = ev(h);
          if (!Number.isFinite(e)) return false;
          if (b.hi == null) return e >= b.lo;
          return e >= b.lo && e < b.hi;
        });
        const w = rows.filter((h) => res(h) === 'WON' || res(h) === 'WIN').length;
        const l = rows.filter((h) => res(h) === 'LOST' || res(h) === 'LOSS').length;
        const decided = w + l;
        const winRate = decided > 0 ? (w / decided) * 100 : 0;
        return {
          band: b.label,
          picks: rows.length,
          wins: w,
          losses: l,
          winRate: Number(winRate.toFixed(1)),
        };
      })
      .filter((x) => x.picks > 0);
  }, [graded]);

  return (
    <div className="space-y-6">
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-black text-white">Model Performance</h1>
          <p className="text-slate-400 text-sm mt-1">All recommended picks (all leagues) • windows based on recommendation date (ET).</p>
        </div>
        <button
          onClick={load}
          className="px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-bold flex items-center gap-2"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
          Refresh
        </button>
      </div>

      {err && <div className="p-3 rounded-lg bg-red-900/20 border border-red-800 text-red-200 text-sm">{err}</div>}

      {!loading && !err && (!history || history.length === 0) && (
        <div className="p-4 rounded-lg bg-slate-900/40 border border-slate-800 text-slate-400 text-sm">
          No model-performance history returned yet. If you expect data here, try Refresh. If it still shows empty, it usually means the backend isn’t returning any stored recommended picks for your user.
        </div>
      )}

      {/* Yesterday graded results */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-2">
          <BarChart3 size={18} className="text-emerald-300" />
          <div className="text-sm font-black text-slate-100 uppercase tracking-wider">Yesterday (graded)</div>
          <div className="ml-auto text-xs text-slate-500">{yesterdayEt}</div>
        </div>
        <div className="flex flex-wrap gap-6 text-sm">
          <div>
            <div className="text-slate-400 text-xs">Record</div>
            <div className="text-white font-black">{yRecord.w}-{yRecord.l}-{yRecord.p}</div>
          </div>
          <div>
            <div className="text-slate-400 text-xs">Win rate</div>
            <div className="text-white font-black">{yRecord.decided ? `${yRecord.winRate.toFixed(1)}%` : '—'}</div>
          </div>
          <div>
            <div className="text-slate-400 text-xs">Graded picks</div>
            <div className="text-white font-black">{gradedYesterday.length}</div>
          </div>
        </div>
        {gradedYesterday.length === 0 && (
          <div className="mt-3 text-xs text-slate-500">No graded recommended picks found for yesterday yet.</div>
        )}
      </div>

      {/* Recommended bet performance bar chart (daily units) */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="text-sm font-black text-slate-100 uppercase tracking-wider mb-2">Recommended bet performance (last 30 days)</div>
        <div className="text-[11px] text-slate-500 mb-3">Daily net units (+1 win, -1 loss, 0 push), based on graded recommended picks.</div>
        <div className="h-[260px] overflow-x-auto">
          <div className="min-w-[360px] h-full">
            <ResponsiveContainer width="100%" height="100%">
            <BarChart data={dailyPerformance} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="day" tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8 }}
                labelStyle={{ color: '#e2e8f0' }}
                formatter={(v, name, props) => {
                  if (name === 'units') return [Number(v).toFixed(0), 'Net Units'];
                  return [v, name];
                }}
              />
              <Legend />
              <Bar dataKey="units" name="Net Units" fill="#60a5fa" radius={[6, 6, 0, 0]} />
            </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Edge band chart */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="text-sm font-black text-slate-100 uppercase tracking-wider mb-2">Performance by EV% band (win rate)</div>
        <div className="h-[260px] overflow-x-auto">
          <div className="min-w-[360px] h-full">
            <ResponsiveContainer width="100%" height="100%">
            <BarChart data={edgeBandChart} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="band" tick={{ fill: '#94a3b8', fontSize: 12 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 12 }} domain={[0, 100]} />
              <Tooltip
                contentStyle={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8 }}
                labelStyle={{ color: '#e2e8f0' }}
              />
              <Legend />
              <Bar dataKey="winRate" name="Win%" fill="#34d399" radius={[6, 6, 0, 0]} />
            </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="mt-2 text-[11px] text-slate-500">Bands are based on EV/u (decimal). Example: 0.08 = 8%.</div>
      </div>

      {/* Confidence breakdown (win% + ROI) */}
      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="text-sm font-black text-slate-100 uppercase tracking-wider mb-2">Performance by confidence</div>
        <div className="text-[11px] text-slate-500 mb-4">Buckets use confidence_0_100: High ≥80, Medium 50–79, Low &lt;50.</div>

        {(!confidenceBreakdown || confidenceBreakdown.length === 0) ? (
          <div className="text-xs text-slate-500">No graded picks with confidence yet.</div>
        ) : (
          <>
            {/* Recap tiles */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
              {confidenceBreakdown.map((b) => (
                <div key={b.bucket} className="bg-slate-950/30 border border-slate-800 rounded-xl p-4">
                  <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black">{b.bucket}</div>
                  <div className="mt-1 text-white font-black text-lg">{b.wins}-{b.losses}{b.pushes ? `-${b.pushes}` : ''}</div>
                  <div className="mt-1 text-slate-400 text-xs">Win% (W/L): <span className="text-slate-200 font-bold">{b.winRate === null ? '—' : `${b.winRate}%`}</span> • ROI: <span className={`font-bold ${Number(b.roi || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{b.roi === null ? '—' : `${b.roi}%`}</span> • N={b.picks}</div>
                </div>
              ))}
            </div>

            {/* Bar chart */}
            <div className="h-[260px] overflow-x-auto">
              <div className="min-w-[360px] h-full">
                <ResponsiveContainer width="100%" height="100%">
                <BarChart data={confidenceBreakdown} margin={{ top: 10, right: 20, left: 0, bottom: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                  <XAxis dataKey="bucket" tick={{ fill: '#94a3b8', fontSize: 12 }} />
                  <YAxis yAxisId="left" tick={{ fill: '#94a3b8', fontSize: 12 }} domain={[0, 100]} />
                  <YAxis yAxisId="right" orientation="right" tick={{ fill: '#94a3b8', fontSize: 12 }} />
                  <Tooltip
                    contentStyle={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8 }}
                    labelStyle={{ color: '#e2e8f0' }}
                    formatter={(v, name) => {
                      if (name === 'winRate') return [`${Number(v).toFixed(1)}%`, 'Win%'];
                      if (name === 'roi') return [`${Number(v).toFixed(1)}%`, 'ROI'];
                      return [v, name];
                    }}
                  />
                  <Legend />
                  <Bar yAxisId="left" dataKey="winRate" name="Win%" fill="#34d399" radius={[6, 6, 0, 0]} />
                  <Bar yAxisId="right" dataKey="roi" name="ROI%" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Existing analytics (kept) */}
      <ModelPerformanceAnalytics history={history || []} />
    </div>
  );
}

