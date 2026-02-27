import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { Zap, Target, TrendingUp, Percent, RefreshCw } from 'lucide-react';

const fmtPct = (x) => {
  const n = Number(x);
  if (!Number.isFinite(n)) return '—';
  return `${(n * 100).toFixed(1)}%`;
};

const fmtOdds = (a) => {
  const n = Number(a);
  if (!Number.isFinite(n) || n === 0) return '—';
  return n > 0 ? `+${n}` : `${n}`;
};

export default function ParlayRecommendations() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [data, setData] = useState(null);

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      const res = await api.get('/api/ncaam/parlays/today', { params: { min_ev_per_unit: 0.02, parlay_odds_lo: -120, parlay_odds_hi: 300 } });
      setData(res.data);
    } catch (e) {
      setErr(e?.response?.data?.detail || e?.message || 'Failed to load parlays');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const renderRow = (c, idx) => {
    const legs = c?.legs || [];
    const a = legs[0] || {};
    const b = legs[1] || {};
    return (
      <div key={idx} className="relative overflow-hidden p-4 rounded-xl border border-slate-700/60 bg-gradient-to-br from-slate-900 to-slate-950 shadow-lg flex flex-col gap-3 transition hover:border-slate-500/50">
        {/* Top bar: Odds and EV */}
        <div className="flex items-center justify-between border-b border-slate-800/60 pb-3">
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-black text-white">{fmtOdds(c?.american_odds)}</span>
            <span className="text-xs font-bold text-slate-500">Total Odds</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-1 bg-green-500/10 px-2 py-1 rounded-md border border-green-500/20">
              <TrendingUp size={14} className="text-green-400" />
              <span className="text-xs font-bold text-green-400">+{fmtPct(c?.ev)} EV</span>
            </div>
            <div className="flex items-center gap-1 bg-blue-500/10 px-2 py-1 rounded-md border border-blue-500/20">
              <Percent size={14} className="text-blue-400" />
              <span className="text-xs font-bold text-blue-400">{fmtPct(c?.p_win)} Win</span>
            </div>
          </div>
        </div>

        {/* Legs container */}
        <div className="flex flex-col gap-3 w-full">
          {/* Leg 1 */}
          <div className="flex items-center justify-between">
            <div className="flex flex-col">
              <span className="text-xs text-slate-400 font-medium truncate max-w-[200px] sm:max-w-[240px]">{a.matchup}</span>
              <span className="text-sm font-bold text-slate-200">{a.team_pick}</span>
            </div>
            <span className="text-sm font-mono font-bold text-slate-500 shrink-0">{fmtOdds(a.price)}</span>
          </div>

          {/* Leg 2 */}
          <div className="flex items-center justify-between">
            <div className="flex flex-col">
              <span className="text-xs text-slate-400 font-medium truncate max-w-[200px] sm:max-w-[240px]">{b.matchup}</span>
              <span className="text-sm font-bold text-slate-200">{b.team_pick}</span>
            </div>
            <span className="text-sm font-mono font-bold text-slate-500 shrink-0">{fmtOdds(b.price)}</span>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="text-sm font-black text-slate-100 uppercase tracking-wider">2-leg ML parlays (today)</div>
          <div className="text-[11px] text-slate-500">Sourced from model recommendations. Sorted by edge.</div>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-2 text-xs font-semibold px-3 py-1.5 rounded-lg border border-slate-700 text-slate-200 bg-slate-800/40 hover:bg-slate-700/60 disabled:opacity-50 transition"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          {loading ? 'Analyzing…' : 'Refresh'}
        </button>
      </div>

      {err && <div className="text-xs text-red-400 bg-red-900/20 border border-red-900/50 p-3 rounded-xl mb-4">{err}</div>}

      {!err && data && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Zap className="text-amber-400" size={18} fill="currentColor" fillOpacity={0.2} />
              <div className="text-sm font-black text-slate-100 uppercase tracking-wider">Highest Confidence</div>
            </div>
            <div className="space-y-3">
              {(data?.high_confidence || []).length === 0 ? (
                <div className="text-sm text-slate-500 bg-slate-950/50 rounded-xl p-6 text-center border border-slate-800/50 font-medium">No high-confidence combos found today.</div>
              ) : (
                (data.high_confidence || []).slice(0, 5).map(renderRow)
              )}
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800/80 rounded-2xl p-5 shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Target className="text-purple-400" size={18} />
              <div className="text-sm font-black text-slate-100 uppercase tracking-wider">Value Matchups (+150 or better)</div>
            </div>
            <div className="space-y-3">
              {(data?.payout_band || []).length === 0 ? (
                <div className="text-sm text-slate-500 bg-slate-950/50 rounded-xl p-6 text-center border border-slate-800/50 font-medium">No value combos found today.</div>
              ) : (
                (data.payout_band || []).slice(0, 5).map(renderRow)
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
