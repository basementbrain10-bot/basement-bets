import React, { useEffect, useState } from 'react';
import api from '../api/axios';

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
      <div key={idx} className="p-3 rounded-lg border border-slate-800 bg-slate-950/20">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <div className="text-xs font-black text-slate-100 whitespace-normal break-words leading-snug">
              {a.matchup} — <span className="text-slate-300">{a.team_pick}</span> <span className="text-slate-500">({fmtOdds(a.price)})</span>
            </div>
            <div className="text-xs font-black text-slate-100 whitespace-normal break-words leading-snug">
              {b.matchup} — <span className="text-slate-300">{b.team_pick}</span> <span className="text-slate-500">({fmtOdds(b.price)})</span>
            </div>
          </div>
          <div className="text-right shrink-0">
            <div className="text-xs font-mono font-black text-slate-200">{fmtOdds(c?.american_odds)}</div>
            <div className="text-[11px] text-slate-400 font-mono">P(win) {fmtPct(c?.p_win)}</div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="mb-6">
      <div className="flex items-center justify-between mb-2">
        <div>
          <div className="text-sm font-black text-slate-100 uppercase tracking-wider">2-leg ML parlays (today)</div>
          <div className="text-[11px] text-slate-500">Leg odds filtered to -120…+300 • sourced from model_predictions ML</div>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="text-xs font-medium px-3 py-1.5 rounded-lg border border-slate-700 text-slate-200 hover:bg-slate-800/50 disabled:opacity-50"
        >
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      {err && <div className="text-xs text-red-300 mb-2">{err}</div>}

      {!err && data && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs font-black text-slate-100 uppercase tracking-wider mb-2">Highest confidence</div>
            <div className="space-y-2">
              {(data?.high_confidence || []).length === 0 ? (
                <div className="text-xs text-slate-500">No combos found.</div>
              ) : (
                (data.high_confidence || []).slice(0, 5).map(renderRow)
              )}
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
            <div className="text-xs font-black text-slate-100 uppercase tracking-wider mb-2">Best payout band</div>
            <div className="space-y-2">
              {(data?.payout_band || []).length === 0 ? (
                <div className="text-xs text-slate-500">No combos found.</div>
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
