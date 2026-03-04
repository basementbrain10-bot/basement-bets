import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { Zap, Target, Home, TrendingUp, Percent, RefreshCw } from 'lucide-react';

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

const fmtTime = (ts) => {
  if (!ts) return '';
  try {
    return new Date(ts).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });
  } catch { return ''; }
};

const TABS = [
  { key: 'high_confidence', label: 'Highest Confidence', icon: Zap, color: 'amber' },
  { key: 'payout_band', label: 'Value Matchups', icon: Target, color: 'purple' },
  { key: 'home_fav', label: 'Home Favorites', icon: Home, color: 'orange' },
];

const ACCENT = {
  amber: { text: 'text-amber-400', bg: 'bg-amber-500/10', border: 'border-amber-500/20', tab: 'border-amber-400 text-amber-300' },
  purple: { text: 'text-purple-400', bg: 'bg-purple-500/10', border: 'border-purple-500/20', tab: 'border-purple-400 text-purple-300' },
  orange: { text: 'text-orange-400', bg: 'bg-orange-500/10', border: 'border-orange-500/20', tab: 'border-orange-400 text-orange-300' },
};

function ComboCard({ c, color = 'amber' }) {
  const legs = c?.legs || [];
  const ac = ACCENT[color] || ACCENT.amber;

  return (
    <div className="relative overflow-hidden p-4 rounded-xl border border-slate-700/60 bg-gradient-to-br from-slate-900 to-slate-950 shadow-lg flex flex-col gap-3 transition hover:border-slate-500/50">
      {/* Header row */}
      <div className="flex items-center justify-between border-b border-slate-800/60 pb-3">
        <div className="flex items-baseline gap-2">
          <span className="text-2xl font-black text-white">{fmtOdds(c?.american_odds)}</span>
          <span className="text-xs font-bold text-slate-500">Parlay Odds</span>
        </div>
        <div className="flex items-center gap-2">
          <div className={`flex items-center gap-1 px-2 py-1 rounded-md border ${ac.bg} ${ac.border}`}>
            <TrendingUp size={13} className={ac.text} />
            <span className={`text-xs font-bold ${ac.text}`}>+{fmtPct(c?.ev)} EV</span>
          </div>
          <div className="flex items-center gap-1 bg-blue-500/10 px-2 py-1 rounded-md border border-blue-500/20">
            <Percent size={13} className="text-blue-400" />
            <span className="text-xs font-bold text-blue-400">{fmtPct(c?.p_win)} Win</span>
          </div>
        </div>
      </div>

      {/* Legs */}
      <div className="flex flex-col gap-3">
        {legs.map((leg, li) => (
          <div key={li} className="flex items-start justify-between gap-3">
            <div className="flex flex-col min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-[10px] text-slate-500 font-bold uppercase">{fmtTime(leg.start_time)}</span>
                <span className="text-xs text-slate-400 font-medium">{leg.matchup}</span>
                {color === 'orange' && leg.is_home_pick && (
                  <span className="text-[9px] font-bold text-orange-400 bg-orange-400/10 border border-orange-400/20 px-1.5 py-0.5 rounded-full uppercase tracking-wider">Home Fav</span>
                )}
              </div>
              <span className="text-sm font-bold text-slate-200 leading-tight">{leg.team_pick}</span>
            </div>
            <span className="text-sm font-mono font-bold text-slate-500 shrink-0">{fmtOdds(leg.price)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function ParlayRecommendations() {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [allData, setAllData] = useState(null);
  const [homeFavData, setHomeFavData] = useState(null);
  const [activeTab, setActiveTab] = useState('high_confidence');

  const load = async () => {
    setLoading(true);
    setErr(null);
    try {
      const [main, homeFav] = await Promise.all([
        api.get('/api/ncaam/parlays/today', { params: { min_ev_per_unit: 0.02, parlay_odds_lo: -120, parlay_odds_hi: 300 } }),
        api.get('/api/ncaam/parlays/today', { params: { strategy: 'home_fav', parlay_odds_lo: -180, parlay_odds_hi: 250, min_ev_per_unit: 0.02 } }).catch(() => ({ data: null })),
      ]);
      setAllData(main.data);
      setHomeFavData(homeFav.data);
    } catch (e) {
      setErr(e?.response?.data?.detail || e?.message || 'Failed to load parlays');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const getCombos = (tabKey) => {
    if (tabKey === 'high_confidence') return (allData?.high_confidence || []).slice(0, 3);
    if (tabKey === 'payout_band') return (allData?.payout_band || []).slice(0, 3);
    if (tabKey === 'home_fav') return (homeFavData?.high_confidence || []).slice(0, 3);
    return [];
  };

  const getColor = (tabKey) => {
    const tab = TABS.find(t => t.key === tabKey);
    return tab?.color || 'amber';
  };

  const combos = getCombos(activeTab);
  const color = getColor(activeTab);
  const activeTabDef = TABS.find(t => t.key === activeTab);

  return (
    <div className="mb-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div>
          <div className="text-sm font-black text-slate-100 uppercase tracking-wider">2-Leg ML Parlays</div>
          <div className="text-[11px] text-slate-500">Sourced from model recommendations · today only</div>
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-2 text-xs font-semibold px-3 py-1.5 rounded-lg border border-slate-700 text-slate-200 bg-slate-800/40 hover:bg-slate-700/60 disabled:opacity-50 transition"
        >
          <RefreshCw size={13} className={loading ? 'animate-spin' : ''} />
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      {err && <div className="text-xs text-red-400 bg-red-900/20 border border-red-900/50 p-3 rounded-xl mb-3">{err}</div>}

      {/* Tab bar */}
      <div className="flex gap-1 mb-4 bg-slate-900/60 border border-slate-800/70 rounded-xl p-1">
        {TABS.map(({ key, label, icon: Icon, color: tabColor }) => {
          const isActive = activeTab === key;
          const ac = ACCENT[tabColor];
          return (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`flex-1 flex items-center justify-center gap-1.5 px-2 py-2 rounded-lg text-xs font-bold transition-all
                ${isActive
                  ? `bg-slate-800 border ${ac.tab} shadow-sm`
                  : 'text-slate-500 hover:text-slate-300 border border-transparent'
                }`}
            >
              <Icon size={13} className={isActive ? ac.text : 'text-slate-600'} />
              <span className="hidden sm:inline">{label}</span>
              <span className="sm:hidden">{label.split(' ')[0]}</span>
            </button>
          );
        })}
      </div>

      {/* Active tab content */}
      {!err && (
        <>
          {combos.length === 0 ? (
            <div className="text-sm text-slate-500 bg-slate-950/50 rounded-xl p-6 text-center border border-slate-800/50 font-medium">
              No {activeTabDef?.label?.toLowerCase()} combos found today.
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
              {combos.map((c, i) => (
                <ComboCard key={i} c={c} color={color} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}
