import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { RefreshCw, Activity, TrendingUp } from 'lucide-react';

const formatCurrency = (val) => {
  try {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val || 0);
  } catch (e) {
    return `$${Number(val || 0).toFixed(2)}`;
  }
};

export default function Dashboard({ financials, periodStats }) {
  const [topPicks, setTopPicks] = useState([]);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
      const res = await api.get('/api/ncaam/top-picks', { params: { date: today, days: 1, limit_games: 25 } });
      const picksObj = res.data?.picks || {};
      const arr = Object.keys(picksObj).map((eid) => ({ event_id: eid, ...(picksObj[eid] || {}) })).filter(x => x.rec);
      setTopPicks(arr);
    } catch (e) {
      setTopPicks([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const dk = (financials?.breakdown || []).find(x => x.provider === 'DraftKings');
  const fd = (financials?.breakdown || []).find(x => x.provider === 'FanDuel');
  const totalInPlay = (financials?.breakdown || [])
    .filter(p => p.provider === 'DraftKings' || p.provider === 'FanDuel')
    .reduce((s, p) => s + (p.in_play || 0), 0);

  const p7 = periodStats?.['7d'];
  const p30 = periodStats?.['30d'];

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div>
          <h1 className="text-2xl font-black text-white">Dashboard</h1>
          <p className="text-slate-400 text-sm mt-1">Balances, performance, and today’s picks.</p>
        </div>
        <button
          onClick={load}
          className="ml-auto px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-bold flex items-center gap-2"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} /> Refresh picks
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
          <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black">Total In Play</div>
          <div className="mt-1 text-2xl font-black text-white">{formatCurrency(totalInPlay)}</div>
          <div className="mt-2 text-xs text-slate-500">DK {formatCurrency(dk?.in_play || 0)} • FD {formatCurrency(fd?.in_play || 0)}</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
          <div className="flex items-center gap-2 text-[10px] uppercase tracking-widest text-slate-500 font-black">
            <TrendingUp size={14} /> Last 7 days
          </div>
          <div className={`mt-1 text-2xl font-black ${((p7?.roi || 0) >= 0) ? 'text-green-400' : 'text-red-400'}`}>{(p7?.roi ?? 0).toFixed(1)}% ROI</div>
          <div className="mt-2 text-xs text-slate-500">{p7?.wins ?? 0}W-{p7?.losses ?? 0}L • {p7?.total_bets ?? 0} bets</div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-4">
          <div className="flex items-center gap-2 text-[10px] uppercase tracking-widest text-slate-500 font-black">
            <Activity size={14} /> Last 30 days
          </div>
          <div className={`mt-1 text-2xl font-black ${((p30?.roi || 0) >= 0) ? 'text-green-400' : 'text-red-400'}`}>{(p30?.roi ?? 0).toFixed(1)}% ROI</div>
          <div className="mt-2 text-xs text-slate-500">{p30?.wins ?? 0}W-{p30?.losses ?? 0}L • {p30?.total_bets ?? 0} bets</div>
        </div>
      </div>

      <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-bold text-slate-200 uppercase tracking-wider">Today’s top picks (NCAAM)</h2>
          <div className="text-xs text-slate-500">server-side generated</div>
        </div>

        {topPicks.length === 0 ? (
          <div className="mt-3 text-slate-500 text-sm">No picks available yet.</div>
        ) : (
          <div className="mt-3 overflow-x-auto border border-slate-800 rounded-xl">
            <table className="min-w-full text-left text-sm">
              <thead className="bg-slate-900/60 border-b border-slate-800">
                <tr className="text-[10px] uppercase tracking-wider text-slate-500">
                  <th className="py-2 px-3">Event</th>
                  <th className="py-2 px-3">Pick</th>
                  <th className="py-2 px-3">EV%</th>
                  <th className="py-2 px-3">Conf</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {topPicks.slice(0, 12).map((x) => (
                  <tr key={x.event_id} className="hover:bg-slate-800/30">
                    <td className="py-2 px-3 text-slate-400 font-mono text-xs">{x.event_id}</td>
                    <td className="py-2 px-3 text-white font-bold">{x.rec?.selection}</td>
                    <td className="py-2 px-3 text-green-300 font-mono font-bold">{x.rec?.edge}</td>
                    <td className="py-2 px-3 text-slate-300">{x.rec?.confidence}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
