import React, { useMemo, useState } from 'react';
import { DollarSign } from 'lucide-react';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from 'recharts';

export default function Bankroll({ financials, bets, formatCurrency }) {
  const [expandedBook, setExpandedBook] = useState(null);

  const normalizedFinancials = financials || {};

  if (!normalizedFinancials?.breakdown) {
    return (
      <div className="space-y-6">
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <h1 className="text-2xl font-black text-white">Bankroll</h1>
          <p className="text-slate-400 text-sm mt-1">Balances, deposits/withdrawals, and reconciliation live here.</p>
        </div>
        <div className="text-slate-500">No bankroll data available.</div>
      </div>
    );
  }

  const rows = normalizedFinancials.breakdown || [];
  const provTop = (name) => rows.find((r) => r.provider === name && r.account_id === null);
  const provAcc = (name, acc) => rows.find((r) => r.provider === name && String(r.account_id || '') === String(acc));

  const providers = ['DraftKings', 'FanDuel', 'Other'];
  const fmt = (x) => (formatCurrency ? formatCurrency(Number(x || 0)) : String(Number(x || 0)));
  const fmtPct = (x) => `${Number(x || 0).toFixed(1)}%`;

  const settledBets = useMemo(() => {
    const xs = Array.isArray(bets) ? bets : [];
    return xs.filter((b) => {
      const st = String(b?.status || '').toUpperCase().trim();
      return st === 'WON' || st === 'LOST' || st === 'PUSH';
    });
  }, [bets]);

  const performanceBySport = useMemo(() => {
    const agg = {};
    for (const b of settledBets) {
      const k = String(b?.sport || 'Unknown');
      agg[k] = agg[k] || { key: k, wins: 0, losses: 0, pushes: 0, wager: 0, profit: 0 };
      const st = String(b?.status || '').toUpperCase().trim();
      if (st === 'WON') agg[k].wins += 1;
      else if (st === 'LOST') agg[k].losses += 1;
      else if (st === 'PUSH') agg[k].pushes += 1;
      agg[k].wager += Number(b?.wager || 0) || 0;
      agg[k].profit += Number(b?.profit || 0) || 0;
    }

    return Object.values(agg)
      .map((r) => {
        const decided = r.wins + r.losses;
        const winPct = decided ? (r.wins / decided) * 100 : 0;
        const roiPct = r.wager ? (r.profit / r.wager) * 100 : 0;
        return {
          key: r.key,
          winPct: Number(winPct.toFixed(1)),
          roiPct: Number(roiPct.toFixed(1)),
          n: decided,
        };
      })
      .filter((r) => r.n > 0)
      .sort((a, b) => b.roiPct - a.roiPct)
      .slice(0, 10);
  }, [settledBets]);

  const performanceByBetType = useMemo(() => {
    const agg = {};
    for (const b of settledBets) {
      const k = String(b?.bet_type || 'Unknown');
      agg[k] = agg[k] || { key: k, wins: 0, losses: 0, pushes: 0, wager: 0, profit: 0 };
      const st = String(b?.status || '').toUpperCase().trim();
      if (st === 'WON') agg[k].wins += 1;
      else if (st === 'LOST') agg[k].losses += 1;
      else if (st === 'PUSH') agg[k].pushes += 1;
      agg[k].wager += Number(b?.wager || 0) || 0;
      agg[k].profit += Number(b?.profit || 0) || 0;
    }

    return Object.values(agg)
      .map((r) => {
        const decided = r.wins + r.losses;
        const winPct = decided ? (r.wins / decided) * 100 : 0;
        const roiPct = r.wager ? (r.profit / r.wager) * 100 : 0;
        return {
          key: r.key,
          winPct: Number(winPct.toFixed(1)),
          roiPct: Number(roiPct.toFixed(1)),
          n: decided,
        };
      })
      .filter((r) => r.n > 0)
      .sort((a, b) => b.roiPct - a.roiPct)
      .slice(0, 10);
  }, [settledBets]);

  const topRows = providers
    .map((p) => {
      if (p === 'Other') {
        return { p: 'Other', top: provTop('Other'), primary: provAcc('Other', 'Main'), secondary: provAcc('Other', 'User2') };
      }
      return { p, top: provTop(p), primary: provAcc(p, 'Main'), secondary: provAcc(p, 'User2') };
    })
    .filter((x) => x.top);

  return (
    <div className="space-y-6">
      <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
        <div className="p-6 border-b border-slate-800">
          <div className="flex items-center justify-between">
            <h3 className="text-xl font-bold flex items-center gap-2">
              <DollarSign className="text-green-400" /> Sportsbook Financials
            </h3>
            <div className="text-[11px] text-slate-500">Statement view (click a row to expand)</div>
          </div>
          <div className="mt-2 text-[10px] text-gray-600 uppercase tracking-widest opacity-70">
            Baseline balances from latest snapshots; ledger includes deposits/withdrawals and open/pending wagers.
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="bg-slate-950/50">
              <tr className="text-slate-400 border-b border-slate-800">
                <th className="py-3 px-4">Sportsbook</th>
                <th className="py-3 px-4 text-right">Current</th>
                <th className="py-3 px-4 text-right">Deposits</th>
                <th className="py-3 px-4 text-right">Withdrawals</th>
                <th className="py-3 px-4 text-right">Realized P/L</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {topRows.map(({ p, top, primary, secondary }) => {
                const cur = Number((top.ledger_in_play ?? top.in_play) || 0);
                const isOpen = expandedBook === p;

                return (
                  <React.Fragment key={p}>
                    <tr
                      className={`cursor-pointer hover:bg-slate-800/30 ${isOpen ? 'bg-slate-800/20' : ''}`}
                      onClick={() => setExpandedBook(isOpen ? null : p)}
                    >
                      <td className="py-3 px-4 font-black text-slate-100">
                        <div className="flex items-center gap-2">
                          <span className="text-slate-400">{isOpen ? '▾' : '▸'}</span>
                          <span>{p}</span>
                        </div>
                      </td>
                      <td className={`py-3 px-4 text-right font-black ${cur >= 0 ? 'text-green-300' : 'text-red-300'}`}>{fmt(cur)}</td>
                      <td className="py-3 px-4 text-right text-slate-300 font-mono">{fmt(top.deposited)}</td>
                      <td className="py-3 px-4 text-right text-slate-300 font-mono">{fmt(top.withdrawn)}</td>
                      <td className={`py-3 px-4 text-right font-mono font-bold ${Number(top.net_profit || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{fmt(top.net_profit)}</td>
                    </tr>

                    {isOpen ? (
                      <tr className="bg-slate-950/20">
                        <td colSpan={6} className="px-4 py-4">
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div className="border border-slate-800 rounded-lg p-4 bg-slate-950/20">
                              <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black">Primary</div>
                              <div className="mt-1 text-xl font-black text-white">{fmt(Number((primary?.ledger_in_play ?? primary?.in_play) || 0))}</div>
                              <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-slate-400">
                                <div>Dep <span className="font-mono text-slate-200">{fmt(Number(primary?.deposited || 0))}</span></div>
                                <div>Wdr <span className="font-mono text-slate-200">{fmt(Number(primary?.withdrawn || 0))}</span></div>
                              </div>
                            </div>
                            <div className="border border-slate-800 rounded-lg p-4 bg-slate-950/20">
                              <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black">Secondary</div>
                              <div className="mt-1 text-xl font-black text-white">{fmt(Number((secondary?.ledger_in_play ?? secondary?.in_play) || 0))}</div>
                              <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] text-slate-400">
                                <div>Dep <span className="font-mono text-slate-200">{fmt(Number(secondary?.deposited || 0))}</span></div>
                                <div>Wdr <span className="font-mono text-slate-200">{fmt(Number(secondary?.withdrawn || 0))}</span></div>
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    ) : null}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-end justify-between gap-3 mb-2">
            <div className="text-sm font-black text-slate-100 uppercase tracking-wider">Win% + ROI by Sport</div>
            <div className="text-[11px] text-slate-500">Settled bets only</div>
          </div>
          <div className="h-[240px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={performanceBySport} margin={{ top: 10, right: 18, left: 6, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="key" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8 }}
                  labelStyle={{ color: '#e2e8f0' }}
                  formatter={(v, name, props) => {
                    if (name === 'Win%') return [`${Number(v).toFixed(1)}%`, 'Win%'];
                    if (name === 'ROI%') return [`${Number(v).toFixed(1)}%`, 'ROI%'];
                    return [v, name];
                  }}
                />
                <Legend wrapperStyle={{ color: '#94a3b8', fontSize: 11 }} />
                <Bar dataKey="winPct" name="Win%" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                <Bar dataKey="roiPct" name="ROI%" fill="#34d399" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5">
          <div className="flex items-end justify-between gap-3 mb-2">
            <div className="text-sm font-black text-slate-100 uppercase tracking-wider">Win% + ROI by Bet Type</div>
            <div className="text-[11px] text-slate-500">Settled bets only</div>
          </div>
          <div className="h-[240px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={performanceByBetType} margin={{ top: 10, right: 18, left: 6, bottom: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="key" tick={{ fill: '#94a3b8', fontSize: 11 }} interval={0} angle={-15} textAnchor="end" height={50} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ background: '#0b1220', border: '1px solid #334155', borderRadius: 8 }}
                  labelStyle={{ color: '#e2e8f0' }}
                  formatter={(v, name) => {
                    if (name === 'Win%') return [`${Number(v).toFixed(1)}%`, 'Win%'];
                    if (name === 'ROI%') return [`${Number(v).toFixed(1)}%`, 'ROI%'];
                    return [v, name];
                  }}
                />
                <Legend wrapperStyle={{ color: '#94a3b8', fontSize: 11 }} />
                <Bar dataKey="winPct" name="Win%" fill="#60a5fa" radius={[6, 6, 0, 0]} />
                <Bar dataKey="roiPct" name="ROI%" fill="#34d399" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
