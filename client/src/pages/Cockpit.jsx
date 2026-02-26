import React, { useEffect, useState, useMemo } from 'react';
import api from '../api/axios';
import {
    ShieldCheck, ShieldAlert, Shield,
    TrendingUp, TrendingDown,
    Activity, BarChart3,
    AlertTriangle, CheckCircle2,
    ArrowUpRight, Info,
    RefreshCw, MousePointer2,
    LineChart as LineChartIcon
} from 'lucide-react';
import {
    ResponsiveContainer, AreaChart, Area,
    XAxis, YAxis, Tooltip,
    BarChart, Bar, Cell,
    LineChart, Line,
    CartesianGrid
} from 'recharts';

import AgentCouncilDebate from '../components/AgentCouncilDebate';

const formatCurrency = (val) => {
    try {
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val || 0);
    } catch (e) {
        return `$${Number(val || 0).toFixed(2)}`;
    }
};

const Cockpit = () => {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Data States
    const [topPicks, setTopPicks] = useState([]);
    const [health, setHealth] = useState([]);
    const [perf, setPerf] = useState(null);
    const [balanceSeries, setBalanceSeries] = useState([]);
    const [dailyRisks, setDailyRisks] = useState([]);
    const [financials, setFinancials] = useState(null);

    // UI States
    const [selectedMatchup, setSelectedMatchup] = useState(null);
    const [isSyncing, setIsSyncing] = useState(false);

    const fetchData = async () => {
        setLoading(true);
        try {
            const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

            const [tpRes, healthRes, perfRes, seriesRes, dashboardRes] = await Promise.all([
                api.get('/api/ncaam/top-picks', { params: { date: today, days: 1, limit_games: 50 } }),
                api.get('/api/data-health'),
                api.get('/api/ncaam/performance-report', { params: { days: 30 } }),
                api.get('/api/financials/inplay/series', { params: { days: 30 } }),
                api.get('/api/dashboard')
            ]);

            // Process Top 6
            const picks = Object.values(tpRes.data?.picks || {})
                .filter(p => p.is_actionable)
                .sort((a, b) => (b.rec?.ev_per_unit || 0) - (a.rec?.ev_per_unit || 0))
                .slice(0, 6);
            setTopPicks(picks);

            setHealth(healthRes.data?.items || []);
            setPerf(perfRes.data || null);
            setBalanceSeries(seriesRes.data?.series || []);
            setFinancials(dashboardRes.data?.financials || null);

            // Extract risks from Top 6
            const risks = picks.flatMap(p => p.rec?.signals?.red_flags || []).filter(r => r && r !== "");
            setDailyRisks([...new Set(risks)]); // Unique risks

        } catch (err) {
            console.error("Cockpit data fetch failed:", err);
            setError("Failed to load cockpit data. Check connection.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);

    // Helpers
    const getHealthStatus = () => {
        const items = health.filter(x => !x.source.includes('NFL'));
        if (items.some(x => x.status === 'error')) return { icon: ShieldAlert, color: 'text-red-400', label: 'Critical' };
        if (items.some(x => x.status === 'stale')) return { icon: AlertTriangle, color: 'text-amber-400', label: 'Stale' };
        return { icon: ShieldCheck, color: 'text-emerald-400', label: 'Healthy' };
    };

    const status = getHealthStatus();
    const balanceTrend = useMemo(() => {
        if (balanceSeries.length < 2) return 0;
        const last = balanceSeries[balanceSeries.length - 1]?.total_balance || 0;
        const prev = balanceSeries[balanceSeries.length - 2]?.total_balance || 0;
        return last - prev;
    }, [balanceSeries]);

    if (loading) return (
        <div className="flex flex-col items-center justify-center min-h-[400px] animate-pulse">
            <RefreshCw size={48} className="text-blue-500 animate-spin mb-4" />
            <div className="text-slate-500 font-bold uppercase tracking-widest text-xs">Calibrating Cockpit...</div>
        </div>
    );

    return (
        <div className="space-y-6 animate-in fade-in duration-500">

            {/* 1. Header: System Health & Balance Trend */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex items-center justify-between">
                    <div>
                        <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-1">System Health</div>
                        <div className={`text-xl font-black ${status.color} flex items-center gap-2`}>
                            <status.icon size={20} /> {status.label}
                        </div>
                    </div>
                    <div className="h-10 w-10 flex items-center justify-center bg-slate-950 rounded-xl border border-slate-800">
                        <Info size={16} className="text-slate-600" />
                    </div>
                </div>

                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 md:col-span-2 flex items-center justify-between">
                    <div className="flex-1">
                        <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-1">Account Balance</div>
                        <div className="text-2xl font-black text-white flex items-baseline gap-2">
                            {formatCurrency(financials?.total_balance || 0)}
                            <span className={`text-xs font-bold ${balanceTrend >= 0 ? 'text-emerald-400' : 'text-red-400'} flex items-center`}>
                                {balanceTrend >= 0 ? <TrendingUp size={12} className="mr-1" /> : <TrendingDown size={12} className="mr-1" />}
                                {formatCurrency(Math.abs(balanceTrend))}
                            </span>
                        </div>
                    </div>
                    <div className="w-32 h-12">
                        <ResponsiveContainer width="100%" height="100%">
                            <AreaChart data={balanceSeries.slice(-10)}>
                                <Area type="monotone" dataKey="total_balance" stroke={balanceTrend >= 0 ? '#10b981' : '#f43f5e'} fill={balanceTrend >= 0 ? '#10b98120' : '#f43f5e20'} strokeWidth={2} />
                            </AreaChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex items-center justify-between">
                    <div>
                        <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-1">Exposure</div>
                        <div className="text-xl font-black text-white">
                            {formatCurrency(financials?.total_in_play || 0)}
                        </div>
                    </div>
                    <Activity size={20} className="text-blue-500" />
                </div>
            </div>

            {/* 2. The Alpha Spotlight: Top 6 Recommended */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 space-y-4">
                    <div className="flex items-center justify-between px-2">
                        <h2 className="text-sm font-black text-slate-100 uppercase tracking-widest flex items-center gap-2">
                            <CheckCircle2 size={16} className="text-emerald-400" /> The Council Six (Today)
                        </h2>
                        <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">Ranked by EV</div>
                    </div>

                    <div className="grid grid-cols-1 gap-3">
                        {topPicks.length === 0 ? (
                            <div className="bg-slate-900/50 border border-slate-800 border-dashed rounded-2xl p-10 text-center">
                                <div className="text-slate-500 text-sm font-bold">No actionable Alpha detected for today's slate.</div>
                            </div>
                        ) : (
                            topPicks.map((p, idx) => (
                                <div key={idx} className="bg-slate-900 border border-slate-800 rounded-2xl p-4 hover:border-slate-700 transition group cursor-pointer" onClick={() => setSelectedMatchup(p)}>
                                    <div className="flex items-center justify-between gap-4">
                                        <div className="flex-1 min-w-0">
                                            <div className="flex items-center gap-2 mb-1">
                                                <span className="text-[10px] font-black text-slate-600">#{idx + 1}</span>
                                                <div className="text-slate-100 font-black text-sm truncate uppercase tracking-tight">
                                                    {p.event?.away_team} @ {p.event?.home_team}
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 text-xs">
                                                <span className="px-2 py-0.5 bg-blue-500/10 text-blue-400 rounded text-[10px] font-black border border-blue-500/20">
                                                    {p.rec?.selection} {p.rec?.line}
                                                </span>
                                                <span className="text-slate-500 font-bold">
                                                    EV {((p.rec?.ev_per_unit || 0) * 100).toFixed(1)}%
                                                </span>
                                            </div>
                                        </div>
                                        <div className="text-right">
                                            <div className="text-[10px] uppercase font-black text-slate-500 tracking-widest mb-1">Confidence</div>
                                            <div className={`text-lg font-black ${p.rec?.confidence >= 70 ? 'text-emerald-400' : 'text-blue-400'}`}>
                                                {Math.round(p.rec?.confidence || 0)}%
                                            </div>
                                        </div>
                                    </div>
                                    {/* Quick Council Snippet */}
                                    <div className="mt-3 pt-3 border-t border-slate-800/50 text-[11px] text-slate-400 italic line-clamp-1 group-hover:text-slate-300">
                                        <span className="font-bold text-slate-500 non-italic mr-1">Oracle:</span>
                                        {p.rec?.oracle_verdict || "Evaluating matchup dynamics..."}
                                    </div>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* 3. Tactical Awareness: Performance & Risks */}
                <div className="space-y-6">
                    {/* Performance Grids */}
                    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                        <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
                            <Activity size={12} className="text-blue-400" /> Momentum Matrix
                        </h3>
                        <div className="grid grid-cols-2 gap-4">
                            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3">
                                <div className="text-[9px] uppercase font-black text-slate-500 mb-1">Yesterday</div>
                                <div className={`text-lg font-black ${perf?.windows?.yesterday?.roi_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                                    {perf?.windows?.yesterday?.roi_pct?.toFixed(1) || '0.0'}%
                                </div>
                            </div>
                            <div className="bg-slate-950 border border-slate-800 rounded-xl p-3">
                                <div className="text-[9px] uppercase font-black text-slate-500 mb-1">30D ROI</div>
                                <div className={`text-lg font-black ${perf?.windows?.['30d']?.roi_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                                    {perf?.windows?.['30d']?.roi_pct?.toFixed(1) || '0.0'}%
                                </div>
                            </div>
                        </div>
                        <div className="pt-2">
                            <div className="text-[9px] uppercase font-black text-slate-500 mb-2">30D Equity Drive</div>
                            <div className="h-16 w-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={perf?.daily_history?.slice(-14) || []}>
                                        <Bar dataKey="net_profit">
                                            {(perf?.daily_history?.slice(-14) || []).map((entry, index) => (
                                                <Cell key={`cell-${index}`} fill={entry.net_profit >= 0 ? '#10b981' : '#f43f5e'} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </div>
                    </div>

                    {/* Daily Risks Feed */}
                    <div className="bg-slate-900 border border-slate-800 rounded-2xl p-5">
                        <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 mb-4">
                            <AlertTriangle size={12} className="text-amber-400" /> Oracle Risk Alerts
                        </h3>
                        <div className="space-y-3">
                            {dailyRisks.length === 0 ? (
                                <div className="text-[11px] text-slate-500 flex items-center gap-2">
                                    <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
                                    No major qualitative threats identified.
                                </div>
                            ) : (
                                dailyRisks.map((risk, i) => (
                                    <div key={i} className="flex gap-3 text-[11px]">
                                        <span className="text-amber-500 flex-shrink-0 mt-0.5">!</span>
                                        <span className="text-slate-300 leading-relaxed">{risk}</span>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>
                </div>
            </div>

            {/* 4. Deep Dive Modal (Full Council Discussion) */}
            {selectedMatchup && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-in fade-in duration-300" onClick={() => setSelectedMatchup(null)}>
                    <div className="bg-slate-900 border border-slate-800 rounded-3xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col shadow-2xl" onClick={e => e.stopPropagation()}>
                        <div className="p-6 border-b border-slate-800 flex items-center justify-between">
                            <div className="flex items-center gap-4">
                                <div className="h-12 w-12 bg-blue-600/10 rounded-2xl flex items-center justify-center border border-blue-500/20">
                                    <BarChart3 className="text-blue-400" />
                                </div>
                                <div>
                                    <h2 className="text-xl font-black text-white uppercase tracking-tight">
                                        {selectedMatchup.event?.away_team} @ {selectedMatchup.event?.home_team}
                                    </h2>
                                    <div className="text-xs font-bold text-slate-400 uppercase tracking-widest">
                                        Council Alpha Review
                                    </div>
                                </div>
                            </div>
                            <button onClick={() => setSelectedMatchup(null)} className="p-2 hover:bg-slate-800 rounded-xl text-slate-500 transition">
                                <ArrowUpRight className="rotate-45" />
                            </button>
                        </div>
                        <div className="flex-1 overflow-y-auto p-6 bg-slate-950/30">
                            <div className="mb-8 grid grid-cols-1 md:grid-cols-3 gap-4">
                                <div className="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
                                    <div className="text-[10px] uppercase font-black text-slate-500 tracking-widest mb-1">Proposed Pick</div>
                                    <div className="text-lg font-black text-blue-400">{selectedMatchup.rec?.selection} {selectedMatchup.rec?.line}</div>
                                </div>
                                <div className="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
                                    <div className="text-[10px] uppercase font-black text-slate-500 tracking-widest mb-1">Calculated Edge</div>
                                    <div className="text-lg font-black text-emerald-400">+{((selectedMatchup.rec?.ev_per_unit || 0) * 100).toFixed(1)}% EV</div>
                                </div>
                                <div className="bg-slate-900/50 border border-slate-800 p-4 rounded-2xl">
                                    <div className="text-[10px] uppercase font-black text-slate-500 tracking-widest mb-1">Risk Score</div>
                                    <div className={`text-lg font-black ${selectedMatchup.rec?.signals?.risk_score < 0.3 ? 'text-emerald-400' : 'text-amber-400'}`}>
                                        {(selectedMatchup.rec?.signals?.risk_score || 0).toFixed(2)}
                                    </div>
                                </div>
                            </div>

                            {/* Reusing existing AgentCouncilDebate for consistency */}
                            <div className="space-y-6">
                                {selectedMatchup.rec?.debate ? (
                                    <AgentCouncilDebate debate={selectedMatchup.rec.debate} />
                                ) : (
                                    <div className="text-slate-500 text-center py-10 font-bold italic">No detailed debate data persisted for this matchup.</div>
                                )}
                            </div>
                        </div>
                        <div className="p-6 border-t border-slate-800 bg-slate-900/80">
                            <button onClick={() => setSelectedMatchup(null)} className="w-full bg-blue-600 hover:bg-blue-500 text-white font-black py-4 rounded-2xl transition shadow-lg shadow-blue-600/20">
                                Acknowledge Alpha Capsule
                            </button>
                        </div>
                    </div>
                </div>
            )}

        </div>
    );
};

export default Cockpit;
