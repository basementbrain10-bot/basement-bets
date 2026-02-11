import React, { useState, useEffect } from 'react';
import api from './api/axios';
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell,
    ScatterChart, Scatter, ZAxis, ReferenceLine
} from 'recharts';
import { TrendingUp, TrendingDown, ArrowUpRight, ArrowDownRight, DollarSign, Activity, PieChart, BarChart2, BarChart3, Calendar, Layout, LayoutDashboard, Search, Menu, X, PlusCircle, Trash, Trash2, CheckCircle, Clock, Percent, List, FileText, Info, Settings, User, RefreshCw, AlertTriangle, AlertCircle, Filter, ChevronDown, ChevronRight, MessageSquare, BookOpen, ExternalLink, ArrowRight, Table } from 'lucide-react';

import axios from 'axios';
import BetTypeAnalysis from './components/BetTypeAnalysis';
import Research from './pages/Research';
import { PasteSlipContainer } from './components/PasteSlipContainer';
// import { StagingBanner } from './components/StagingBanner';

console.log("Basement Bets Frontend v1.6.2 Loaded at " + new Date().toISOString());

// --- Login Modal Component ---
const LoginModal = ({ onSubmit }) => {
    const [pass, setPass] = useState('');
    return (
        <div className="fixed inset-0 bg-black/80 flex items-center justify-center z-[9999]">
            <div className="bg-slate-900 border border-slate-700 p-8 rounded-xl max-w-md w-full shadow-2xl">
                <h2 className="text-2xl font-bold text-white mb-4">Authentication</h2>
                <p className="text-slate-400 mb-6">Enter the Basement Password to access this server.</p>
                <form onSubmit={(e) => { e.preventDefault(); onSubmit(pass); }}>
                    <input
                        type="password"
                        value={pass}
                        onChange={(e) => setPass(e.target.value)}
                        className="w-full bg-slate-800 border border-slate-600 text-white rounded p-3 mb-4 focus:ring-2 focus:ring-blue-500 outline-none"
                        placeholder="Password..."
                        autoFocus
                    />
                    <button
                        type="submit"
                        className="w-full bg-blue-600 hover:bg-blue-500 text-white font-bold py-3 rounded transition-colors"
                    >
                        Login
                    </button>
                </form>
            </div>
        </div>
    );
};

// Helpers
const formatCurrency = (val) => new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).filter ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val) : (typeof val === 'number' ? `$${val.toFixed(2)}` : val);

// Always show dates as MM/DD/YYYY (ET)
const formatDateMDY = (s) => {
    if (!s) return '';
    try {
        const str = String(s).trim();
        // handle YYYY-MM-DD
        const m = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (m) {
            const [, yy, mm, dd] = m;
            return `${mm}/${dd}/${yy}`;
        }
        const d = new Date(str);
        if (!isNaN(d.getTime())) {
            return d.toLocaleDateString('en-US', {
                month: '2-digit',
                day: '2-digit',
                year: 'numeric',
                timeZone: 'America/New_York'
            });
        }
    } catch (e) {}
    return String(s);
};

class ErrorBoundary extends React.Component {
    constructor(props) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error) {
        return { hasError: true, error };
    }

    componentDidCatch(error, errorInfo) {
        console.error("UI Error:", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="p-10 text-red-500 bg-slate-900 border border-red-900 m-8 rounded-xl">
                    <h2 className="text-xl font-bold mb-2">Something went wrong.</h2>
                    <pre className="text-sm bg-black p-4 rounded overflow-auto">
                        {this.state.error?.toString()}
                    </pre>
                </div>
            );
        }

        return this.props.children;
    }
}

function App() {
    const [view, setView] = useState('research'); // research | actuals
    const [stats, setStats] = useState(null);
    const [bets, setBets] = useState([]);
    const [sportBreakdown, setSportBreakdown] = useState([]);
    const [playerBreakdown, setPlayerBreakdown] = useState([]);
    const [monthlyBreakdown, setMonthlyBreakdown] = useState([]);
    const [betTypeBreakdown, setBetTypeBreakdown] = useState([]);
    const [balances, setBalances] = useState({});
    const [error, setError] = useState(null);
    const [timeSeries, setTimeSeries] = useState([]);
    const [drawdown, setDrawdown] = useState(null);
    const [financials, setFinancials] = useState({ total_in_play: 0, total_deposited: 0, total_withdrawn: 0, realized_profit: 0 });
    const [periodStats, setPeriodStats] = useState({ '7d': null, '30d': null, 'ytd': null, 'all': null });
    const [edgeBreakdown, setEdgeBreakdown] = useState([]);
    const [showAddBet, setShowAddBet] = useState(false);
    const [isSyncing, setIsSyncing] = useState(false);

    // Auth State
    const [showLogin, setShowLogin] = useState(() => {
        return !localStorage.getItem('basement_password');
    });

    const handleLogin = (pass) => {
        localStorage.setItem('basement_password', pass);
        window.location.reload();
    };

    const handleSyncResults = async () => {
        setIsSyncing(true);
        try {
            // Sync all active leagues
            const leagues = ['NFL', 'NCAAM', 'NCAAF', 'EPL'];
            for (const league of leagues) {
                await api.post(`/api/jobs/ingest_results/${league}`);
            }
            // Add reconciliation and grading
            await api.post('/api/jobs/reconcile');
            await api.post('/api/jobs/grade_predictions');

            alert("Results synced and bets settled successfully!");
            window.location.reload();
        } catch (err) {
            console.error("Sync Error", err);
            alert("Failed to sync results. Check backend logs.");
        } finally {
            setIsSyncing(false);
        }
    };

    if (showLogin) {
        return <LoginModal onSubmit={handleLogin} />;
    }

    useEffect(() => {
        // Fetch Data
        const fetchData = async () => {
            try {
                // Helper to get data or default
                const getVal = (res, defaultVal) => res.status === 'fulfilled' ? res.value.data : defaultVal;

                // Parallelize API calls
                const results = await Promise.allSettled([
                    api.get('/api/stats'),
                    api.get('/api/bets'),
                    api.get('/api/breakdown/sport'),
                    api.get('/api/breakdown/player'),
                    api.get('/api/breakdown/monthly'),
                    api.get('/api/breakdown/bet_type'),
                    api.get('/api/balances/snapshots/latest'),
                    api.get('/api/financials'),
                    api.get('/api/analytics/series'),
                    api.get('/api/analytics/drawdown'),
                    api.get('/api/breakdown/edge')
                ]);

                // Check for 403 or 500 in results to alert user
                const failed = results.find(r => r.status === 'rejected');
                if (failed) {
                    const reason = failed.reason;
                    if (reason && ((reason.response && reason.response.status === 403) || (reason.message && reason.message.includes("403")))) {
                        localStorage.removeItem('basement_password'); // Force clear storage
                        setShowLogin(true);
                    }
                    // Since we catch globally in axios for 403, this is likely 500 or Network
                    // throw failed.reason;
                }

                // Fetch Period Stats in parallel
                const currentYear = new Date().getFullYear();
                const periodResults = await Promise.allSettled([
                    api.get('/api/stats/period?days=7'),
                    api.get('/api/stats/period?days=30'),
                    api.get(`/api/stats/period?year=${currentYear}`),
                    api.get('/api/stats/period')
                ]);

                setStats(getVal(results[0], { total_bets: 0, total_profit: 0, win_rate: 0, roi: 0 }));
                setBets(getVal(results[1], []));
                setSportBreakdown(getVal(results[2], []));
                setPlayerBreakdown(getVal(results[3], []));
                setMonthlyBreakdown(getVal(results[4], []));

                // Manual Fallback for Bet Type Wins if API returns 0s
                const rawBets = getVal(results[1], []);
                const apiBetBreakdown = getVal(results[5], []);

                // Re-calculate wins from raw bets to be safe
                const calculatedBreakdown = {};
                apiBetBreakdown.forEach(b => {
                    calculatedBreakdown[b.bet_type] = { ...b };
                });

                if (rawBets.length > 0) {
                    rawBets.forEach(bet => {
                        const type = bet.bet_type || 'Unknown';
                        if (!calculatedBreakdown[type]) {
                            calculatedBreakdown[type] = { bet_type: type, bets: 0, wins: 0, profit: 0, wager: 0, roi: 0 };
                        }

                        // Force recalculation
                        // We trust total count and profit from API, but Wins might be 0 due to backend bug?
                        // Actually, let's just re-aggregate wins here.
                        if (bet.status && bet.status.toUpperCase() === 'WON') {
                            // Note: API returns aggregated wins. If we just += 1 here, we might double count if we started with API val.
                            // But since user says API returns 0 wins...
                            // Let's rely on our calc if API says 0.
                            if (calculatedBreakdown[type].wins === 0) {
                                // Just increment local counter (we need a separate tracker or assume 0 start)
                            }
                        }
                    });

                    // Better approach: Re-build breakdown completely from raw bets to guarantee accuracy
                    const freshBreakdown = {};
                    rawBets.forEach(bet => {
                        const type = bet.bet_type || 'Unknown';
                        if (!freshBreakdown[type]) {
                            freshBreakdown[type] = { bet_type: type, bets: 0, wins: 0, profit: 0, wager: 0 };
                        }
                        freshBreakdown[type].bets += 1;
                        freshBreakdown[type].profit += bet.profit;
                        freshBreakdown[type].wager += bet.wager;
                        if (bet.status && bet.status.toUpperCase() === 'WON') {
                            freshBreakdown[type].wins += 1;
                        }
                    });

                    // Convert to array and calc rates, filtering out financials
                    const finalBreakdown = Object.values(freshBreakdown)
                        .filter(item => item.bet_type !== 'Deposit' && item.bet_type !== 'Withdrawal' && item.bet_type !== 'Other')
                        .map(item => ({
                            ...item,
                            win_rate: item.bets > 0 ? (item.wins / item.bets * 100) : 0,
                            roi: item.wager > 0 ? (item.profit / item.wager * 100) : 0
                        })).sort((a, b) => b.profit - a.profit);

                    setBetTypeBreakdown(finalBreakdown);
                } else {
                    setBetTypeBreakdown(apiBetBreakdown);
                }

                // Balances now come from explicit balance snapshots (source-of-truth)
                const rawSnaps = getVal(results[6], {});
                const mapped = {};
                try {
                    Object.entries(rawSnaps || {}).forEach(([provider, snap]) => {
                        if (!provider) return;
                        mapped[provider] = {
                            balance: snap?.balance ?? 0,
                            captured_at: snap?.captured_at || snap?.capturedAt || null,
                            source: snap?.source || 'manual'
                        };
                    });
                } catch (e) {}
                setBalances(mapped);
                setFinancials(getVal(results[7], { total_in_play: 0, total_deposited: 0, total_withdrawn: 0, realized_profit: 0 }));
                setTimeSeries(getVal(results[8], []));
                setDrawdown(getVal(results[9], { max_drawdown: 0.0, current_drawdown: 0.0, peak_profit: 0.0 }));
                setEdgeBreakdown(getVal(results[10], []));

                setPeriodStats({
                    '7d': getVal(periodResults[0], { net_profit: 0, roi: 0, wins: 0, losses: 0, total_bets: 0, actual_win_rate: 0, implied_win_rate: 0 }),
                    '30d': getVal(periodResults[1], { net_profit: 0, roi: 0, wins: 0, losses: 0, total_bets: 0, actual_win_rate: 0, implied_win_rate: 0 }),
                    'ytd': getVal(periodResults[2], { net_profit: 0, roi: 0, wins: 0, losses: 0, total_bets: 0, actual_win_rate: 0, implied_win_rate: 0 }),
                    'all': getVal(periodResults[3], { net_profit: 0, roi: 0, wins: 0, losses: 0, total_bets: 0, actual_win_rate: 0, implied_win_rate: 0 })
                });

            } catch (err) {
                console.error("API Error", err);
                setError(err.message || "Failed to load dashboard data.");
            }
        };
        fetchData();
    }, []);

    if (error) return (
        <div className="flex flex-col items-center justify-center min-h-screen text-red-500 bg-slate-950 p-6 text-center">
            <AlertCircle size={48} className="mb-4" />
            <h2 className="text-2xl font-bold mb-2">Connection Error</h2>
            <p className="text-gray-400 mb-6">{error}</p>
            <p className="text-sm text-gray-500 mb-6">
                Most common cause: Database not initialized.<br />
            </p>
            <div className="flex flex-wrap gap-4 justify-center">
                <button
                    onClick={() => {
                        const pass = prompt("Enter Basement Password:");
                        if (pass) {
                            localStorage.setItem('basement_password', pass);
                            window.location.reload();
                        }
                    }}
                    className="px-6 py-2 bg-slate-100 hover:bg-white text-slate-950 rounded-lg font-bold transition"
                >
                    Update Password
                </button>
                <button
                    onClick={() => {
                        api.get('/api/admin/init-db')
                            .then(() => alert("Database Initialized!"))
                            .catch(e => alert("Error: " + (e.response?.data?.message || e.message)));
                    }}
                    className="px-6 py-2 bg-blue-600 hover:bg-blue-500 rounded-lg text-white font-bold transition"
                >
                    Initialize Database
                </button>
                <button
                    onClick={() => window.location.reload()}
                    className="px-6 py-2 bg-slate-800 hover:bg-slate-700 rounded-lg text-white font-bold transition"
                >
                    Retry
                </button>
            </div>
        </div>
    );

    // Actuals sub-tab (combined Performance + Transactions)
    const [actualsTab, setActualsTab] = useState('performance'); // performance | transactions

    if (!stats) return <div className="min-h-screen flex items-center justify-center bg-slate-950 text-white font-mono animate-pulse">Loading Basement Bets...</div>;

    return (
        <ErrorBoundary>
            {/* <StagingBanner /> */}
            <div className="min-h-screen bg-slate-950 text-white p-8 font-sans selection:bg-green-500 selection:text-black">
                <div className="max-w-7xl mx-auto">
                    {/* Header */}
                    <header className="mb-8 flex justify-between items-center">
                        <div>
                            <div>
                                <h1 className="text-3xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                                    Basement Bets
                                </h1>
                                <p className="text-gray-400">Balances, performance, and the board.</p>
                            </div>        </div>
                        <div className="flex gap-2">
                            <button
                                onClick={() => setView('research')}
                                className={`px-4 py-2 rounded-lg flex items-center gap-2 transition-all ${view === 'research' ? 'bg-purple-500 text-white font-bold shadow-[0_0_15px_rgba(168,85,247,0.4)]' : 'bg-slate-800 hover:bg-slate-700'}`}
                            >
                                <TrendingUp size={18} /> Model Recommendations
                            </button>
                            <button
                                onClick={() => setView('actuals')}
                                className={`px-4 py-2 rounded-lg flex items-center gap-2 transition-all ${view === 'actuals' ? 'bg-blue-500 text-white font-bold shadow-[0_0_15px_rgba(59,130,246,0.4)]' : 'bg-slate-800 hover:bg-slate-700'}`}
                            >
                                <LayoutDashboard size={18} /> Actuals
                            </button>

                            {view === 'actuals' && (
                                <div className="flex gap-1 ml-2 bg-slate-900/50 border border-slate-800 rounded-lg p-1">
                                    <button
                                        onClick={() => setActualsTab('performance')}
                                        className={`px-3 py-1 rounded-md text-sm font-bold ${actualsTab === 'performance' ? 'bg-slate-700 text-white' : 'text-slate-300 hover:text-white'}`}
                                    >
                                        Bet Performance
                                    </button>
                                    <button
                                        onClick={() => setActualsTab('transactions')}
                                        className={`px-3 py-1 rounded-md text-sm font-bold ${actualsTab === 'transactions' ? 'bg-slate-700 text-white' : 'text-slate-300 hover:text-white'}`}
                                    >
                                        Transactions
                                    </button>
                                </div>
                            )}

                            <button
                                onClick={handleSyncResults}
                                disabled={isSyncing}
                                className={`px-4 py-2 rounded-lg flex items-center gap-2 font-bold transition-all ${isSyncing ? 'bg-slate-800 text-gray-500 animate-pulse' : 'bg-slate-800 hover:bg-slate-700 text-blue-400'}`}
                            >
                                <RefreshCw size={18} className={isSyncing ? 'animate-spin' : ''} />
                                {isSyncing ? 'Syncing...' : 'Sync Scores'}
                            </button>
                            {/* Sync DK temporarily removed (rate limits / unreliable) */}
                            <button
                                onClick={() => setShowAddBet(true)}
                                className="px-4 py-2 bg-green-600 hover:bg-green-500 rounded-lg flex items-center gap-2 font-bold transition-all shadow-[0_0_15px_rgba(34,197,94,0.3)]"
                            >
                                <PlusCircle size={18} /> Add Bet
                            </button>
                        </div>
                    </header>

                    {/* Content */}
                    {showAddBet && (
                        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm animate-in fade-in duration-200">
                            <div className="w-full max-w-2xl relative">
                                <PasteSlipContainer
                                    onSaveSuccess={() => {
                                        setShowAddBet(false);
                                        // Refresh data
                                        window.location.reload(); // Simple refresh for now
                                    }}
                                    onClose={() => setShowAddBet(false)}
                                />
                            </div>
                        </div>
                    )}

                    {view === 'research' ? (
                        <Research onAddBet={() => setShowAddBet(true)} />
                    ) : (
                        <>
                            {actualsTab === 'transactions' ? (
                                <TransactionView bets={bets} financials={financials} />
                            ) : (
                                <PerformanceView
                                    timeSeries={timeSeries}
                                    financials={financials}
                                    periodStats={periodStats}
                                    edgeBreakdown={edgeBreakdown}
                                    bets={bets}
                                    onOpenTransactions={(prefill) => {
                                        try { localStorage.setItem('txn_prefill', JSON.stringify(prefill || {})); } catch (e) {}
                                        setView('actuals');
                                        setActualsTab('transactions');
                                    }}
                                />
                            )}
                        </>
                    )}
                </div>
            </div>
        </ErrorBoundary>
    );
}

function PerformanceView({ timeSeries, financials, periodStats, edgeBreakdown, bets, onOpenTransactions }) {
    // Performance tab is focused on actual betting performance + breakdowns.

    // Scatter / drivers controls
    const [minSegmentBets, setMinSegmentBets] = useState(5);
    const [segmentWindow, setSegmentWindow] = useState('30d'); // 30d | 90d | all
    const [scatterColorMode, setScatterColorMode] = useState('profit'); // profit | sport
    const [scatterBubbleMode, setScatterBubbleMode] = useState('bets'); // bets | wager

    const settledBets = React.useMemo(() => {
        const xs = (bets || []);
        return xs.filter(b => {
            const prov = (b.provider || '').toLowerCase();
            if (!(prov.includes('fanduel') || prov.includes('draftkings'))) return false;
            const st = (b.status || '').toUpperCase();
            if (!['WON', 'LOST', 'PUSH'].includes(st)) return false;
            if (b.category && String(b.category).toLowerCase() === 'transaction') return false;
            // Guard: ignore rows that look like deposits/withdrawals in bet table
            const bt = (b.bet_type || '').toLowerCase();
            if (bt === 'deposit' || bt === 'withdrawal') return false;
            return true;
        });
    }, [bets]);

    const parseBetDate = (b) => {
        const s = b?.sort_date || b?.date;
        if (!s) return null;
        const d = new Date(String(s));
        return isNaN(d.getTime()) ? null : d;
    };

    const segmentAgg = React.useMemo(() => {
        const now = new Date();
        const msDay = 24 * 60 * 60 * 1000;
        const cutoff = segmentWindow === '30d' ? new Date(now.getTime() - 30 * msDay)
            : segmentWindow === '90d' ? new Date(now.getTime() - 90 * msDay)
            : null;

        const inWindow = (b) => {
            if (!cutoff) return true;
            const d = parseBetDate(b);
            if (!d) return false;
            return d >= cutoff;
        };

        const add = (map, b) => {
            const sport = (b.sport || 'Unknown').toUpperCase();
            const bt = (b.bet_type || 'Unknown');
            const key = `${sport}|||${bt}`;
            if (!map[key]) map[key] = { sport, bet_type: bt, bets: 0, wins: 0, losses: 0, push: 0, wager: 0, profit: 0, odds_sum: 0, odds_n: 0 };
            const r = map[key];
            const st = (b.status || '').toUpperCase();
            r.bets += 1;
            if (st === 'WON') r.wins += 1;
            else if (st === 'LOST') r.losses += 1;
            else r.push += 1;
            r.wager += Number(b.wager || 0);
            r.profit += Number(b.profit || 0);

            const o = (b.odds === null || b.odds === undefined || b.odds === '') ? null : Number(b.odds);
            if (o !== null && Number.isFinite(o)) {
                r.odds_sum += o;
                r.odds_n += 1;
            }
        };

        const all = {};
        const w = {};
        settledBets.filter(inWindow).forEach(b => add(all, b));

        // driver deltas: last 30d vs prior 30d (always computed off settled bets)
        const last30Cutoff = new Date(now.getTime() - 30 * msDay);
        const prev30Cutoff = new Date(now.getTime() - 60 * msDay);
        const inLast30 = (b) => {
            const d = parseBetDate(b);
            return d && d >= last30Cutoff;
        };
        const inPrev30 = (b) => {
            const d = parseBetDate(b);
            return d && d >= prev30Cutoff && d < last30Cutoff;
        };
        const last30 = {};
        const prev30 = {};
        settledBets.filter(inLast30).forEach(b => add(last30, b));
        settledBets.filter(inPrev30).forEach(b => add(prev30, b));

        const toRows = (map) => Object.values(map).map(r => {
            const decided = r.wins + r.losses;
            const winp = decided > 0 ? (r.wins / decided * 100) : 0;
            const roi = r.wager > 0 ? (r.profit / r.wager * 100) : 0;
            const avgStake = r.bets > 0 ? (r.wager / r.bets) : 0;
            const profitPerBet = r.bets > 0 ? (r.profit / r.bets) : 0;
            const avgOdds = r.odds_n > 0 ? (r.odds_sum / r.odds_n) : null;
            return {
                ...r,
                actual_win_rate: Number(winp.toFixed(1)),
                roi: Number(roi.toFixed(1)),
                profit: Number(r.profit.toFixed(2)),
                wager: Number(r.wager.toFixed(2)),
                avg_stake: Number(avgStake.toFixed(2)),
                profit_per_bet: Number(profitPerBet.toFixed(2)),
                avg_odds: (avgOdds === null) ? null : Number(avgOdds.toFixed(0)),
            };
        });

        const rows = toRows(all).filter(r => r.bets >= minSegmentBets);
        const rowsLast30 = toRows(last30);
        const rowsPrev30 = toRows(prev30);

        const lookup = (rows) => {
            const m = {};
            rows.forEach(r => { m[`${r.sport}|||${r.bet_type}`] = r; });
            return m;
        };

        const m30 = lookup(rowsLast30);
        const mp30 = lookup(rowsPrev30);

        const driving = rows.map(r => {
            const key = `${r.sport}|||${r.bet_type}`;
            const a = m30[key];
            const p = mp30[key];
            const roi30 = a ? a.roi : null;
            const roiPrev = p ? p.roi : null;
            const profit30 = a ? a.profit : null;
            const profitPrev = p ? p.profit : null;
            const roiDelta = (roi30 === null || roiPrev === null) ? null : Number((roi30 - roiPrev).toFixed(1));
            return { ...r, roi_30d: roi30, roi_prev30d: roiPrev, roi_delta: roiDelta, profit_30d: profit30, profit_prev30d: profitPrev, bets_30d: a ? a.bets : 0 };
        });

        return {
            rows,
            driving
        };
    }, [settledBets, minSegmentBets, segmentWindow]);

    // Charts-only view (no daily picks feed here).
    if (!timeSeries || timeSeries.length === 0) {
        return (
            <div className="bg-slate-900 border border-slate-800 p-10 rounded-xl text-center">
                <p className="text-gray-400">No performance data available yet. Settle some bets to see your equity curve!</p>
            </div>
        );
    }

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-bottom-4 duration-500">
            {/* Financial Overview tiles removed for Performance tab (focus on betting performance) */}

            {/* Sportsbook Balance Summary Tiles */}
            {financials?.breakdown && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                    {financials.breakdown
                        .filter(prov => prov.provider === 'DraftKings' || prov.provider === 'FanDuel')
                        .map((prov) => (
                            <div key={prov.provider} className={`bg-slate-900 border rounded-xl p-5 ${prov.provider === 'DraftKings' ? 'border-orange-600/30' : 'border-blue-600/30'}`}>
                                <div className="flex items-center justify-between mb-3">
                                    <span className={`text-sm font-bold uppercase tracking-wider ${prov.provider === 'DraftKings' ? 'text-orange-400' : 'text-blue-400'}`}>
                                        {prov.provider}
                                    </span>
                                    <DollarSign className={`w-5 h-5 ${prov.provider === 'DraftKings' ? 'text-orange-400' : 'text-blue-400'}`} />
                                </div>
                                <div className="text-3xl font-bold text-white mb-1">
                                    {formatCurrency(prov.in_play || 0)}
                                </div>
                                <div className="text-xs text-gray-400">Current Balance</div>
                            </div>
                        ))}
                    {/* Total In Play Tile (Calculated) */}
                    {(() => {
                        const calculatedTotal = financials.breakdown
                            .filter(prov => prov.provider === 'DraftKings' || prov.provider === 'FanDuel')
                            .reduce((sum, p) => sum + (p.in_play || 0), 0);

                        return (
                            <div className="bg-slate-900 border border-green-600/30 rounded-xl p-5">
                                <div className="flex items-center justify-between mb-3">
                                    <span className="text-sm font-bold uppercase tracking-wider text-green-400">Total In Play</span>
                                    <Activity className="w-5 h-5 text-green-400" />
                                </div>
                                <div className="text-3xl font-bold text-white mb-1">
                                    {formatCurrency(calculatedTotal)}
                                </div>
                                <div className="text-xs text-gray-400">All Sportsbooks</div>
                            </div>
                        );
                    })()}
                </div>
            )}

            {/* Audit Message */}
            <div className="text-[10px] text-gray-600 text-center mb-8 uppercase tracking-widest opacity-50">
                Data Integrity Audit: Totals calculated from individual sportsbook balances.
            </div>

            {/* Provider Breakdown Table */}
            {
                financials?.breakdown && (
                    <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl mb-8 p-6">
                        <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                            <DollarSign className="text-green-400" /> Sportsbook Financials
                        </h3>
                        <table className="w-full text-left text-sm">
                            <thead>
                                <tr className="text-gray-400 border-b border-gray-700">
                                    <th className="pb-2">Sportsbook</th>
                                    <th className="pb-2 text-right">In Play</th>
                                    <th className="pb-2 text-right">Total Deposited</th>
                                    <th className="pb-2 text-right">Total Withdrawn</th>
                                    <th className="pb-2 text-right">Realized Profit</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-gray-800">
                                {financials.breakdown.map((prov) => (
                                    <tr key={prov.provider} className="hover:bg-gray-800/30">
                                        <td className="py-3 font-bold text-white">{prov.provider}</td>
                                        <td className={`py-3 text-right font-bold ${(prov.in_play || 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {formatCurrency(prov.in_play || 0)}
                                        </td>
                                        <td className="py-3 text-right text-gray-400">{formatCurrency(prov.deposited)}</td>
                                        <td className="py-3 text-right text-gray-400">{formatCurrency(prov.withdrawn)}</td>
                                        <td className={`py-3 text-right font-bold ${prov.net_profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {formatCurrency(prov.net_profit)}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )
            }

            {/* Drawdown tiles removed (keep Performance tab focused on betting performance + curves) */}

            {/* Betting performance windows (settled bets) */}
            <div className="bg-slate-900 border border-slate-800 p-6 rounded-xl backdrop-blur-sm">
                <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                    <TrendingUp className="text-blue-400" /> Betting Performance
                </h3>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {(['7d', '30d', 'ytd', 'all']).map((k) => {
                        const w = periodStats?.[k];
                        if (!w) return null;
                        const profit = Number(w.net_profit || 0);
                        const roi = Number(w.roi || 0);
                        return (
                            <div key={k} className="bg-slate-800/40 border border-slate-700 rounded-xl p-4">
                                <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-2">
                                    {k === '7d' ? 'Last 7 days' : k === '30d' ? 'Last 30 days' : k === 'ytd' ? 'YTD' : 'All-time'}
                                </div>
                                <div className="grid grid-cols-2 gap-3 text-sm">
                                    <div>
                                        <div className="text-slate-400 text-xs">Net P/L</div>
                                        <div className={`font-black text-lg ${profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>{formatCurrency(profit)}</div>
                                    </div>
                                    <div>
                                        <div className="text-slate-400 text-xs">ROI</div>
                                        <div className={`font-black text-lg ${roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>{roi.toFixed(1)}%</div>
                                    </div>
                                    <div>
                                        <div className="text-slate-400 text-xs">Record</div>
                                        <div className="text-white font-bold">{w.wins}-{w.losses}</div>
                                    </div>
                                    <div>
                                        <div className="text-slate-400 text-xs">Win%</div>
                                        <div className="text-white font-bold">{Number(w.actual_win_rate || 0).toFixed(1)}%</div>
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* Scatter: Win% vs ROI by sport + bet type */}
            <div className="bg-slate-900 border border-slate-800 p-6 rounded-xl backdrop-blur-sm">
                <div className="flex items-center justify-between gap-4 mb-4">
                    <h3 className="text-xl font-bold flex items-center gap-2">
                        <BarChart3 className="text-blue-400" /> Win% vs ROI (by sport + bet type)
                    </h3>
                    <div className="text-xs text-slate-500">Click a dot to filter Transactions</div>
                </div>

                <div className="flex flex-wrap items-center gap-3 mb-3">
                    <div className="text-xs text-slate-500">Window</div>
                    <select
                        className="bg-slate-950/40 border border-slate-700 rounded px-2 py-1 text-xs text-slate-200"
                        value={segmentWindow}
                        onChange={(e) => setSegmentWindow(e.target.value)}
                    >
                        <option value="30d">Last 30 days</option>
                        <option value="90d">Last 90 days</option>
                        <option value="all">All-time</option>
                    </select>

                    <div className="ml-2 text-xs text-slate-500">Color</div>
                    <select
                        className="bg-slate-950/40 border border-slate-700 rounded px-2 py-1 text-xs text-slate-200"
                        value={scatterColorMode}
                        onChange={(e) => setScatterColorMode(e.target.value)}
                    >
                        <option value="profit">Profit sign</option>
                        <option value="sport">Sport</option>
                    </select>

                    <div className="ml-2 text-xs text-slate-500">Bubble</div>
                    <select
                        className="bg-slate-950/40 border border-slate-700 rounded px-2 py-1 text-xs text-slate-200"
                        value={scatterBubbleMode}
                        onChange={(e) => setScatterBubbleMode(e.target.value)}
                    >
                        <option value="bets"># Bets</option>
                        <option value="wager">$ Wager</option>
                    </select>

                    <div className="ml-2 text-xs text-slate-500">Min N</div>
                    <input
                        type="number"
                        min={1}
                        max={999}
                        value={minSegmentBets}
                        onChange={(e) => setMinSegmentBets(Math.max(1, parseInt(e.target.value || '1', 10)))}
                        className="w-20 bg-slate-950/40 border border-slate-700 rounded px-2 py-1 text-xs text-slate-200"
                    />
                    <button
                        type="button"
                        onClick={() => setMinSegmentBets(10)}
                        className={`px-2 py-1 text-xs rounded border ${minSegmentBets >= 10 ? 'bg-blue-600/20 text-blue-300 border-blue-500/30' : 'bg-slate-900/40 text-slate-300 border-slate-700'}`}
                    >
                        N≥10
                    </button>
                </div>

                {(segmentAgg.rows || []).length === 0 ? (
                    <div className="text-slate-500 text-sm">No segments meet the filter yet (try lowering min bets).</div>
                ) : (
                    <div className="h-[440px] bg-slate-800/20 rounded-xl border border-slate-800/50 p-4">
                        <ResponsiveContainer width="100%" height="100%">
                            <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                                <XAxis
                                    type="number"
                                    dataKey="roi"
                                    name="ROI"
                                    unit="%"
                                    stroke="#94a3b8"
                                    fontSize={10}
                                    domain={['auto', 'auto']}
                                    label={{ value: 'ROI (%)', position: 'bottom', fill: '#64748b', fontSize: 10 }}
                                    tickFormatter={(val) => `${val}%`}
                                />
                                <YAxis
                                    type="number"
                                    dataKey="actual_win_rate"
                                    name="Win Rate"
                                    unit="%"
                                    stroke="#94a3b8"
                                    fontSize={10}
                                    domain={[0, 100]}
                                    label={{ value: 'Win% (settled)', angle: -90, position: 'left', fill: '#64748b', fontSize: 10 }}
                                />
                                <ZAxis type="number" dataKey={scatterBubbleMode === 'wager' ? 'wager' : 'bets'} range={[60, 360]} name="Volume" />
                                <Tooltip
                                    cursor={{ strokeDasharray: '3 3' }}
                                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b', borderRadius: '8px' }}
                                    itemStyle={{ color: '#fff' }}
                                    content={({ active, payload }) => {
                                        if (active && payload && payload.length) {
                                            const d = payload[0].payload;
                                            return (
                                                <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-xl">
                                                    <p className="font-bold text-blue-400 mb-1">{d.sport} - {d.bet_type}</p>
                                                    <div className="grid grid-cols-2 gap-x-4 text-[10px]">
                                                        <span className="text-slate-400">Bets:</span> <span className="text-white text-right">{d.bets}</span>
                                                        <span className="text-slate-400">Record:</span> <span className="text-white text-right">{d.wins}-{d.losses}{d.push ? `-${d.push}` : ''}</span>
                                                        <span className="text-slate-400">Net P/L:</span> <span className={d.profit >= 0 ? 'text-green-400 text-right' : 'text-red-400 text-right'}>{formatCurrency(d.profit)}</span>
                                                        <span className="text-slate-400">Wager:</span> <span className="text-white text-right">{formatCurrency(d.wager)}</span>
                                                        <span className="text-slate-400">ROI:</span> <span className="text-white text-right">{d.roi}%</span>
                                                        <span className="text-slate-400">Win%:</span> <span className="text-white text-right">{d.actual_win_rate}%</span>
                                                    </div>
                                                </div>
                                            );
                                        }
                                        return null;
                                    }}
                                />
                                <ReferenceLine x={0} stroke="#475569" strokeWidth={1} />
                                <ReferenceLine y={50} stroke="#1f2937" strokeWidth={1} strokeDasharray="4 4" />

                                <Scatter
                                    name="Segments"
                                    data={segmentAgg.rows}
                                    onClick={(d) => {
                                        const payload = d?.payload || d;
                                        const sport = payload?.sport;
                                        const betType = payload?.bet_type;
                                        if (sport && betType && typeof onOpenTransactions === 'function') {
                                            onOpenTransactions({ sport, type: betType });
                                        }
                                    }}
                                >
                                    {(segmentAgg.rows || []).map((entry, index) => {
                                        const sportColors = {
                                            NFL: '#60a5fa',
                                            NBA: '#a78bfa',
                                            NCAAM: '#f97316',
                                            NCAAF: '#fbbf24',
                                            MLB: '#22c55e',
                                            NHL: '#38bdf8',
                                            UNKNOWN: '#94a3b8'
                                        };
                                        const fill = scatterColorMode === 'sport'
                                            ? (sportColors[String(entry.sport || 'UNKNOWN').toUpperCase()] || '#94a3b8')
                                            : (entry.profit >= 0 ? '#10b981' : '#ef4444');
                                        const stroke = fill;
                                        return (
                                            <Cell
                                                key={`cell-${index}`}
                                                fill={fill}
                                                fillOpacity={0.55}
                                                stroke={stroke}
                                            />
                                        );
                                    })}
                                </Scatter>
                            </ScatterChart>
                        </ResponsiveContainer>
                    </div>
                )}

                <div className="text-[10px] text-slate-500 text-center mt-2 italic">
                    Breakeven line at ROI=0%. Dotted line at Win%=50% (context only).
                </div>

                {/* What's driving results */}
                <div className="mt-6">
                    <div className="flex items-center justify-between mb-2">
                        <h4 className="text-sm font-black text-slate-200 uppercase tracking-widest">What’s driving results (last 30d vs prior 30d)</h4>
                        <div className="text-xs text-slate-500">Sorted by last 30d ROI (then 30d volume)</div>
                    </div>

                    {/* Top winners / losers */}
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
                        {(() => {
                            const rows = (segmentAgg.driving || []).slice().filter(r => (r.bets_30d || 0) >= Math.max(3, Math.min(10, minSegmentBets)));
                            const winners = rows.slice().sort((a, b) => Number(b.profit_30d || 0) - Number(a.profit_30d || 0)).slice(0, 10);
                            const losers = rows.slice().sort((a, b) => Number(a.profit_30d || 0) - Number(b.profit_30d || 0)).slice(0, 10);
                            const Card = ({ title, data }) => (
                                <div className="bg-slate-800/20 border border-slate-800 rounded-xl p-4">
                                    <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-2">{title}</div>
                                    <div className="space-y-2">
                                        {data.length === 0 ? (
                                            <div className="text-xs text-slate-500">No segments meet the min-N.</div>
                                        ) : data.map((r, i) => (
                                            <div key={i} className="flex items-center justify-between text-sm">
                                                <div className="text-slate-200 font-bold truncate pr-3">{r.sport} — <span className="text-slate-400 font-normal">{r.bet_type}</span></div>
                                                <div className={`font-mono font-bold ${Number(r.profit_30d || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{formatCurrency(Number(r.profit_30d || 0))}</div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            );
                            return (
                                <>
                                    <Card title="Top winners (30d net P/L)" data={winners} />
                                    <Card title="Top losers (30d net P/L)" data={losers} />
                                </>
                            );
                        })()}
                    </div>

                    {/* Biggest changes */}
                    <div className="bg-slate-800/20 border border-slate-800 rounded-xl p-4 mb-4">
                        <div className="text-[10px] uppercase tracking-widest text-slate-500 font-black mb-2">Biggest changes (ROI Δ, min N)</div>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                            {(() => {
                                const rows = (segmentAgg.driving || [])
                                    .filter(r => r.roi_delta !== null && r.roi_delta !== undefined)
                                    .filter(r => (r.bets_30d || 0) >= Math.max(3, minSegmentBets));
                                const top = rows.slice().sort((a, b) => Math.abs(Number(b.roi_delta || 0)) - Math.abs(Number(a.roi_delta || 0))).slice(0, 9);
                                return top.map((r, i) => (
                                    <div key={i} className="bg-slate-900/30 border border-slate-800 rounded-xl p-3">
                                        <div className="text-slate-200 font-bold text-sm truncate">{r.sport} — <span className="text-slate-400 font-normal">{r.bet_type}</span></div>
                                        <div className={`mt-1 font-mono font-black ${(Number(r.roi_delta || 0) >= 0) ? 'text-green-300' : 'text-red-300'}`}>{Number(r.roi_delta) >= 0 ? '+' : ''}{Number(r.roi_delta).toFixed(1)}%</div>
                                        <div className="text-[10px] text-slate-500">ROI 30d: {r.roi_30d === null ? '—' : `${Number(r.roi_30d).toFixed(1)}%`} • prev: {r.roi_prev30d === null ? '—' : `${Number(r.roi_prev30d).toFixed(1)}%`} • N={r.bets_30d || 0}</div>
                                    </div>
                                ));
                            })()}
                        </div>
                    </div>

                    <div className="overflow-x-auto border border-slate-800 rounded-xl">
                        <table className="min-w-full text-left text-sm">
                            <thead className="bg-slate-900/60 border-b border-slate-800">
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500">
                                    <th className="py-2 px-3">Segment</th>
                                    <th className="py-2 px-3 text-right">Bets (all)</th>
                                    <th className="py-2 px-3 text-right">Avg odds</th>
                                    <th className="py-2 px-3 text-right">Avg stake</th>
                                    <th className="py-2 px-3 text-right">P/B</th>
                                    <th className="py-2 px-3 text-right">ROI (all)</th>
                                    <th className="py-2 px-3 text-right">Win% (all)</th>
                                    <th className="py-2 px-3 text-right">P/L (30d)</th>
                                    <th className="py-2 px-3 text-right">ROI (30d)</th>
                                    <th className="py-2 px-3 text-right">ROI Δ</th>
                                </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-800/60">
                                {(segmentAgg.driving || [])
                                    .slice()
                                    .sort((a, b) => {
                                        // sort by last 30d ROI desc, then by sample size desc
                                        const ar = (a.roi_30d === null || a.roi_30d === undefined) ? -Infinity : Number(a.roi_30d);
                                        const br = (b.roi_30d === null || b.roi_30d === undefined) ? -Infinity : Number(b.roi_30d);
                                        if (br !== ar) return br - ar;
                                        const an = Number(a.bets_30d || 0);
                                        const bn = Number(b.bets_30d || 0);
                                        if (bn !== an) return bn - an;
                                        return Number(b.profit_30d || 0) - Number(a.profit_30d || 0);
                                    })
                                    .slice(0, 25)
                                    .map((r, i) => {
                                        const roiDelta = r.roi_delta;
                                        const roiDeltaCls = roiDelta === null ? 'text-slate-500' : roiDelta >= 0 ? 'text-green-400' : 'text-red-400';
                                        return (
                                            <tr key={i} className="hover:bg-slate-800/30">
                                                <td className="py-2 px-3 text-slate-200 font-bold">{r.sport} — <span className="text-slate-400 font-normal">{r.bet_type}</span></td>
                                                <td className="py-2 px-3 text-right text-slate-300 font-mono">{r.bets}</td>
                                                <td className="py-2 px-3 text-right text-slate-300 font-mono">{r.avg_odds === null ? '—' : (r.avg_odds > 0 ? `+${r.avg_odds}` : String(r.avg_odds))}</td>
                                                <td className="py-2 px-3 text-right text-slate-300 font-mono">{formatCurrency(r.avg_stake || 0)}</td>
                                                <td className={`py-2 px-3 text-right font-mono ${(r.profit_per_bet || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{formatCurrency(r.profit_per_bet || 0)}</td>
                                                <td className={`py-2 px-3 text-right font-mono ${Number(r.roi || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{r.roi.toFixed(1)}%</td>
                                                <td className="py-2 px-3 text-right text-slate-300 font-mono">{Number(r.actual_win_rate || 0).toFixed(1)}%</td>
                                                <td className={`py-2 px-3 text-right font-mono ${Number(r.profit_30d || 0) >= 0 ? 'text-green-300' : 'text-red-300'}`}>{r.profit_30d === null ? '—' : formatCurrency(r.profit_30d)}</td>
                                                <td className="py-2 px-3 text-right text-slate-300 font-mono">{r.roi_30d === null ? '—' : `${Number(r.roi_30d).toFixed(1)}%`}</td>
                                                <td className={`py-2 px-3 text-right font-mono ${roiDeltaCls}`}>{roiDelta === null ? '—' : `${roiDelta >= 0 ? '+' : ''}${roiDelta.toFixed(1)}%`}</td>
                                            </tr>
                                        );
                                    })}
                            </tbody>
                        </table>
                    </div>

                    <div className="mt-2 text-[10px] text-slate-600">
                        ROI Δ compares last 30d ROI to prior 30d ROI for the same (sport, bet type) segment.
                    </div>
                </div>
            </div>
        </div>
    );
}

const BankrollCard = ({ provider, data }) => (
    <div className="bg-slate-900 border border-slate-800 p-4 rounded-xl flex items-center justify-between min-w-[220px]">
        <div>
            <div className="text-gray-400 text-xs font-bold uppercase tracking-wider mb-0.5">{provider}</div>
            <div className={`text-xl font-bold ${Number(data.balance) >= 0 ? 'text-white' : 'text-red-400'}`}>
                {formatCurrency(Number(data.balance || 0))}
            </div>

            <div className="text-[10px] text-gray-600 mt-1 font-mono">
                Source: {data.source || 'manual'}
                {data.captured_at ? ` • ${new Date(data.captured_at).toLocaleString([], { month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit' })}` : ''}
            </div>
        </div>
        <div className={`p-2 rounded-full ${provider === 'DraftKings' ? 'bg-orange-900/20 text-orange-400' : 'bg-blue-900/20 text-blue-400'}`}>
            <DollarSign size={20} />
        </div>
    </div>
);

function SummaryView({ stats, sportBreakdown, playerBreakdown, monthlyBreakdown, timeSeries, betTypeBreakdown, edgeBreakdown, balances, periodStats, financials }) {
    const [sortConfig, setSortConfig] = useState({ key: 'edge', direction: 'desc' });

    // Sort sport breakdown by profit for chart
    const sortedSportBreakdown = [...sportBreakdown].sort((a, b) => b.profit - a.profit);

    // Sorting Logic for Edge Analysis
    const handleSort = (key) => {
        let direction = 'desc';
        if (sortConfig.key === key && sortConfig.direction === 'desc') {
            direction = 'asc';
        }
        setSortConfig({ key, direction });
    };

    const sortedEdgeBreakdown = [...edgeBreakdown].sort((a, b) => {
        if (a[sortConfig.key] < b[sortConfig.key]) {
            return sortConfig.direction === 'asc' ? -1 : 1;
        }
        if (a[sortConfig.key] > b[sortConfig.key]) {
            return sortConfig.direction === 'asc' ? 1 : -1;
        }
        return 0;
    });

    return (
        <div className="space-y-8">
            {/* Bankroll Section */}
            <div className="flex flex-wrap gap-4 items-stretch">
                {/* Clean Summary: No FinancialHeader here */}
                {Object.entries(balances)
                    .filter(([provider]) => provider && provider !== 'Barstool' && provider !== 'Other' && provider !== 'Total Net Profit')
                    .map(([provider, data]) => (
                        <BankrollCard key={provider} provider={provider} data={data} />
                    ))}

                {/* Total In Play Tile (Audited) */}
                {financials?.breakdown && (
                    <div className="bg-slate-900 border border-green-600/30 rounded-xl p-4 flex items-center justify-between min-w-[220px]">
                        <div>
                            <div className="text-gray-400 text-xs font-bold uppercase tracking-wider mb-0.5">Total In Play</div>
                            <div className="text-xl font-bold text-white">
                                {formatCurrency(
                                    financials.breakdown
                                        .filter(prov => prov.provider === 'DraftKings' || prov.provider === 'FanDuel')
                                        .reduce((sum, p) => sum + (p.in_play || 0), 0)
                                )}
                            </div>
                            <div className="text-[10px] text-gray-600 mt-1 font-mono">
                                Audit: Sum of DK + FD
                            </div>
                        </div>
                        <div className="p-2 rounded-full bg-green-900/20 text-green-400">
                            <Activity size={20} />
                        </div>
                    </div>
                )}
            </div>



            {/* Period Analytics (7d, 30d, YTD) */}
            <div>
                <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                    <TrendingUp className="text-green-400" /> Performance Windows
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    {['7d', '30d', 'ytd'].map(period => {
                        const data = periodStats[period];
                        if (!data) return null;
                        const label = period === 'ytd' ? 'Year to Date' : `Last ${period.replace('d', ' Days')}`;
                        return (
                            <div key={period} className="bg-slate-900/50 border border-slate-800 p-5 rounded-xl">
                                <div className="text-gray-400 text-xs font-bold uppercase mb-2">{label}</div>
                                <div className="flex justify-between items-end mb-2">
                                    <span className={`text-2xl font-bold ${data.net_profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                        {formatCurrency(data.net_profit)}
                                    </span>
                                    <span className={`text-sm font-bold ${data.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                        {data.roi.toFixed(1)}% ROI
                                    </span>
                                </div>
                                <div className="text-xs text-gray-500 flex justify-between">
                                    <span>{data.wins}W - {data.losses}L</span>
                                    <span>{data.total_bets} Bets</span>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* Bet Performance Summary Table */}
            <div className="bg-slate-900/50 border border-slate-800 rounded-xl overflow-hidden p-6">
                <h3 className="text-xl font-bold mb-4">Bet Performance Summary</h3>
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="text-gray-400 border-b border-slate-800 text-[10px] uppercase tracking-wider">
                                <th className="pb-3 pl-2">Period</th>
                                <th className="pb-3 text-right">Record</th>
                                <th className="pb-3 text-right">Implied WR</th>
                                <th className="pb-3 text-right">Actual WR</th>
                                <th className="pb-3 text-right pr-2">Edge</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/50">
                            {['7d', '30d', 'ytd', 'all'].map(p => {
                                const d = periodStats[p];
                                if (!d || d.total_bets === 0) return null;
                                const label = p === 'all' ? 'All Time' : p === 'ytd' ? 'Year to Date' : `Last ${p.replace('d', ' Days')}`;
                                const edge = d.actual_win_rate - d.implied_win_rate;
                                return (
                                    <tr key={p} className="hover:bg-slate-800/20">
                                        <td className="py-3 pl-2 font-medium text-white">{label}</td>
                                        <td className="py-3 text-right text-gray-400">
                                            {d.wins} - {d.losses} {(d.total_bets - d.wins - d.losses) > 0 ? `- ${d.total_bets - d.wins - d.losses} (P/V)` : ''}
                                        </td>
                                        <td className="py-3 text-right text-gray-400">
                                            {d.implied_win_rate.toFixed(1)}%
                                        </td>
                                        <td className={`py-3 text-right font-bold ${d.actual_win_rate >= d.implied_win_rate ? 'text-green-400' : 'text-gray-200'}`}>
                                            {d.actual_win_rate.toFixed(1)}%
                                        </td>
                                        <td className={`py-3 text-right pr-2 font-bold ${edge >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {edge > 0 ? '+' : ''}{edge.toFixed(1)}%
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Charts Grid */}
            {/* Charts Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Total Money In Play (Daily) */}
                <div className="bg-slate-900/50 border border-slate-800 p-6 rounded-xl backdrop-blur-sm">
                    <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                        Total Money In Play
                    </h3>
                    <div className="h-[300px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={timeSeries}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                                <XAxis
                                    dataKey="date"
                                    stroke="#94a3b8"
                                    tickFormatter={(val) => formatDateMDY(val)}
                                    minTickGap={30}
                                />
                                <YAxis stroke="#94a3b8" />
                                <Tooltip
                                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b' }}
                                    itemStyle={{ color: '#fff' }}
                                    formatter={(value) => formatCurrency(value)}
                                    labelFormatter={(label) => formatDateMDY(label)}
                                />
                                <Line type="monotone" dataKey="balance" stroke="#3b82f6" strokeWidth={2} dot={false} activeDot={{ r: 6 }} name="Balance" />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>

                {/* Profit by Sport (Bar Chart) */}
                <div className="bg-slate-900/50 border border-slate-800 p-6 rounded-xl backdrop-blur-sm">
                    <h3 className="text-xl font-bold mb-4 flex items-center gap-2">
                        Profit by Sport
                    </h3>
                    <div className="h-[300px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={sortedSportBreakdown}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#334155" vertical={false} />
                                <XAxis dataKey="sport" stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} height={60} angle={-45} textAnchor="end" interval={0} />
                                <YAxis stroke="#94a3b8" fontSize={12} tickLine={false} axisLine={false} tickFormatter={(val) => `$${val}`} />
                                <Tooltip
                                    contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b' }}
                                    itemStyle={{ color: '#fff' }}
                                    formatter={(value) => formatCurrency(value)}
                                />
                                <Bar dataKey="profit" radius={[4, 4, 0, 0]}>
                                    {sortedSportBreakdown.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={entry.profit >= 0 ? '#22c55e' : '#ef4444'} />
                                    ))}
                                </Bar>
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>

            {/* Advanced Edge Analysis Segment */}
            <div className="bg-slate-900/50 border border-slate-800 p-6 rounded-xl backdrop-blur-sm mt-8">
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
                    <h3 className="text-xl font-bold flex items-center gap-2">
                        <BarChart3 className="text-blue-400" /> Win% vs ROI (by sport + bet type)
                    </h3>

                    {edgeBreakdown.length > 0 && (
                        <div className="flex flex-wrap gap-4 text-xs">
                            {(() => {
                                const total = edgeBreakdown.reduce((s, i) => s + i.bets, 0);
                                const wins = edgeBreakdown.reduce((s, i) => s + i.wins, 0);
                                const profit = edgeBreakdown.reduce((s, i) => s + i.profit, 0);
                                const impliedSum = edgeBreakdown.reduce((s, i) => s + (i.implied_win_rate * i.bets), 0);
                                const avgImplied = total > 0 ? impliedSum / total : 0;
                                const avgActual = total > 0 ? (wins / total) * 100 : 0;

                                return (
                                    <>
                                        <div className="bg-slate-800/50 px-3 py-1.5 rounded-lg border border-slate-700">
                                            <span className="text-slate-400">Total Bets: </span>
                                            <span className="text-white font-bold">{total}</span>
                                            <span className="text-slate-500 ml-2">({wins}W - {total - wins}L)</span>
                                        </div>
                                        <div className="bg-slate-800/50 px-3 py-1.5 rounded-lg border border-slate-700">
                                            <span className="text-slate-400">Actual WR: </span>
                                            <span className="text-white font-bold">{avgActual.toFixed(1)}%</span>
                                        </div>
                                        <div className="bg-slate-800/50 px-3 py-1.5 rounded-lg border border-slate-700">
                                            <span className="text-slate-400">Implied WR: </span>
                                            <span className="text-white font-bold">{avgImplied.toFixed(1)}%</span>
                                        </div>
                                        <div className="bg-slate-800/50 px-3 py-1.5 rounded-lg border border-slate-700">
                                            <span className="text-slate-400">Total P/L: </span>
                                            <span className={profit >= 0 ? 'text-green-400 font-bold' : 'text-red-400 font-bold'}>
                                                {formatCurrency(profit)}
                                            </span>
                                        </div>
                                    </>
                                );
                            })()}
                        </div>
                    )}
                </div>

                {/* Weighted Performance Visualization */}
                <div className="h-[400px] mb-8 bg-slate-800/20 rounded-xl border border-slate-800/50 p-4">
                    <ResponsiveContainer width="100%" height="100%">
                        <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                            <XAxis
                                type="number"
                                dataKey="roi"
                                name="ROI"
                                unit="%"
                                stroke="#94a3b8"
                                fontSize={10}
                                domain={['auto', 'auto']}
                                label={{ value: 'ROI (%)', position: 'bottom', fill: '#64748b', fontSize: 10 }}
                                tickFormatter={(val) => `${val}%`}
                            />
                            <YAxis
                                type="number"
                                dataKey="actual_win_rate"
                                name="Actual Win Rate"
                                unit="%"
                                stroke="#94a3b8"
                                fontSize={10}
                                domain={[0, 100]}
                                label={{ value: 'Your Performance (Actual WR)', angle: -90, position: 'left', fill: '#64748b', fontSize: 10 }}
                            />
                            <ZAxis type="number" dataKey="bets" range={[50, 400]} name="Volume" />
                            <Tooltip
                                cursor={{ strokeDasharray: '3 3' }}
                                contentStyle={{ backgroundColor: '#0f172a', borderColor: '#1e293b', borderRadius: '8px' }}
                                itemStyle={{ color: '#fff' }}
                                content={({ active, payload }) => {
                                    if (active && payload && payload.length) {
                                        const data = payload[0].payload;
                                        return (
                                            <div className="bg-slate-900 border border-slate-700 p-3 rounded-lg shadow-xl">
                                                <p className="font-bold text-blue-400 mb-1">{data.sport} - {data.bet_type}</p>
                                                <div className="grid grid-cols-2 gap-x-4 text-[10px]">
                                                    <span className="text-slate-400">Bets:</span> <span className="text-white text-right">{data.bets}</span>
                                                    <span className="text-slate-400">Profit:</span> <span className={data.profit >= 0 ? 'text-green-400 text-right' : 'text-red-400 text-right'}>{formatCurrency(data.profit)}</span>
                                                    <span className="text-slate-400">ROI:</span> <span className="text-white text-right">{data.roi}%</span>
                                                    <span className="text-slate-400">Actual WR:</span> <span className="text-white text-right">{data.actual_win_rate}%</span>
                                                    <span className="text-slate-400">Implied WR:</span> <span className="text-white text-right">{data.implied_win_rate}%</span>
                                                    <span className="text-slate-400">Edge:</span> <span className={data.edge >= 0 ? 'text-green-400 text-right' : 'text-red-400 text-right'}>{data.edge > 0 ? '+' : ''}{data.edge}%</span>
                                                </div>
                                            </div>
                                        );
                                    }
                                    return null;
                                }}
                            />
                            {/* Breakeven ROI Reference Line */}
                            <ReferenceLine x={0} stroke="#475569" strokeWidth={1} />

                            <Scatter name="Segments" data={edgeBreakdown}>
                                {edgeBreakdown.map((entry, index) => (
                                    <Cell key={`cell-${index}`} fill={entry.profit >= 0 ? '#10b981' : '#ef4444'} fillOpacity={0.6} stroke={entry.profit >= 0 ? '#10b981' : '#ef4444'} />
                                ))}
                            </Scatter>
                        </ScatterChart>
                    </ResponsiveContainer>
                    <div className="text-[10px] text-slate-500 text-center mt-2 italic">
                        Segments plotted by ROI (X) and Actual Win Rate (Y). Bubble size represents bet volume.
                    </div>
                </div>

                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="text-slate-400 border-b border-slate-700 text-[10px] uppercase tracking-wider">
                            <tr>
                                <th
                                    className="pb-3 pl-2 cursor-pointer hover:text-white transition-colors"
                                    onClick={() => handleSort('sport')}
                                >
                                    Segment {sortConfig.key === 'sport' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                                </th>
                                <th
                                    className="pb-3 text-right cursor-pointer hover:text-white transition-colors"
                                    onClick={() => handleSort('bets')}
                                >
                                    Volume {sortConfig.key === 'bets' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                                </th>
                                <th
                                    className="pb-3 text-right cursor-pointer hover:text-white transition-colors"
                                    onClick={() => handleSort('profit')}
                                >
                                    Profit {sortConfig.key === 'profit' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                                </th>
                                <th
                                    className="pb-3 text-right cursor-pointer hover:text-white transition-colors"
                                    onClick={() => handleSort('roi')}
                                >
                                    ROI {sortConfig.key === 'roi' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                                </th>
                                <th
                                    className="pb-3 text-right pr-2 cursor-pointer hover:text-white transition-colors"
                                    onClick={() => handleSort('edge')}
                                >
                                    Edge vs Market {sortConfig.key === 'edge' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                                </th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/50">
                            {sortedEdgeBreakdown.map((item, idx) => {
                                const isPositive = item.edge >= 0;
                                return (
                                    <tr key={idx} className="hover:bg-slate-800/20 transition-colors group">
                                        <td className="py-3 pl-2">
                                            <div className="flex flex-col">
                                                <span className="font-bold text-slate-200">{item.sport}</span>
                                                <span className="text-xs text-slate-500">{item.bet_type}</span>
                                            </div>
                                        </td>
                                        <td className="py-3 text-right text-slate-400 font-mono">
                                            {item.bets} <span className="text-[10px] text-slate-600">bets</span>
                                        </td>
                                        <td className={`py-3 text-right font-bold ${item.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {formatCurrency(item.profit)}
                                        </td>
                                        <td className={`py-3 text-right font-medium ${item.roi >= 0 ? 'text-slate-300' : 'text-red-400/80'}`}>
                                            {item.roi > 0 ? '+' : ''}{item.roi.toFixed(1)}%
                                        </td>
                                        <td className={`py-3 text-right pr-2 font-bold ${isPositive ? 'text-emerald-400' : 'text-rose-400'}`}>
                                            <div className="flex items-center justify-end gap-1 font-mono">
                                                <span>{item.edge > 0 ? '+' : ''}{item.edge.toFixed(1)}%</span>
                                                {isPositive ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                                            </div>
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
                <div className="mt-4 p-3 bg-blue-900/10 border border-blue-900/20 rounded-lg text-[11px] text-blue-300 leading-relaxed">
                    <strong>How to read this:</strong> We compare your actual win percentage for each (Sport + Bet Type) combination against the market's implied expectations.
                    Segments with high <strong>positive edge</strong> are where you consistently find value. Focus your bankroll on these green zones.
                </div>
            </div>



            {/* Live Odds Section (Full Width) */}
            <div className="bg-slate-900/50 border border-slate-800 p-6 rounded-xl backdrop-blur-sm">
                <div className="flex justify-between items-center mb-4">
                    <h3 className="text-xl font-bold flex items-center gap-2">
                        Live Odds (NFL)
                    </h3>
                    <span className="text-xs text-green-400 bg-green-900/30 border border-green-800 px-2 py-1 rounded">Live Data Active</span>
                </div>
                <OddsTicker />
            </div>
        </div >
    );
}

function OddsTicker() {
    const [odds, setOdds] = useState([]);

    useEffect(() => {
        api.get('/api/odds/NFL')
            .then(res => {
                if (Array.isArray(res.data)) {
                    setOdds(res.data);
                } else {
                    console.warn("Odds API returned non-array:", res.data);
                    setOdds([]);
                }
            })
            .catch(err => {
                console.error("Odds API Error:", err);
                setOdds([]);
            });
    }, []);

    if (!Array.isArray(odds) || odds.length === 0) return <div className="text-gray-400 text-sm">Loading Live Odds (or no live games)...</div>;

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {odds.slice(0, 4).map((game) => {
                // Safe access
                const book = game.bookmakers?.[0];
                const market = book?.markets?.[0];
                if (!market) return null;

                const home = market.outcomes.find(o => o.name === game.home_team);
                const away = market.outcomes.find(o => o.name === game.away_team);

                return (
                    <div key={game.id} className="bg-slate-800 p-3 rounded-lg border border-slate-700 text-sm">
                        <div className="flex justify-between items-center mb-2">
                            <span className="text-xs text-blue-400 font-bold uppercase">{game.sport_key.replace('americanfootball_', '')}</span>
                            <span className="text-[10px] text-gray-500">FanDuel</span>
                        </div>
                        <div className="flex justify-between items-center mb-1">
                            <span className="font-medium text-gray-300">{game.away_team}</span>
                            <span className="font-mono text-green-400">{away?.price}</span>
                        </div>
                        <div className="flex justify-between items-center">
                            <span className="font-medium text-gray-300">{game.home_team}</span>
                            <span className="font-mono text-green-400">{home?.price}</span>
                        </div>
                    </div>
                );
            })}
        </div>
    );
}

function TransactionView({ bets, financials }) {
    const [filters, setFilters] = useState({
        date: "",
        sportsbook: "All",
        sport: "All",
        type: "All",
        selection: "",
        status: "All"
    });

    // Optional prefill from other charts (e.g., Performance scatter)
    useEffect(() => {
        try {
            const raw = localStorage.getItem('txn_prefill');
            if (!raw) return;
            const p = JSON.parse(raw);
            localStorage.removeItem('txn_prefill');
            setFilters(f => ({
                ...f,
                sport: p?.sport ? String(p.sport).toUpperCase() : f.sport,
                type: p?.type ? String(p.type) : f.type,
                sportsbook: p?.sportsbook ? String(p.sportsbook) : f.sportsbook,
                selection: p?.selection ? String(p.selection) : f.selection,
            }));
        } catch (e) {}
    }, []);

    const [showManualAdd, setShowManualAdd] = useState(false);
    const [showEdit, setShowEdit] = useState(false);
    const [editBet, setEditBet] = useState(null);
    const [editNote, setEditNote] = useState('');

    const [manualBet, setManualBet] = useState({
        sportsbook: "DraftKings",
        sport: "NFL",
        market_type: "Straight",
        event_name: "",
        selection: "",
        odds: "",
        stake: "",
        status: "LOST",
        placed_at: new Date().toISOString().slice(0, 10)
    });

    // Extract unique options for dropdowns - keep "All" at top
    const sportsbooks = ["All", ...[...new Set(bets.map(b => b.provider).filter(Boolean))].sort()];
    const sports = ["All", ...[...new Set(bets.map(b => b.sport).filter(Boolean))].sort()];
    const types = ["All", ...[...new Set(bets.map(b => b.bet_type).filter(Boolean))].sort()];
    const [sortConfig, setSortConfig] = useState({ key: 'date', direction: 'descending' });
    const [error, setError] = useState(null);
    const [isUpdating, setIsUpdating] = useState(false);

    const requestSort = (key) => {
        let direction = 'ascending';
        if (sortConfig.key === key && sortConfig.direction === 'ascending') {
            direction = 'descending';
        }
        setSortConfig({ key, direction });
    };

    const getSortIcon = (name) => {
        if (sortConfig.key !== name) return <div className="w-3 h-3 inline-block ml-1 opacity-20">↕</div>;
        return sortConfig.direction === 'ascending' ?
            <div className="w-3 h-3 inline-block ml-1">↑</div> :
            <div className="w-3 h-3 inline-block ml-1">↓</div>;
    };

    const handleSettle = async (betId, status) => {
        setIsUpdating(true);
        try {
            await api.patch(`/api/bets/${betId}/settle`, { status });
            // For now, reload to get fresh stats
            window.location.reload();
        } catch (err) {
            console.error("Settle Error:", err);
            alert("Failed to settle bet.");
        } finally {
            setIsUpdating(false);
        }
    };

    const handleEditSave = async () => {
        if (!editBet?.id) return;
        setIsUpdating(true);
        try {
            await api.patch(`/api/bets/${editBet.id}`, {
                provider: editBet.provider,
                date: editBet.date,
                sport: editBet.sport,
                bet_type: editBet.bet_type,
                wager: editBet.wager,
                odds: editBet.odds,
                profit: editBet.profit,
                status: editBet.status,
                description: editBet.description,
                selection: editBet.selection,
                update_note: editNote,
            });
            setShowEdit(false);
            setEditBet(null);
            setEditNote('');
            window.location.reload();
        } catch (err) {
            console.error('Edit save failed', err);
            alert('Failed to update bet.');
        } finally {
            setIsUpdating(false);
        }
    };

    const handleDelete = async (betId) => {
        if (!confirm("Are you sure you want to delete this bet?")) return;
        setIsUpdating(true);
        try {
            await api.delete(`/api/bets/${betId}`);
            window.location.reload();
        } catch (err) {
            console.error("Delete Error:", err);
            alert("Failed to delete bet.");
        } finally {
            setIsUpdating(false);
        }
    };

    const submitManualBet = async () => {
        setIsUpdating(true);
        try {
            const american = manualBet.odds ? parseInt(manualBet.odds, 10) : null;
            const stake = manualBet.stake ? parseFloat(manualBet.stake) : 0;

            await api.post('/api/bets/manual', {
                sportsbook: manualBet.sportsbook,
                sport: manualBet.sport,
                market_type: manualBet.market_type,
                event_name: manualBet.event_name,
                selection: manualBet.selection,
                price: { american },
                stake,
                status: manualBet.status,
                placed_at: manualBet.placed_at,
                raw_text: 'manual-ui'
            });

            setShowManualAdd(false);
            setManualBet({
                sportsbook: manualBet.sportsbook,
                sport: manualBet.sport,
                market_type: manualBet.market_type,
                event_name: "",
                selection: "",
                odds: "",
                stake: "",
                status: manualBet.status,
                placed_at: new Date().toISOString().slice(0, 10)
            });
            window.location.reload();
        } catch (err) {
            console.error('Manual bet save failed', err);
            alert('Failed to add bet. Check required fields.');
        } finally {
            setIsUpdating(false);
        }
    };

    const statuses = ['All', 'PENDING', 'WON', 'LOST', 'PUSH'];
    const filtered = bets.filter(b => {
        // Filter out internal Wallet Transfers
        if ((b.description || "").toLowerCase().includes("wallet transfer")) return false;

        const matchDate = b.date.includes(filters.date);
        const matchSportsbook = filters.sportsbook === "All" || b.provider === filters.sportsbook;
        const matchSport = filters.sport === "All" || b.sport === filters.sport;
        const matchType = filters.type === "All" || b.bet_type === filters.type;
        const matchSelection = (b.selection || b.description || "").toLowerCase().includes(filters.selection.toLowerCase());
        const matchStatus = filters.status === "All" || b.status === filters.status;
        return matchDate && matchSportsbook && matchSport && matchType && matchSelection && matchStatus;
    });

    const sortedBets = React.useMemo(() => {
        let sortableItems = [...filtered];
        if (sortConfig.key !== null) {
            sortableItems.sort((a, b) => {
                let aValue = a[sortConfig.key];
                let bValue = b[sortConfig.key];

                // Special handling for date: use sort_date for proper chronological sort
                if (sortConfig.key === 'date') {
                    aValue = a.sort_date || a.date || "";
                    bValue = b.sort_date || b.date || "";
                }

                // Special handling for selection fallback
                if (sortConfig.key === 'selection') {
                    aValue = a.selection || a.description || "";
                    bValue = b.selection || b.description || "";
                }

                // Handle string comparisons
                if (typeof aValue === 'string') aValue = aValue.toLowerCase();
                if (typeof bValue === 'string') bValue = bValue.toLowerCase();

                // Handle null/undefined (push to bottom usually, or top? let's standardise)
                if (aValue === null || aValue === undefined) return 1;
                if (bValue === null || bValue === undefined) return -1;

                if (aValue < bValue) {
                    return sortConfig.direction === 'ascending' ? -1 : 1;
                }
                if (aValue > bValue) {
                    return sortConfig.direction === 'ascending' ? 1 : -1;
                }
                return 0;
            });
        }
        return sortableItems;
    }, [filtered, sortConfig]);

    const resetFilters = () => setFilters({ date: "", sportsbook: "All", sport: "All", type: "All", selection: "", status: "All" });

    return (
        <div className="space-y-8">
            {showEdit && editBet && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
                    <div className="w-full max-w-2xl bg-slate-900 border border-slate-700 rounded-xl shadow-2xl p-5">
                        <div className="flex items-center justify-between mb-4">
                            <div className="text-white font-bold">Edit Bet</div>
                            <button type="button" className="text-gray-400 hover:text-white" onClick={() => { setShowEdit(false); setEditBet(null); setEditNote(''); }}>✕</button>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Date</div>
                                <input
                                    type="date"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={(editBet.date || '').slice(0, 10)}
                                    onChange={(e) => setEditBet({ ...editBet, date: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Sportsbook</div>
                                <select
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.provider || ''}
                                    onChange={(e) => setEditBet({ ...editBet, provider: e.target.value })}
                                >
                                    <option value="DraftKings">DraftKings</option>
                                    <option value="FanDuel">FanDuel</option>
                                </select>
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Sport</div>
                                <input
                                    type="text"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.sport || ''}
                                    onChange={(e) => setEditBet({ ...editBet, sport: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Bet Type</div>
                                <input
                                    type="text"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.bet_type || ''}
                                    onChange={(e) => setEditBet({ ...editBet, bet_type: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Wager</div>
                                <input
                                    type="number"
                                    step="0.01"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.wager ?? ''}
                                    onChange={(e) => setEditBet({ ...editBet, wager: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Odds (American)</div>
                                <input
                                    type="number"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.odds ?? ''}
                                    onChange={(e) => setEditBet({ ...editBet, odds: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Profit</div>
                                <input
                                    type="number"
                                    step="0.01"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.profit ?? ''}
                                    onChange={(e) => setEditBet({ ...editBet, profit: e.target.value })}
                                />
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Status</div>
                                <select
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.status || 'PENDING'}
                                    onChange={(e) => setEditBet({ ...editBet, status: e.target.value })}
                                >
                                    <option value="WON">WON</option>
                                    <option value="LOST">LOST</option>
                                    <option value="PUSH">PUSH</option>
                                    <option value="PENDING">PENDING</option>
                                </select>
                            </div>
                            <div className="md:col-span-2">
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Event / Description</div>
                                <input
                                    type="text"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.description || ''}
                                    onChange={(e) => setEditBet({ ...editBet, description: e.target.value })}
                                />
                            </div>
                            <div className="md:col-span-2">
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Selection</div>
                                <input
                                    type="text"
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    value={editBet.selection || ''}
                                    onChange={(e) => setEditBet({ ...editBet, selection: e.target.value })}
                                />
                            </div>

                            <div className="md:col-span-2">
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Audit note (optional)</div>
                                <textarea
                                    className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
                                    rows={2}
                                    placeholder="e.g., fixed odds typo"
                                    value={editNote}
                                    onChange={(e) => setEditNote(e.target.value)}
                                />
                            </div>
                        </div>

                        <div className="mt-5 flex items-center justify-end gap-3">
                            <button
                                type="button"
                                className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-bold"
                                onClick={() => { setShowEdit(false); setEditBet(null); }}
                                disabled={isUpdating}
                            >
                                Cancel
                            </button>
                            <button
                                type="button"
                                className="px-4 py-2 bg-green-600 hover:bg-green-500 text-black rounded-lg text-sm font-black"
                                onClick={handleEditSave}
                                disabled={isUpdating}
                            >
                                {isUpdating ? 'Saving…' : 'Save'}
                            </button>
                        </div>
                        <div className="mt-2 text-[10px] text-slate-500">
                            Saves directly to the database (persists for history + analytics).
                        </div>
                    </div>
                </div>
            )}

            {showManualAdd && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
                    <div className="w-full max-w-2xl bg-slate-900 border border-slate-700 rounded-xl shadow-2xl p-5">
                        <div className="flex items-center justify-between mb-4">
                            <div className="text-white font-bold">Add Bet (Manual)</div>
                            <button type="button" className="text-gray-400 hover:text-white" onClick={() => setShowManualAdd(false)}>✕</button>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                            <div>
                                <label className="text-xs text-gray-400">Sportsbook</label>
                                <select className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    value={manualBet.sportsbook}
                                    onChange={e => setManualBet({ ...manualBet, sportsbook: e.target.value })}
                                >
                                    <option>DraftKings</option>
                                    <option>FanDuel</option>
                                </select>
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Date (YYYY-MM-DD)</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    value={manualBet.placed_at}
                                    onChange={e => setManualBet({ ...manualBet, placed_at: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Sport</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="NFL, NBA, NCAAM..."
                                    value={manualBet.sport}
                                    onChange={e => setManualBet({ ...manualBet, sport: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Type</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="Straight, SGP, Parlay..."
                                    value={manualBet.market_type}
                                    onChange={e => setManualBet({ ...manualBet, market_type: e.target.value })}
                                />
                            </div>
                            <div className="md:col-span-2">
                                <label className="text-xs text-gray-400">Event / Game</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="e.g., Patriots vs Broncos"
                                    value={manualBet.event_name}
                                    onChange={e => setManualBet({ ...manualBet, event_name: e.target.value })}
                                />
                            </div>
                            <div className="md:col-span-2">
                                <label className="text-xs text-gray-400">Selection</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="e.g., Under 28.5"
                                    value={manualBet.selection}
                                    onChange={e => setManualBet({ ...manualBet, selection: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Odds (American)</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="e.g., -110 or 254"
                                    value={manualBet.odds}
                                    onChange={e => setManualBet({ ...manualBet, odds: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Wager ($)</label>
                                <input className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    placeholder="e.g., 10"
                                    value={manualBet.stake}
                                    onChange={e => setManualBet({ ...manualBet, stake: e.target.value })}
                                />
                            </div>
                            <div>
                                <label className="text-xs text-gray-400">Status</label>
                                <select className="w-full bg-slate-950 border border-slate-700 rounded px-2 py-2 text-sm text-white"
                                    value={manualBet.status}
                                    onChange={e => setManualBet({ ...manualBet, status: e.target.value })}
                                >
                                    <option value="WON">WON</option>
                                    <option value="LOST">LOST</option>
                                    <option value="PENDING">PENDING</option>
                                    <option value="PUSH">PUSH</option>
                                </select>
                            </div>
                            <div className="md:col-span-1 flex items-end justify-end gap-2">
                                <button
                                    type="button"
                                    className="px-3 py-2 rounded-lg border border-slate-700 text-gray-200 hover:bg-slate-800"
                                    onClick={() => setShowManualAdd(false)}
                                >
                                    Cancel
                                </button>
                                <button
                                    type="button"
                                    className="px-3 py-2 rounded-lg bg-green-600 hover:bg-green-500 text-white font-bold"
                                    onClick={submitManualBet}
                                    disabled={isUpdating}
                                >
                                    Save
                                </button>
                            </div>
                        </div>

                        <div className="mt-3 text-[11px] text-gray-500">
                            This creates a bet row directly in your Transactions table (manual tracking).
                        </div>
                    </div>
                </div>
            )}
            {/* Sportsbook Balance Summary Tiles */}
            {financials?.breakdown && (
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                    {financials.breakdown
                        .filter(prov => prov.provider === 'DraftKings' || prov.provider === 'FanDuel')
                        .map((prov) => (
                            <div key={prov.provider} className={`bg-slate-900 border rounded-xl p-5 ${prov.provider === 'DraftKings' ? 'border-orange-600/30' : 'border-blue-600/30'}`}>
                                <div className="flex items-center justify-between mb-3">
                                    <span className={`text-sm font-bold uppercase tracking-wider ${prov.provider === 'DraftKings' ? 'text-orange-400' : 'text-blue-400'}`}>
                                        {prov.provider}
                                    </span>
                                    <DollarSign className={`w-5 h-5 ${prov.provider === 'DraftKings' ? 'text-orange-400' : 'text-blue-400'}`} />
                                </div>
                                <div className="text-3xl font-bold text-white mb-1">
                                    {formatCurrency(prov.in_play || 0)}
                                </div>
                                <div className="text-xs text-gray-400">Current Balance</div>
                            </div>
                        ))}
                    {/* Total In Play Tile (Calculated) */}
                    {(() => {
                        const calculatedTotal = financials.breakdown
                            .filter(prov => prov.provider === 'DraftKings' || prov.provider === 'FanDuel')
                            .reduce((sum, p) => sum + (p.in_play || 0), 0);

                        return (
                            <div className="bg-slate-900 border border-green-600/30 rounded-xl p-5">
                                <div className="flex items-center justify-between mb-3">
                                    <span className="text-sm font-bold uppercase tracking-wider text-green-400">Total In Play</span>
                                    <Activity className="w-5 h-5 text-green-400" />
                                </div>
                                <div className="text-3xl font-bold text-white mb-1">
                                    {formatCurrency(calculatedTotal)}
                                </div>
                                <div className="text-xs text-gray-400">All Sportsbooks</div>
                            </div>
                        );
                    })()}
                </div>
            )}




            <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden shadow-xl">
                {/* Toolbar / Summary */}
                <div className="p-4 border-b border-gray-800 flex justify-between items-center bg-gray-900/50 backdrop-blur">
                    <div className="text-gray-400 text-sm">
                        Showing <span className="text-white font-bold">{filtered.length}</span> of {bets.length} transactions
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            onClick={() => setShowManualAdd(true)}
                            className="text-xs text-green-300 hover:text-green-200 font-medium px-3 py-1.5 rounded-lg border border-green-900/40 hover:bg-green-900/20 transition"
                        >
                            + Add Bet
                        </button>
                        <button
                            onClick={resetFilters}
                            className="text-xs text-blue-400 hover:text-blue-300 font-medium px-3 py-1.5 rounded-lg border border-blue-900/30 hover:bg-blue-900/20 transition"
                        >
                            Clear Filters
                        </button>
                    </div>
                </div>

                {/* Grid */}
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm whitespace-nowrap">
                        <thead className="bg-gray-800 text-gray-400 font-medium uppercase text-xs tracking-wider">
                            {/* Header Labels */}
                            <tr>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('date')}
                                >
                                    Date {getSortIcon('date')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('provider')}
                                >
                                    Sportsbook {getSortIcon('provider')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('sport')}
                                >
                                    Sport {getSortIcon('sport')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('bet_type')}
                                >
                                    Type {getSortIcon('bet_type')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('selection')}
                                >
                                    Selection {getSortIcon('selection')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 text-right cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('odds')}
                                >
                                    Odds {getSortIcon('odds')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 text-right cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('wager')}
                                >
                                    Wager {getSortIcon('wager')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 text-center cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('status')}
                                >
                                    Status {getSortIcon('status')}
                                </th>
                                <th
                                    className="px-6 py-3 border-b border-gray-700 text-right cursor-pointer hover:bg-gray-800 select-none"
                                    onClick={() => requestSort('profit')}
                                >
                                    Profit / Loss {getSortIcon('profit')}
                                </th>
                                <th className="px-6 py-3 border-b border-gray-700 text-right">Actions</th>
                            </tr>
                            {/* Filter Row */}
                            <tr className="bg-gray-850">
                                <th className="px-2 py-2">
                                    <input
                                        type="text"
                                        placeholder="Filter Date..."
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.date}
                                        onChange={e => setFilters({ ...filters, date: e.target.value })}
                                    />
                                </th>
                                <th className="px-2 py-2">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.sportsbook}
                                        onChange={e => setFilters({ ...filters, sportsbook: e.target.value })}
                                    >
                                        <option value="All">All Books</option>
                                        {sportsbooks.filter(s => s !== 'All').map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-2 py-2">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.sport}
                                        onChange={e => setFilters({ ...filters, sport: e.target.value })}
                                    >
                                        <option value="All">All Sports</option>
                                        {sports.filter(s => s !== 'All').map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-2 py-2">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.type}
                                        onChange={e => setFilters({ ...filters, type: e.target.value })}
                                    >
                                        {types.map(t => <option key={t} value={t}>{t}</option>)}
                                    </select>
                                </th>
                                <th className="px-2 py-2">
                                    <input
                                        type="text"
                                        placeholder="Search Selection..."
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.selection}
                                        onChange={e => setFilters({ ...filters, selection: e.target.value })}
                                    />
                                </th>
                                <th className="px-2 py-2"></th> {/* Odds */}
                                <th className="px-2 py-2"></th> {/* Wager */}
                                <th className="px-2 py-2">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-1 text-xs text-white focus:border-blue-500 outline-none"
                                        value={filters.status}
                                        onChange={e => setFilters({ ...filters, status: e.target.value })}
                                    >
                                        {statuses.map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-2 py-2"></th> {/* Profit */}
                                <th className="px-2 py-2"></th> {/* Actions */}
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-800">
                            {sortedBets.map((bet) => {
                                const isTxn = bet.category === 'Transaction';
                                const isDeposit = bet.bet_type === 'Deposit' || (bet.bet_type === 'Other' && bet.amount > 0);
                                return (
                                    <tr
                                        key={bet.id || bet.txn_id}
                                        className="hover:bg-gray-800/50 transition duration-150 cursor-pointer"
                                        onClick={() => {
                                            if (!bet.id) return;
                                            setEditBet({
                                                id: bet.id,
                                                provider: bet.provider,
                                                date: (bet.sort_date || bet.date || '').slice(0, 10),
                                                sport: bet.sport,
                                                bet_type: bet.bet_type,
                                                wager: bet.wager,
                                                odds: bet.odds,
                                                profit: bet.profit,
                                                status: bet.status,
                                                description: bet.description,
                                                selection: bet.selection,
                                            });
                                            setEditNote('');
                                            setShowEdit(true);
                                        }}
                                    >
                                        <td className="px-6 py-3 text-gray-300 font-mono text-xs" title={formatDateMDY(bet.sort_date || bet.date)}>{formatDateMDY(bet.sort_date || bet.date)}</td>
                                        <td className="px-6 py-3">
                                            <span className="px-2 py-1 rounded text-[10px] text-gray-300 border border-gray-700 bg-gray-800 shadow-sm uppercase font-bold tracking-wider">
                                                {bet.provider}
                                            </span>
                                        </td>
                                        <td className="px-6 py-3">
                                            <span className="px-2 py-1 rounded text-[10px] text-gray-300 border border-gray-700 bg-gray-800 shadow-sm uppercase font-bold tracking-wider">
                                                {bet.sport}
                                            </span>
                                        </td>
                                        <td className="px-6 py-3 text-gray-400 text-xs">{bet.bet_type}</td>
                                        <td className="px-6 py-3 max-w-xs truncate text-gray-300 text-xs" title={bet.selection || bet.description}>
                                            {bet.display_selection || bet.selection || bet.description}
                                            {bet.is_live && <span className="ml-2 text-[9px] bg-red-900/50 text-red-300 px-1 rounded border border-red-800">LIVE</span>}
                                            {bet.is_bonus && <span className="ml-2 text-[9px] bg-yellow-900/50 text-yellow-300 px-1 rounded border border-yellow-800">BONUS</span>}
                                        </td>
                                        <td className="px-6 py-3 text-right font-mono text-gray-400 text-xs">
                                            {!isTxn ? (
                                                <>
                                                    {bet.odds ? (bet.odds > 0 ? `+${bet.odds}` : bet.odds) : '-'}
                                                    {bet.closing_odds && (
                                                        <div className="flex flex-col items-end mt-1">
                                                            <span className="text-[10px] text-gray-500 font-mono">
                                                                CL: {bet.closing_odds > 0 ? '+' : ''}{bet.closing_odds}
                                                            </span>
                                                            <span className={`text-[10px] font-bold ${calculateCLV(bet.odds, bet.closing_odds) > 0 ? 'text-green-400' : 'text-red-400'
                                                                }`}>
                                                                {calculateCLV(bet.odds, bet.closing_odds) > 0 ? '+' : ''}{calculateCLV(bet.odds, bet.closing_odds).toFixed(1)}% CLV
                                                            </span>
                                                        </div>
                                                    )}
                                                </>
                                            ) : '-'}
                                        </td>
                                        <td className={`px-6 py-3 text-right font-medium text-xs ${isTxn ? 'text-gray-400' : 'text-gray-300'}`}>
                                            {formatCurrency(bet.wager)}
                                        </td>
                                        <td className="px-6 py-3 text-center">
                                            <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider border ${isTxn ? (isDeposit ? 'bg-green-900/20 text-green-400 border-green-900' : 'bg-gray-800 text-gray-400 border-gray-700') :
                                                ['WON', 'WIN'].includes(bet.status) ? 'bg-green-900/20 text-green-400 border-green-900' :
                                                    ['LOST', 'LOSE'].includes(bet.status) ? 'bg-red-900/20 text-red-400 border-red-900' :
                                                        'bg-gray-800 text-gray-400 border-gray-700'
                                                }`}>
                                                {isTxn ? (isDeposit ? 'DEPOSIT' : 'WITHDRAWAL') : bet.status}
                                            </span>
                                        </td>
                                        <td className={`px-6 py-3 text-right font-bold text-xs ${bet.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {(bet.profit !== undefined && bet.profit !== null) ? (bet.profit >= 0 ? '+' : '') + formatCurrency(bet.profit) : '-'}
                                        </td>
                                        <td className="px-6 py-3 text-right space-x-2">
                                            {!isTxn && (
                                                <>
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); handleSettle(bet.id, 'WON'); }}
                                                        className="p-1 text-green-500 hover:bg-green-500/10 rounded border border-green-500/20 title='Settle as Win'"
                                                        disabled={isUpdating}
                                                    >
                                                        W
                                                    </button>
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); handleSettle(bet.id, 'LOST'); }}
                                                        className="p-1 text-red-500 hover:bg-red-500/10 rounded border border-red-500/20 title='Settle as Loss'"
                                                        disabled={isUpdating}
                                                    >
                                                        L
                                                    </button>
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); handleSettle(bet.id, 'PUSH'); }}
                                                        className="p-1 text-yellow-500 hover:bg-yellow-500/10 rounded border border-yellow-500/20 title='Settle as Push'"
                                                        disabled={isUpdating}
                                                    >
                                                        P
                                                    </button>
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); handleDelete(bet.id); }}
                                                        className="p-1 text-gray-500 hover:text-red-400 title='Delete'"
                                                        disabled={isUpdating}
                                                    >
                                                        <Trash size={12} />
                                                    </button>
                                                </>
                                            )}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>

                {filtered.length === 0 && (
                    <div className="p-8 text-center text-gray-500 text-sm">
                        No transactions found matching criteria.
                    </div>
                )}
            </div>
        </div >
    );
}

const calculateCLV = (placed, closing) => {
    const implied = (o) => o > 0 ? 100 / (o + 100) : Math.abs(o) / (Math.abs(o) + 100);
    const p = implied(Number(placed));
    const c = implied(Number(closing));
    if (!p || !c) return 0;
    // EV = (TrueProb / BreakEvenProb) - 1
    // TrueProb = Implied(Closing) (assuming efficient market)
    // BreakEvenProb = Implied(Placed)
    return ((c / p) - 1) * 100;
};

// --- Financial Summary Component ---
// --- Financial Header Component ---
const FinancialCard = ({ label, value, icon: Icon, colorClass, borderColor }) => (
    <div className={`bg-slate-900 border ${borderColor || 'border-slate-800'} p-4 rounded-xl flex items-center justify-between min-w-[220px] shadow-sm`}>
        <div>
            <div className="text-gray-400 text-[10px] font-bold uppercase tracking-wider mb-1">{label}</div>
            <div className={`text-xl font-bold ${value < 0 ? 'text-red-400' : 'text-white'}`}>
                {formatCurrency(value)}
            </div>
        </div>
        <div className={`p-2 rounded-full ${colorClass}`}>
            <Icon size={20} />
        </div>
    </div>
);

const FinancialHeader = ({ financials, mode = 'all' }) => {
    if (!financials) return null;
    return (
        <div className="flex flex-wrap gap-4 mb-8">
            <div className="text-[10px] text-slate-500 absolute top-2 right-4">v1.6.2</div>

            {mode !== 'performance' && (
                <FinancialCard
                    label="Total In Play"
                    value={financials.total_in_play}
                    icon={TrendingUp}
                    borderColor="border-green-500/30"
                    colorClass="bg-green-900/20 text-green-400"
                />
            )}
            {mode === 'performance' && (
                <>
                    <FinancialCard
                        label="Net Deposits"
                        value={financials.total_deposited}
                        icon={ArrowUpRight}
                        colorClass="bg-blue-900/20 text-blue-400"
                    />
                    <FinancialCard
                        label="Net Withdrawals"
                        value={financials.total_withdrawn}
                        icon={ArrowDownRight}
                        colorClass="bg-orange-900/20 text-orange-400"
                    />
                    <FinancialCard
                        label="Realized Profit"
                        value={financials.realized_profit}
                        icon={DollarSign}
                        borderColor={financials.realized_profit >= 0 ? "border-green-500/20" : "border-red-500/20"}
                        colorClass={financials.realized_profit >= 0 ? "bg-green-900/10 text-green-500" : "bg-red-900/10 text-red-500"}
                    />
                </>
            )}
        </div>
    );
};




export default App;
// force rebuild
