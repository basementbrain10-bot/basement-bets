import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { ResponsiveContainer, AreaChart, Area, Tooltip } from 'recharts';
import { Trash } from 'lucide-react';

// Extracted from App.jsx. Keep behavior identical; dependencies are passed as props when needed.

export default function TransactionView({ bets, setBets, financials, reconciliation, loading, formatCurrency, formatDateMDY }) {
    // Row selection disabled (checkbox column removed)
    const [isBulkUpdating, setIsBulkUpdating] = useState(false);

    const SkeletonRow = () => (
        <tr className="animate-pulse border-b border-gray-800/20">
            <td className="px-3 py-4 w-[40px]"><div className="h-4 bg-gray-800/50 rounded w-4" /></td>
            <td className="px-3 py-4 w-[84px]"><div className="h-2 bg-gray-800/50 rounded w-16" /></td>
            <td className="px-3 py-4 w-[80px]"><div className="h-4 bg-gray-800/50 rounded w-14" /></td>
            <td className="px-3 py-4 w-[64px]"><div className="h-4 bg-gray-800/50 rounded w-10" /></td>
            <td className="px-3 py-4 w-[70px]"><div className="h-4 bg-gray-800/50 rounded w-12" /></td>
            <td className="px-3 py-4 w-[200px]"><div className="h-4 bg-gray-800/50 rounded w-40" /></td>
            <td className="px-3 py-4 w-[60px] text-right"><div className="h-4 bg-gray-800/50 rounded w-8 ml-auto" /></td>
            <td className="px-3 py-4 w-[70px] text-right"><div className="h-4 bg-gray-800/50 rounded w-12 ml-auto" /></td>
            <td className="px-3 py-4 w-[64px] text-center"><div className="h-5 bg-gray-800/50 rounded w-16 mx-auto" /></td>
            <td className="px-3 py-4 w-[76px] text-right"><div className="h-4 bg-gray-800/50 rounded w-14 ml-auto" /></td>
        </tr>
    );

    const [filters, setFilters] = useState({
        date: "",
        sportsbook: "All",
        sport: "All",
        type: "All",
        selection: "",
        status: "All"
    });

    const sevenDayData = React.useMemo(() => {
        const days = [];
        for (let i = 6; i >= 0; i--) {
            const d = new Date();
            d.setDate(d.getDate() - i);
            days.push(d.toISOString().slice(0, 10));
        }

        let cumulative = 0;
        return days.map(day => {
            const dayProfit = bets
                .filter(b => b.date.includes(day) && (b.category || '') !== 'Transaction')
                .reduce((sum, b) => sum + (Number(b.profit) || 0), 0);
            cumulative += dayProfit;
            return {
                name: day.slice(5), // MM-DD
                profit: cumulative
            };
        });
    }, [bets]);

    // Bulk selection actions removed (checkbox column removed)


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
        } catch (e) { }
    }, []);

    const [showManualAdd, setShowManualAdd] = useState(false);
    const [showEdit, setShowEdit] = useState(false);
    const [editBet, setEditBet] = useState(null);
    const [editNote, setEditNote] = useState('');

    // Auditor UI (sport mismatches)
    const [showAudit, setShowAudit] = useState(false);
    const [auditLoading, setAuditLoading] = useState(false);
    const [auditItems, setAuditItems] = useState([]);

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
            const payload = {
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
                // also persist event_text if we can infer it from the updated fields
                event_text: computeEventText(editBet) || undefined,
                update_note: editNote,
            };

            await api.patch(`/api/bets/${editBet.id}`, payload);

            // Optimistically update the row in-memory so the UI reflects the save immediately.
            if (typeof setBets === 'function') {
                setBets((prev) => (prev || []).map((b) => {
                    if (Number(b.id) !== Number(editBet.id)) return b;
                    const date = String(editBet.date || '').slice(0, 10);
                    const next = {
                        ...b,
                        provider: editBet.provider,
                        date,
                        sort_date: date,
                        sport: editBet.sport,
                        bet_type: editBet.bet_type,
                        wager: editBet.wager === '' ? b.wager : Number(editBet.wager),
                        odds: editBet.odds === '' ? b.odds : Number(editBet.odds),
                        profit: editBet.profit === '' ? b.profit : Number(editBet.profit),
                        status: editBet.status,
                        description: editBet.description,
                        selection: editBet.selection,
                    };
                    try {
                        next.event_text = computeEventText(next) || next.event_text;
                    } catch (e) { }
                    return next;
                }));
            }

            // Reliable way to refresh aggregates (tiles, performance summaries, etc.)
            // without risking stale cached stats.
            window.location.reload();

            // (below won't run, but keep structure safe if we ever remove reload)
            // setShowEdit(false);
            // setEditBet(null);
            // setEditNote('');
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
            if (typeof setBets === 'function') {
                setBets((prev) => (prev || []).filter((b) => Number(b.id) !== Number(betId)));
            }
        } catch (err) {
            console.error("Delete Error:", err);
            alert("Failed to delete bet.");
        } finally {
            setIsUpdating(false);
        }
    };

    const runAudit = async () => {
        setAuditLoading(true);
        try {
            // Audit the full Transactions history (not just a recent slice)
            const { data } = await api.get('/api/audit/bets/sport-mismatches', { params: { days: 3650, limit: 20000 } });
            setAuditItems(data?.items || []);
            setShowAudit(true);
        } catch (err) {
            console.error('Audit failed', err);
            alert('Audit failed. See console.');
        } finally {
            setAuditLoading(false);
        }
    };

    const applyAuditFix = async (item) => {
        const betId = item?.bet_id;
        const sport = item?.suggested_sport;
        if (!betId || !sport) return;
        try {
            await api.post(`/api/audit/bets/${betId}/apply-sport`, { sport });

            // Update UI row immediately
            if (typeof setBets === 'function') {
                setBets((prev) => (prev || []).map((b) => {
                    if (Number(b.id) !== Number(betId)) return b;
                    return { ...b, sport };
                }));
            }

            // Remove from audit list
            setAuditItems((prev) => (prev || []).filter((x) => Number(x.bet_id) !== Number(betId)));
        } catch (err) {
            console.error('Apply audit fix failed', err);
            alert('Failed to apply fix.');
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

            // easiest: refresh to pick up the inserted bet + refreshed analytics
            window.location.reload();
        } catch (err) {
            console.error('Manual bet save failed', err);
            alert('Failed to add bet. Check required fields.');
        } finally {
            setIsUpdating(false);
        }
    };

    const statuses = ['All', 'PENDING', 'WON', 'LOST', 'PUSH'];

    const computeEventText = (bet) => {
        // Compute a clean matchup string ignoring any pre-existing event_text.
        const sources = [bet?.raw_text, bet?.description, bet?.selection].filter(Boolean).map(s => String(s));
        const cleanSide = (x) => {
            let s = String(x || '').trim();
            s = s.replace(/\s+[+\-−–]\d+(?:\.\d+)?\b.*$/, '').trim();
            s = s.replace(/\s+(over|under)\s*\d+(?:\.\d+)?\b.*$/i, '').trim();
            s = s.replace(/\b([A-Za-z]{3,})\s+\1\b/gi, '$1').trim();
            // Strip pipes
            s = s.split('|')[0].trim();
            return s;
        };

        // Prefer explicit matchup line
        for (const src of sources) {
            for (const ln of String(src).split(/\n/).map(l => l.trim()).filter(Boolean)) {
                if (ln.includes('@') || /\b(vs\.?|versus)\b/i.test(ln)) {
                    const mm = ln.match(/(.+?)\s*(?:@|vs\.?|versus)\s*(.+)/i);
                    if (mm) {
                        const a = cleanSide(mm[1]);
                        const b = cleanSide(mm[2]);
                        if (a && b) return `${a} @ ${b}`;
                    }
                }
            }
        }

        // Fallback to combined regex
        const raw = sources.join(' \n ').replace(/\s+/g, ' ').trim();
        if (!raw) return '';
        const m = raw.match(/([A-Za-z0-9\.'\-\s&]+?)\s*(?:@|vs\.?|versus)\s*([A-Za-z0-9\.'\-\s&]+?)(?:\s*\||\s*$)/i);
        if (m) {
            const a = cleanSide(m[1]);
            const b = cleanSide(m[2]);
            if (a && b) return `${a} @ ${b}`;
        }
        return '';
    };

    const extractEvent = (bet) => {
        // UI display function
        if (bet?.event_text) return String(bet.event_text);
        return computeEventText(bet);
    };
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
            {showAudit && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
                    <div className="w-full max-w-4xl bg-slate-900 border border-slate-700 rounded-xl shadow-2xl p-5">
                        <div className="flex items-center justify-between mb-3">
                            <div>
                                <div className="text-white font-bold">Sport Auditor</div>
                                <div className="text-xs text-slate-400">Flags bets where sport disagrees with matched event league (teams + date window).</div>
                            </div>
                            <button type="button" className="text-gray-400 hover:text-white" onClick={() => setShowAudit(false)}>✕</button>
                        </div>

                        <div className="mb-3 flex items-center justify-between">
                            <div className="text-xs text-slate-400">Found <span className="text-white font-bold">{(auditItems || []).length}</span> potential mismatches.</div>
                            <button
                                type="button"
                                onClick={runAudit}
                                disabled={auditLoading}
                                className={`text-xs font-medium px-3 py-1.5 rounded-lg border transition ${auditLoading ? 'text-gray-500 border-gray-800 bg-gray-900/40 animate-pulse' : 'text-amber-300 hover:text-amber-200 border-amber-900/40 hover:bg-amber-900/20'}`}
                            >
                                {auditLoading ? 'Re-running…' : 'Re-run'}
                            </button>
                        </div>

                        <div className="max-h-[60vh] overflow-auto border border-slate-800 rounded-lg">
                            <table className="w-full text-xs">
                                <thead className="sticky top-0 bg-slate-950/90 backdrop-blur border-b border-slate-800">
                                    <tr className="text-slate-400">
                                        <th className="text-left px-3 py-2">Date</th>
                                        <th className="text-left px-3 py-2">Matchup</th>
                                        <th className="text-left px-3 py-2">Book</th>
                                        <th className="text-left px-3 py-2">Type</th>
                                        <th className="text-left px-3 py-2">Current</th>
                                        <th className="text-left px-3 py-2">Suggested</th>
                                        <th className="text-left px-3 py-2">Action</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-800">
                                    {(auditItems || []).length === 0 ? (
                                        <tr><td className="px-3 py-6 text-slate-500" colSpan={7}>No mismatches found.</td></tr>
                                    ) : (
                                        (auditItems || []).map((it) => (
                                            <tr key={it.bet_id} className="hover:bg-slate-800/40">
                                                <td className="px-3 py-2 font-mono text-[11px] text-slate-300 whitespace-nowrap">{it.date}</td>
                                                <td className="px-3 py-2 text-slate-200">{it.matchup || '-'}{it.is_bonus ? <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded border border-yellow-800 bg-yellow-900/30 text-yellow-200">BONUS</span> : null}</td>
                                                <td className="px-3 py-2 text-slate-400">{it.provider || '-'}</td>
                                                <td className="px-3 py-2 text-slate-400">{it.bet_type || '-'}</td>
                                                <td className="px-3 py-2">
                                                    <span className="text-[10px] px-2 py-1 rounded border border-slate-700 bg-slate-950 text-slate-200 font-bold">{it.sport}</span>
                                                </td>
                                                <td className="px-3 py-2">
                                                    <span className="text-[10px] px-2 py-1 rounded border border-amber-900/40 bg-amber-900/20 text-amber-200 font-bold">{it.suggested_sport}</span>
                                                </td>
                                                <td className="px-3 py-2">
                                                    <button
                                                        type="button"
                                                        onClick={() => applyAuditFix(it)}
                                                        className="text-[11px] px-2 py-1 rounded border border-green-900/40 bg-green-900/20 text-green-200 hover:bg-green-900/35"
                                                    >
                                                        Apply
                                                    </button>
                                                </td>
                                            </tr>
                                        ))
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            )}

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

                        <div className="mt-5 flex items-center justify-between">
                            <button
                                type="button"
                                className="px-3 py-2 bg-red-900/30 hover:bg-red-900/60 text-red-400 hover:text-red-300 rounded-lg text-sm font-bold border border-red-900/40 flex items-center gap-1.5 transition"
                                onClick={() => { handleDelete(editBet.id); setShowEdit(false); setEditBet(null); }}
                                disabled={isUpdating}
                            >
                                <Trash size={14} /> Delete
                            </button>
                            <div className="flex items-center gap-3">
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




            <div className="glass-card rounded-2xl overflow-hidden shadow-2xl relative">
                {/* Subtle top gradient line */}
                <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-green-500/30 to-transparent" />

                {/* Toolbar / Summary */}
                <div className="p-4 border-b border-gray-800 flex justify-between items-center bg-gray-900/50 backdrop-blur min-h-[72px]">
                    <div className="flex items-center gap-4">
                        <div className="text-gray-400 text-sm">
                            Showing <span className="text-white font-bold">{filtered.length}</span> of {bets.length} transactions
                        </div>
                        {!loading && bets.length > 0 && (
                            <div className="h-10 w-32 hidden md:block opacity-80 hover:opacity-100 transition-opacity">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={sevenDayData}>
                                        <defs>
                                            <linearGradient id="trendGradient" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor={sevenDayData[6]?.profit >= 0 ? "#4ade80" : "#f87171"} stopOpacity={0.3} />
                                                <stop offset="95%" stopColor={sevenDayData[6]?.profit >= 0 ? "#4ade80" : "#f87171"} stopOpacity={0} />
                                            </linearGradient>
                                        </defs>

                                        <Tooltip
                                            contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px' }}
                                            itemStyle={{ color: '#e2e8f0', fontWeight: 'bold' }}
                                            labelStyle={{ color: '#94a3b8' }}
                                            formatter={(val) => [formatCurrency(Number(val) || 0), '7d cumulative']}
                                            labelFormatter={(label) => `Day: ${label}`}
                                        />

                                        <Area
                                            type="monotone"
                                            dataKey="profit"
                                            stroke={sevenDayData[6]?.profit >= 0 ? "#4ade80" : "#f87171"}
                                            strokeWidth={2}
                                            fillOpacity={1}
                                            fill="url(#trendGradient)"
                                            isAnimationActive={false}
                                        />
                                    </AreaChart>
                                </ResponsiveContainer>
                                <div className="text-[8px] text-gray-500 font-bold uppercase tracking-tighter text-center -mt-1">
                                    {(() => {
                                        const start = Number(sevenDayData?.[0]?.profit || 0);
                                        const end = Number(sevenDayData?.[6]?.profit || 0);
                                        const delta = end - start;
                                        const dcls = delta >= 0 ? 'text-green-400' : 'text-red-400';
                                        return (
                                            <span>
                                                7D Trend • End <span className="text-slate-200">{formatCurrency(end)}</span> • Δ <span className={dcls}>{formatCurrency(delta)}</span>
                                            </span>
                                        );
                                    })()}
                                </div>
                            </div>
                        )}
                    </div>
                    <div className="flex items-center gap-2">
                        <>
                            <button
                                onClick={() => setShowManualAdd(true)}
                                className="text-xs text-green-300 hover:text-green-200 font-medium px-3 py-1.5 rounded-lg border border-green-900/40 hover:bg-green-900/20 transition"
                            >
                                + Add Bet Slip
                            </button>

                            <button
                                onClick={runAudit}
                                disabled={auditLoading}
                                className={`text-xs font-medium px-3 py-1.5 rounded-lg border transition ${auditLoading ? 'text-gray-500 border-gray-800 bg-gray-900/40 animate-pulse' : 'text-amber-300 hover:text-amber-200 border-amber-900/40 hover:bg-amber-900/20'}`}
                                title="Scan for bets whose sport looks wrong (based on matching teams to events)"
                            >
                                {auditLoading ? 'Auditing…' : 'Audit sport'}
                            </button>

                            <button
                                onClick={resetFilters}
                                className="text-xs text-blue-400 hover:text-blue-300 font-medium px-3 py-1.5 rounded-lg border border-blue-900/30 hover:bg-blue-900/20 transition"
                            >
                                Clear Filters
                            </button>
                        </>
                    </div>
                </div>

                {/* Grid */}

                {(() => {
                    const betRows = (sortedBets || []).filter(b => (b.category || '') !== 'Transaction');
                    const sum = betRows.reduce((acc, b) => acc + (Number(b.profit) || 0), 0);
                    return (
                        <div className="px-4 py-3 border-b border-gray-800 bg-gray-900/40 flex items-center justify-between text-xs backdrop-blur-sm">
                            <div className="text-gray-500 font-medium">Filtered: <span className="text-green-400 font-bold">{betRows.length}</span></div>
                            <div className="text-gray-500 font-medium">Σ Profit/Loss: <span className={`font-mono font-bold ${sum >= 0 ? 'text-green-400' : 'text-red-400'}`}>{formatCurrency(sum)}</span></div>
                        </div>
                    );
                })()}

                <div className="scrollbar-refined overflow-x-auto">
                    <table className="w-full text-left text-xs table-fixed border-separate border-spacing-0">
                        <thead className="bg-gray-800 text-gray-400 font-medium uppercase text-xs tracking-wider">
                            {/* Header Labels */}
                            <tr className="bg-gray-900/20 backdrop-blur-sm">
                                <th className="px-3 py-3 border-b border-gray-700/50 cursor-pointer hover:bg-gray-800/50 select-none w-[84px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('date')}>Date{getSortIcon('date')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 cursor-pointer hover:bg-gray-800/50 select-none w-[80px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('provider')}>Book{getSortIcon('provider')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 cursor-pointer hover:bg-gray-800/50 select-none w-[64px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('sport')}>Sport{getSortIcon('sport')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 cursor-pointer hover:bg-gray-800/50 select-none w-[70px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('bet_type')}>Type{getSortIcon('bet_type')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 select-none w-[180px] text-gray-500 font-black uppercase tracking-tighter">Event</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 cursor-pointer hover:bg-gray-800/50 select-none w-[200px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('selection')}>Selection{getSortIcon('selection')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 text-right cursor-pointer hover:bg-gray-800/50 select-none w-[60px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('odds')}>Odds{getSortIcon('odds')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 text-right cursor-pointer hover:bg-gray-800/50 select-none w-[70px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('wager')}>Wager{getSortIcon('wager')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 text-center cursor-pointer hover:bg-gray-800/50 select-none w-[64px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('status')}>Status{getSortIcon('status')}</th>
                                <th className="px-3 py-3 border-b border-gray-700/50 text-right cursor-pointer hover:bg-gray-800/50 select-none w-[76px] text-gray-500 font-black uppercase tracking-tighter" onClick={() => requestSort('profit')}>P/L{getSortIcon('profit')}</th>
                            </tr>
                            {/* Filter Row */}
                            <tr className="bg-gray-850">
                                <th className="px-1 py-1">
                                    <input
                                        type="text"
                                        placeholder="Date"
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.date}
                                        onChange={e => setFilters({ ...filters, date: e.target.value })}
                                    />
                                </th>
                                <th className="px-1 py-1">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.sportsbook}
                                        onChange={e => setFilters({ ...filters, sportsbook: e.target.value })}
                                    >
                                        <option value="All">All</option>
                                        {sportsbooks.filter(s => s !== 'All').map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-1 py-1">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.sport}
                                        onChange={e => setFilters({ ...filters, sport: e.target.value })}
                                    >
                                        <option value="All">All</option>
                                        {sports.filter(s => s !== 'All').map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-1 py-1">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.type}
                                        onChange={e => setFilters({ ...filters, type: e.target.value })}
                                    >
                                        {types.map(t => <option key={t} value={t}>{t}</option>)}
                                    </select>
                                </th>
                                <th className="px-1 py-1">
                                    <input
                                        type="text"
                                        placeholder="Search..."
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.selection}
                                        onChange={e => setFilters({ ...filters, selection: e.target.value })}
                                    />
                                </th>
                                <th className="px-1 py-1"></th>
                                <th className="px-1 py-1"></th>
                                <th className="px-1 py-1"></th>
                                <th className="px-1 py-1">
                                    <select
                                        className="w-full bg-gray-900 border border-gray-700 rounded px-1 py-0.5 text-[10px] text-white focus:border-blue-500 outline-none"
                                        value={filters.status}
                                        onChange={e => setFilters({ ...filters, status: e.target.value })}
                                    >
                                        {statuses.map(s => <option key={s} value={s}>{s}</option>)}
                                    </select>
                                </th>
                                <th className="px-1 py-1"></th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-800">
                            {loading ? (
                                Array(10).fill(0).map((_, i) => <SkeletonRow key={i} />)
                            ) : sortedBets.map((bet) => {
                                const isTxn = (bet.category === 'Transaction');
                                const isDeposit = isTxn && (Number(bet.wager) < 0 || (bet.description || '').toLowerCase().includes('deposit'));
                                const isSelected = false;
                                return (
                                    <tr
                                        key={bet.id || bet.txn_id}
                                        className={`transition duration-150 cursor-pointer border-b border-gray-800/30 ${isSelected ? 'bg-blue-500/10 hover:bg-blue-500/20' : 'hover:bg-gray-800/40'}`}
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
                                        <td
                                            className="px-3 py-3 text-gray-400 font-mono text-[10px] whitespace-nowrap opacity-60"
                                            title={formatDateMDY(bet.sort_date || bet.date)}
                                        >
                                            {formatDateMDY(bet.sort_date || bet.date)}
                                        </td>
                                        <td className="px-3 py-3">
                                            <span className={`text-[10px] px-2 py-0.5 rounded font-black tracking-tighter uppercase border transition-colors duration-200 ${bet.provider === 'DraftKings' ? "bg-green-500/10 text-green-400 border-green-500/20" :
                                                bet.provider === 'FanDuel' ? "bg-blue-500/10 text-blue-400 border-blue-500/20" :
                                                    "bg-gray-800/40 text-gray-400 border-gray-700/50"
                                                }`}>
                                                {bet.provider}
                                            </span>
                                        </td>
                                        <td className="px-3 py-3">
                                            <span className="text-[10px] text-gray-300 uppercase font-black tracking-widest opacity-80">
                                                {bet.sport}
                                            </span>
                                        </td>
                                        <td className="px-3 py-3 text-gray-500 text-[10px] font-medium tracking-tight whitespace-nowrap overflow-hidden">{bet.bet_type}</td>

                                        <td className="px-3 py-3 text-gray-400 text-[11px] font-medium tracking-tight truncate" title={extractEvent(bet)}>
                                            {extractEvent(bet) || '-'}
                                        </td>

                                        <td className="px-3 py-3 truncate text-gray-200 text-xs font-bold tracking-tight" title={bet.selection || bet.description}>
                                            {(() => {
                                                // Keep Selection column concise: just team(s) + bet, not slip metadata.
                                                const raw = bet.display_selection || bet.selection || bet.description || '';
                                                let s = String(raw);
                                                // Strip slip/settlement details that belong in other columns
                                                ['Wager:', 'Paid:', 'Payout:', 'Final Score', 'Cash Out:', 'Potential Payout:'].forEach((tok) => {
                                                    if (s.includes(tok)) s = s.split(tok)[0];
                                                });
                                                s = s.replace(/\s+/g, ' ').trim();
                                                if (s.length > 80) s = s.slice(0, 77) + '...';
                                                return s;
                                            })()}
                                            {bet.is_live && <span className="ml-2 text-[9px] bg-red-900/50 text-red-300 px-1 rounded border border-red-800">LIVE</span>}
                                            {bet.is_bonus && <span className="ml-2 text-[9px] bg-yellow-900/50 text-yellow-300 px-1 rounded border border-yellow-800">BONUS</span>}
                                        </td>
                                        <td className="px-3 py-3 text-right font-mono text-gray-400 text-[11px] whitespace-nowrap">
                                            {!isTxn ? (bet.odds ? (bet.odds > 0 ? `+${bet.odds}` : bet.odds) : '-') : '-'}
                                        </td>
                                        <td className={`px-3 py-3 text-right font-medium text-[11px] whitespace-nowrap ${isTxn ? 'text-gray-400' : 'text-gray-300'}`}>
                                            {formatCurrency(bet.wager)}
                                        </td>
                                        <td className="px-3 py-3 text-center">
                                            <span className={`px-2 py-1 rounded text-[10px] font-black tracking-widest border transition-all duration-300 ${isTxn ? (isDeposit ? 'bg-green-500/10 text-green-400 border-green-500/20' : 'bg-gray-500/10 text-gray-400 border-gray-500/20') :
                                                ['WON', 'WIN'].includes(bet.status) ? 'bg-green-500/10 text-green-400 border-green-500/20 shadow-[0_0_15px_rgba(34,197,94,0.1)]' :
                                                    ['LOST', 'LOSE'].includes(bet.status) ? 'bg-red-500/10 text-red-400 border-red-500/20' :
                                                        bet.status === 'PUSH' ? 'bg-gray-500/10 text-gray-400 border-gray-500/20' :
                                                            'bg-blue-500/10 text-blue-400 border-blue-500/20'
                                                }`}>
                                                {isTxn ? (isDeposit ? 'DEP' : 'WDR') : bet.status}
                                            </span>
                                        </td>
                                        <td className={`px-3 py-3 text-right font-bold text-[11px] whitespace-nowrap ${bet.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {(bet.profit !== undefined && bet.profit !== null) ? (bet.profit >= 0 ? '+' : '') + formatCurrency(bet.profit) : '-'}
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
