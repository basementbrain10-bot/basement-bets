const ModelPerformanceAnalytics = ({ history }) => {
    // Schema Migration: use graded_result (new) or outcome or result
    const getResult = (h) => h.graded_result || h.outcome || h.result;

    const isGradedResult = (res) => {
        if (!res) return false;
        const s = String(res).toUpperCase();
        return s === 'WON' || s === 'WIN' || s === 'LOST' || s === 'LOSS' || s === 'PUSH';
    };

    const graded = history.filter(h => isGradedResult(getResult(h)));
    const wins = graded.filter(h => {
        const s = String(getResult(h)).toUpperCase();
        return s === 'WON' || s === 'WIN';
    }).length;
    const losses = graded.filter(h => {
        const s = String(getResult(h)).toUpperCase();
        return s === 'LOST' || s === 'LOSS';
    }).length;
    const pushes = graded.filter(h => String(getResult(h)).toUpperCase() === 'PUSH').length;
    const winRate = graded.length > 0 ? (wins / (wins + losses) * 100) : 0;
    const roi = graded.length > 0 ? ((wins * 9.09 - losses * 10) / (graded.length * 10) * 100) : 0;

    // CLV Metrics
    const clvData = history.filter(h => h.clv_points !== null && h.clv_points !== undefined);
    const avgClv = clvData.length > 0 ? (clvData.reduce((a, b) => a + (b.clv_points || 0), 0) / clvData.length) : 0;
    const posClv = clvData.filter(h => (h.clv_points || 0) > 0).length;
    const posClvRate = clvData.length > 0 ? (posClv / clvData.length * 100) : 0;

    // Performance by edge threshold
    // Data has evolved over time; edge can be stored as:
    // - h.edge (points or % depending on sport/model)
    // - h.edge_points
    // - h.ev_per_unit (decimal EV, e.g. 0.04 = 4%)
    const getEdge = (h) => {
        const raw = h?.edge ?? h?.edge_points;
        const n = Number(raw);
        if (Number.isFinite(n)) return n;
        const ev = Number(h?.ev_per_unit ?? h?.ev);
        if (Number.isFinite(ev)) return ev; // keep in decimal units
        return null;
    };

    const edgeVals = graded.map(getEdge).filter(v => v !== null && v !== undefined && Number.isFinite(v));
    const maxAbs = edgeVals.length ? Math.max(...edgeVals.map(v => Math.abs(v))) : 0;

    // Heuristic: if edges are mostly in [0,1], treat as EV decimals; else treat as point edges.
    const edgeMode = maxAbs <= 1.0 ? 'ev' : 'points';
    const edgeThresholds = edgeMode === 'ev'
        ? [0.01, 0.02, 0.03, 0.05, 0.08]
        : [0.5, 1.0, 2.0, 3.0, 5.0];

    const edgePerformance = edgeThresholds.map(threshold => {
        const filtered = graded.filter(h => {
            const e = getEdge(h);
            if (!Number.isFinite(e)) return false;
            return e >= threshold;
        });
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const wr = filtered.length > 0 ? (w / (w + l) * 100) : 0;
        const r = filtered.length > 0 ? ((w * 9.09 - l * 10) / (filtered.length * 10) * 100) : 0;
        return { threshold, count: filtered.length, wins: w, losses: l, winRate: wr, roi: r };
    }).filter(e => e.count > 0);

    // Performance by sport
    const getSport = (h) => h?.sport || h?.league;
    const sports = [...new Set(graded.map(getSport).filter(Boolean))];
    const sportPerformance = sports.map(sport => {
        const filtered = graded.filter(h => getSport(h) === sport);
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const wr = filtered.length > 0 ? (w / (w + l) * 100) : 0;
        const r = filtered.length > 0 ? ((w * 9.09 - l * 10) / (filtered.length * 10) * 100) : 0;
        return { sport, count: filtered.length, wins: w, losses: l, winRate: wr, roi: r };
    });

    // Performance by market type
    const getMarket = (h) => h?.market || h?.market_type;
    const markets = [...new Set(graded.map(getMarket).filter(Boolean))];
    const marketPerformance = markets.map(market => {
        const filtered = graded.filter(h => getMarket(h) === market);
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const wr = filtered.length > 0 ? (w / (w + l) * 100) : 0;
        const r = filtered.length > 0 ? ((w * 9.09 - l * 10) / (filtered.length * 10) * 100) : 0;
        return { market, count: filtered.length, wins: w, losses: l, winRate: wr, roi: r };
    });

    // Performance by confidence level
    // Confidence should always be attached to the *recommended bet*. Depending on schema version,
    // it may live on the row, inside outputs_json, or inside recommendation_json.
    const normalizeConfidence = (val) => {
        // numeric confidence
        const asNum = Number(val);
        if (Number.isFinite(asNum)) {
            if (asNum >= 0.75) return 'High';
            if (asNum >= 0.6) return 'Medium';
            return 'Low';
        }

        const s = String(val ?? '').trim();
        const up = s.toUpperCase();
        if (!up || up === '—') return null;
        if (up === 'H' || up.startsWith('HIGH')) return 'High';
        if (up === 'M' || up.startsWith('MED')) return 'Medium';
        if (up === 'L' || up.startsWith('LOW')) return 'Low';
        return null;
    };

    const inferConfidenceFromEv = (h) => {
        // Bucket by EV/u using the SAME thresholds as the model UI labels:
        // confidence = High if ev*100*5 > 80  => ev > 0.16
        // confidence = Medium if ev*100*5 > 50 => ev > 0.10
        const ev = Number(h?.ev_per_unit ?? h?.ev);
        if (!Number.isFinite(ev)) return 'Low';
        if (ev >= 0.16) return 'High';
        if (ev >= 0.10) return 'Medium';
        return 'Low';
    };

    const getConfidenceLabel = (h) => {
        // 1) explicit row-level fields
        const explicit = h?.confidence_label || h?.confidenceLevel || h?.confidence_level || h?.confidence;
        const n1 = normalizeConfidence(explicit);
        if (n1) return n1;

        // 2) outputs_json: { confidence_label } or { recommendations: [{confidence_level/confidence_label/confidence}] }
        try {
            if (h?.outputs_json) {
                const out = JSON.parse(h.outputs_json);
                const n2 = normalizeConfidence(out?.confidence_label || out?.confidenceLevel || out?.confidence);
                if (n2) return n2;
                const rec = Array.isArray(out?.recommendations) ? out.recommendations[0] : null;
                const n2b = normalizeConfidence(rec?.confidence_level || rec?.confidence_label || rec?.confidence);
                if (n2b) return n2b;
            }
        } catch (e) {}

        // 3) recommendation_json: legacy recommendations array
        try {
            if (h?.recommendation_json) {
                const recs = JSON.parse(h.recommendation_json);
                const rec = Array.isArray(recs) ? recs[0] : recs;
                const n3 = normalizeConfidence(rec?.confidence_level || rec?.confidence_label || rec?.confidence);
                if (n3) return n3;
            }
        } catch (e) {}

        // 4) last resort: infer from EV
        return inferConfidenceFromEv(h);
    };

    const confidenceLevels = ['High', 'Medium', 'Low'];
    const confidencePerformance = confidenceLevels.map(level => {
        const filtered = graded.filter(h => getConfidenceLabel(h) === level);
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const wr = (w + l) > 0 ? (w / (w + l) * 100) : 0;
        const r = filtered.length > 0 ? ((w * 9.09 - l * 10) / (filtered.length * 10) * 100) : 0;
        return { level, count: filtered.length, wins: w, losses: l, winRate: wr, roi: r };
    });

    const toEtDay = (ts) => {
        if (!ts) return null;
        try {
            const d = new Date(ts);
            const s = d.toLocaleDateString('en-US', { timeZone: 'America/New_York' });
            return s || null;
        } catch (e) {
            return null;
        }
    };

    const fmtEtDayShort = (ts) => {
        const day = toEtDay(ts);
        if (!day) return '—';
        // day is like M/D/YYYY
        try {
            const parts = day.split('/');
            if (parts.length === 3) {
                const mm = String(parts[0]).padStart(2, '0');
                const dd = String(parts[1]).padStart(2, '0');
                return `${mm}/${dd}`;
            }
        } catch (e) {}
        return day;
    };

    const isWithinDays = (h, days) => {
        const ts = h?.analyzed_at || h?.created_at;
        if (!ts) return false;
        try {
            const t = new Date(ts).getTime();
            if (!Number.isFinite(t)) return false;
            const cutoff = Date.now() - (days * 24 * 60 * 60 * 1000);
            return t >= cutoff;
        } catch (e) {
            return false;
        }
    };

    const isYesterdayET = (h) => {
        const ts = h?.analyzed_at || h?.created_at;
        const day = toEtDay(ts);
        if (!day) return false;
        const now = new Date();
        const y = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const yday = y.toLocaleDateString('en-US', { timeZone: 'America/New_York' });
        return day === yday;
    };

    const confidenceTrend = (predicateFn) => confidenceLevels.map(level => {
        const filtered = graded
            .filter(h => getConfidenceLabel(h) === level)
            .filter(predicateFn);
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const p = filtered.filter(h => getResult(h) === 'PUSH' || getResult(h) === 'Push').length;
        const decided = w + l;
        const wr = decided > 0 ? (w / decided * 100) : 0;
        return { level, count: filtered.length, wins: w, losses: l, pushes: p, winRate: wr };
    });

    // --- Top 6 (by EV) subset (used for daily tracking) ---
    const topN = 6;
    const getEv = (h) => {
        const ev = Number(h?.ev_per_unit ?? h?.ev);
        return Number.isFinite(ev) ? ev : 0;
    };

    const dailyTopN = (() => {
        // Group all recommended+graded bets by ET day of analyzed_at.
        const groups = {};
        for (const h of graded) {
            const day = toEtDay(h?.analyzed_at || h?.created_at);
            if (!day) continue;
            if (!groups[day]) groups[day] = [];
            groups[day].push(h);
        }

        const days = Object.keys(groups).sort((a, b) => {
            const da = new Date(a).getTime();
            const db = new Date(b).getTime();
            if (Number.isFinite(da) && Number.isFinite(db)) return db - da;
            return String(b).localeCompare(String(a));
        });

        const out = [];
        for (const day of days) {
            const bets = (groups[day] || [])
                .slice()
                .sort((x, y) => getEv(y) - getEv(x))
                .slice(0, topN);

            const w = bets.filter(h => {
                const s = String(getResult(h)).toUpperCase();
                return s === 'WON' || s === 'WIN';
            }).length;
            const l = bets.filter(h => {
                const s = String(getResult(h)).toUpperCase();
                return s === 'LOST' || s === 'LOSS';
            }).length;
            const p = bets.filter(h => String(getResult(h)).toUpperCase() === 'PUSH').length;
            const decided = w + l;
            const wr = decided > 0 ? (w / decided * 100) : 0;
            const roi = bets.length > 0 ? ((w * 9.09 - l * 10) / (bets.length * 10) * 100) : 0;

            out.push({ day, bets, count: bets.length, wins: w, losses: l, pushes: p, winRate: wr, roi });
        }
        return out;
    })();

    const dailyTopN7 = dailyTopN.slice(0, 7);

    // Trends by confidence (ALL graded recommended bets)
    const trendYday = confidenceTrend((h) => isYesterdayET(h));
    const trend3 = confidenceTrend((h) => isWithinDays(h, 3));
    const trend7 = confidenceTrend((h) => isWithinDays(h, 7));
    const trend30 = confidenceTrend((h) => isWithinDays(h, 30));

    const sum = (xs) => (xs || []).reduce((a, b) => a + (Number(b) || 0), 0);

    // Sanity checks: these should add up to graded.length.
    const confidenceCountSum = sum(confidencePerformance.map(x => x.count));
    const sportCountSum = sum(sportPerformance.map(x => x.count));
    const marketCountSum = sum(marketPerformance.map(x => x.count));

    const sanity = {
        graded: graded.length,
        confidence: confidenceCountSum,
        sport: sportCountSum,
        market: marketCountSum,
    };

    if (graded.length === 0) return null;

    return (
        <div className="bg-slate-800/50 rounded-xl p-6 border border-slate-700 mb-6">
            <h3 className="text-lg font-bold text-white mb-4 flex items-center">
                📊 Model Performance Analytics
            </h3>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Overall Stats */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">Overall Performance</h4>
                    <div className="space-y-2">
                        <div className="flex justify-between">
                            <span className="text-slate-400">Record:</span>
                            <span className="text-white font-bold">{wins}-{losses}-{pushes}</span>
                        </div>
                        <div className="flex justify-between">
                            <span className="text-slate-400">Win Rate:</span>
                            <span className={`font-bold ${winRate >= 55 ? 'text-green-400' : winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                {winRate.toFixed(1)}%
                            </span>
                        </div>
                        <div className="flex justify-between">
                            <span className="text-slate-400">ROI:</span>
                            <span className={`font-bold ${roi >= 5 ? 'text-green-400' : roi >= 0 ? 'text-yellow-400' : 'text-red-400'}`}>
                                {roi >= 0 ? '+' : ''}{roi.toFixed(1)}%
                            </span>
                        </div>
                        <div className="flex justify-between">
                            <span className="text-slate-400">Total Bets:</span>
                            <span className="text-white font-bold">{graded.length}</span>
                        </div>
                    </div>
                </div>

                {/* Performance by Edge */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">Performance by Edge</h4>

                    <div className="text-[10px] text-slate-500 mb-2">
                        Thresholds are {edgeMode === 'ev' ? 'EV%' : 'edge points'}. Rows are cumulative (Edge ≥ threshold).
                    </div>

                    <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-700">
                                    <th className="py-1 pr-2 text-left">Edge ≥</th>
                                    <th className="py-1 px-2 text-right">N</th>
                                    <th className="py-1 px-2 text-right">W-L</th>
                                    <th className="py-1 px-2 text-right">Win%</th>
                                    <th className="py-1 pl-2 text-right">ROI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {edgePerformance.map(edge => (
                                    <tr key={edge.threshold} className="border-b border-slate-800/60 last:border-0">
                                        <td className="py-1 pr-2 text-slate-300">
                                            {edgeMode === 'ev' ? `${(edge.threshold * 100).toFixed(0)}%` : `${edge.threshold} pts`}
                                        </td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{edge.count}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{edge.wins}-{edge.losses}</td>
                                        <td className={`py-1 px-2 text-right font-bold ${edge.winRate >= 55 ? 'text-green-400' : edge.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                            {edge.winRate.toFixed(0)}%
                                        </td>
                                        <td className={`py-1 pl-2 text-right font-mono ${edge.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {edge.roi >= 0 ? '+' : ''}{edge.roi.toFixed(0)}%
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>

                {/* CLV Stats */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">CLV Analysis</h4>
                    <div className="space-y-2">
                        <div className="flex justify-between">
                            <span className="text-slate-400">Avg CLV:</span>
                            <span className={`font-bold ${avgClv > 0 ? 'text-green-400' : avgClv < 0 ? 'text-red-400' : 'text-slate-200'}`}>
                                {avgClv > 0 ? '+' : ''}{avgClv.toFixed(2)} pts
                            </span>
                        </div>
                        <div className="flex justify-between">
                            <span className="text-slate-400">Positive CLV %:</span>
                            <span className="text-white font-bold">{posClvRate.toFixed(1)}%</span>
                        </div>
                        <div className="text-[10px] text-slate-500 mt-2">
                            *CLV = Closing Line Value (Pts vs Market Close)
                        </div>
                    </div>
                </div>

                {/* Performance by Confidence */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">By Confidence</h4>

                    <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-700">
                                    <th className="py-1 pr-2 text-left">Level</th>
                                    <th className="py-1 px-2 text-right">N</th>
                                    <th className="py-1 px-2 text-right">W-L</th>
                                    <th className="py-1 px-2 text-right">Win%</th>
                                    <th className="py-1 pl-2 text-right">ROI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {confidencePerformance.map(c => (
                                    <tr key={c.level} className="border-b border-slate-800/60 last:border-0">
                                        <td className="py-1 pr-2 text-slate-300 font-bold">{c.level}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{c.count}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{c.wins}-{c.losses}</td>
                                        <td className={`py-1 px-2 text-right font-bold ${c.winRate >= 55 ? 'text-green-400' : c.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                            {c.winRate.toFixed(0)}%
                                        </td>
                                        <td className={`py-1 pl-2 text-right font-mono ${c.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {c.roi >= 0 ? '+' : ''}{c.roi.toFixed(0)}%
                                        </td>
                                    </tr>
                                ))}
                                <tr className="border-t border-slate-700">
                                    <td className="py-1 pr-2 text-slate-500 font-black">TOTAL</td>
                                    <td className="py-1 px-2 text-right text-slate-200 font-black font-mono">{confidenceCountSum}</td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 pl-2"></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <div className="mt-4 pt-3 border-t border-slate-700">
                        <div className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2">Trend by confidence (all graded bets)</div>
                        <div className="grid grid-cols-2 gap-3 text-[11px]">
                            <div className="bg-slate-950/30 rounded-md p-2 border border-slate-800">
                                <div className="text-[10px] text-slate-500 font-bold mb-1">Yesterday</div>
                                {trendYday.map(t => (
                                    <div key={t.level} className="flex justify-between">
                                        <span className="text-slate-400">{t.level}</span>
                                        <span className="text-slate-300">{t.wins}-{t.losses}{t.pushes ? `-${t.pushes}` : ''} ({t.winRate.toFixed(0)}%)</span>
                                    </div>
                                ))}
                            </div>
                            <div className="bg-slate-950/30 rounded-md p-2 border border-slate-800">
                                <div className="text-[10px] text-slate-500 font-bold mb-1">Last 3 days</div>
                                {trend3.map(t => (
                                    <div key={t.level} className="flex justify-between">
                                        <span className="text-slate-400">{t.level}</span>
                                        <span className="text-slate-300">{t.wins}-{t.losses}{t.pushes ? `-${t.pushes}` : ''} ({t.winRate.toFixed(0)}%)</span>
                                    </div>
                                ))}
                            </div>
                            <div className="bg-slate-950/30 rounded-md p-2 border border-slate-800">
                                <div className="text-[10px] text-slate-500 font-bold mb-1">Last 7 days</div>
                                {trend7.map(t => (
                                    <div key={t.level} className="flex justify-between">
                                        <span className="text-slate-400">{t.level}</span>
                                        <span className="text-slate-300">{t.wins}-{t.losses}{t.pushes ? `-${t.pushes}` : ''} ({t.winRate.toFixed(0)}%)</span>
                                    </div>
                                ))}
                            </div>
                            <div className="bg-slate-950/30 rounded-md p-2 border border-slate-800">
                                <div className="text-[10px] text-slate-500 font-bold mb-1">Last 30 days</div>
                                {trend30.map(t => (
                                    <div key={t.level} className="flex justify-between">
                                        <span className="text-slate-400">{t.level}</span>
                                        <span className="text-slate-300">{t.wins}-{t.losses}{t.pushes ? `-${t.pushes}` : ''} ({t.winRate.toFixed(0)}%)</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                        <div className="text-[10px] text-slate-500 mt-2">*Trends include graded bets only.</div>
                    </div>
                </div>

                {/* Top 6 performance by day */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">Top 6 (by EV) — Daily performance</h4>
                    <div className="text-[10px] text-slate-500 mb-2">Each day: take the 6 highest-EV recommended bets and grade their results.</div>
                    <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-700">
                                    <th className="py-1 pr-2 text-left">Day</th>
                                    <th className="py-1 px-2 text-right">N</th>
                                    <th className="py-1 px-2 text-right">W-L-P</th>
                                    <th className="py-1 px-2 text-right">Win%</th>
                                    <th className="py-1 pl-2 text-right">ROI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {dailyTopN7.map(d => (
                                    <tr key={d.day} className="border-b border-slate-800/60 last:border-0">
                                        <td className="py-1 pr-2 text-slate-300 font-bold">{fmtEtDayShort(d.day)}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{d.count}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{d.wins}-{d.losses}{d.pushes ? `-${d.pushes}` : ''}</td>
                                        <td className={`py-1 px-2 text-right font-bold ${d.winRate >= 55 ? 'text-green-400' : d.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                            {d.winRate.toFixed(0)}%
                                        </td>
                                        <td className={`py-1 pl-2 text-right font-mono ${d.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {d.roi >= 0 ? '+' : ''}{d.roi.toFixed(0)}%
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </div>

                {/* Performance by Sport & Market */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">By Sport</h4>
                    <div className="overflow-x-auto mb-4">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-700">
                                    <th className="py-1 pr-2 text-left">Sport</th>
                                    <th className="py-1 px-2 text-right">N</th>
                                    <th className="py-1 px-2 text-right">W-L</th>
                                    <th className="py-1 px-2 text-right">Win%</th>
                                    <th className="py-1 pl-2 text-right">ROI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {sportPerformance.map(s => (
                                    <tr key={s.sport} className="border-b border-slate-800/60 last:border-0">
                                        <td className="py-1 pr-2 text-slate-300 font-bold">{s.sport}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{s.count}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{s.wins}-{s.losses}</td>
                                        <td className={`py-1 px-2 text-right font-bold ${s.winRate >= 55 ? 'text-green-400' : s.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                            {s.winRate.toFixed(0)}%
                                        </td>
                                        <td className={`py-1 pl-2 text-right font-mono ${s.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {s.roi >= 0 ? '+' : ''}{s.roi.toFixed(0)}%
                                        </td>
                                    </tr>
                                ))}
                                <tr className="border-t border-slate-700">
                                    <td className="py-1 pr-2 text-slate-500 font-black">TOTAL</td>
                                    <td className="py-1 px-2 text-right text-slate-200 font-black font-mono">{sportCountSum}</td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 pl-2"></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">By Market</h4>
                    <div className="overflow-x-auto">
                        <table className="w-full text-xs">
                            <thead>
                                <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-700">
                                    <th className="py-1 pr-2 text-left">Market</th>
                                    <th className="py-1 px-2 text-right">N</th>
                                    <th className="py-1 px-2 text-right">W-L</th>
                                    <th className="py-1 px-2 text-right">Win%</th>
                                    <th className="py-1 pl-2 text-right">ROI</th>
                                </tr>
                            </thead>
                            <tbody>
                                {marketPerformance.map(m => (
                                    <tr key={m.market} className="border-b border-slate-800/60 last:border-0">
                                        <td className="py-1 pr-2 text-slate-300 font-bold">{m.market}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{m.count}</td>
                                        <td className="py-1 px-2 text-right text-slate-400 font-mono">{m.wins}-{m.losses}</td>
                                        <td className={`py-1 px-2 text-right font-bold ${m.winRate >= 55 ? 'text-green-400' : m.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                            {m.winRate.toFixed(0)}%
                                        </td>
                                        <td className={`py-1 pl-2 text-right font-mono ${m.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                            {m.roi >= 0 ? '+' : ''}{m.roi.toFixed(0)}%
                                        </td>
                                    </tr>
                                ))}
                                <tr className="border-t border-slate-700">
                                    <td className="py-1 pr-2 text-slate-500 font-black">TOTAL</td>
                                    <td className="py-1 px-2 text-right text-slate-200 font-black font-mono">{marketCountSum}</td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 px-2"></td>
                                    <td className="py-1 pl-2"></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>

                    {/* Sanity check */}
                    <div className="mt-4 text-[10px] text-slate-500">
                        Sanity: graded={sanity.graded} • by confidence={sanity.confidence} • by sport={sanity.sport} • by market={sanity.market}
                        {(sanity.confidence !== sanity.graded || sanity.sport !== sanity.graded || sanity.market !== sanity.graded) ? (
                            <span className="text-amber-300"> (mismatch: some rows missing labels)</span>
                        ) : null}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ModelPerformanceAnalytics;
