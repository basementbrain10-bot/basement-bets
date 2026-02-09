const ModelPerformanceAnalytics = ({ history }) => {
    // Schema Migration: use graded_result (new) or outcome or result
    const getResult = (h) => h.graded_result || h.outcome || h.result;

    const graded = history.filter(h => {
        const res = getResult(h);
        return res && res !== 'PENDING' && res !== 'Pending';
    });
    const wins = graded.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
    const losses = graded.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
    const pushes = graded.filter(h => getResult(h) === 'PUSH' || getResult(h) === 'Push').length;
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
    const getConfidenceLabel = (h) => {
        // prefer explicit label fields
        const explicit = h?.confidence_label || h?.confidenceLevel || h?.confidence_level;
        if (explicit) return String(explicit);

        // try outputs_json
        try {
            if (h?.outputs_json) {
                const out = JSON.parse(h.outputs_json);
                const c = out?.confidence_label || out?.confidenceLevel || out?.confidence;
                if (c !== undefined && c !== null && c !== '') {
                    // if numeric, bucket below
                    if (typeof c === 'string' && isNaN(Number(c))) return String(c);
                    const n = Number(c);
                    if (Number.isFinite(n)) {
                        if (n >= 0.75) return 'High';
                        if (n >= 0.6) return 'Medium';
                        return 'Low';
                    }
                    return String(c);
                }
            }
        } catch (e) {}

        const n = Number(h?.confidence);
        if (Number.isFinite(n)) {
            if (n >= 0.75) return 'High';
            if (n >= 0.6) return 'Medium';
            return 'Low';
        }

        return '—';
    };

    const confidenceLevels = ['High', 'Medium', 'Low'];
    const confidencePerformance = confidenceLevels.map(level => {
        const filtered = graded.filter(h => getConfidenceLabel(h) === level);
        const w = filtered.filter(h => getResult(h) === 'WON' || getResult(h) === 'Win').length;
        const l = filtered.filter(h => getResult(h) === 'LOST' || getResult(h) === 'Loss').length;
        const wr = filtered.length > 0 ? (w / (w + l) * 100) : 0;
        const r = filtered.length > 0 ? ((w * 9.09 - l * 10) / (filtered.length * 10) * 100) : 0;
        return { level, count: filtered.length, wins: w, losses: l, winRate: wr, roi: r };
    }).filter(x => x.count > 0);

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
                    <div className="space-y-2 text-xs">
                        {edgePerformance.map(edge => (
                            <div key={edge.threshold} className="flex justify-between items-center">
                                <span className="text-slate-400">
                                    Edge ≥ {edgeMode === 'ev' ? `${(edge.threshold * 100).toFixed(0)}%` : `${edge.threshold} pts`}:
                                </span>
                                <div className="flex items-center space-x-2">
                                    <span className="text-slate-500">{edge.wins}-{edge.losses}</span>
                                    <span className={`font-bold ${edge.winRate >= 55 ? 'text-green-400' : edge.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                        {edge.winRate.toFixed(0)}%
                                    </span>
                                    <span className={`text-xs ${edge.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                        ({edge.roi >= 0 ? '+' : ''}{edge.roi.toFixed(0)}%)
                                    </span>
                                </div>
                            </div>
                        ))}
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
                    <div className="space-y-2 text-xs">
                        {confidencePerformance.length === 0 && (
                            <div className="text-slate-500">No confidence labels found.</div>
                        )}
                        {confidencePerformance.map(c => (
                            <div key={c.level} className="flex justify-between items-center">
                                <span className="text-slate-400">{c.level}:</span>
                                <div className="flex items-center space-x-2">
                                    <span className="text-slate-500">{c.wins}-{c.losses}</span>
                                    <span className={`font-bold ${c.winRate >= 55 ? 'text-green-400' : c.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                        {c.winRate.toFixed(0)}%
                                    </span>
                                    <span className={`text-xs ${c.roi >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                        ({c.roi >= 0 ? '+' : ''}{c.roi.toFixed(0)}%)
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Performance by Sport & Market */}
                <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
                    <h4 className="text-sm font-bold text-slate-400 uppercase mb-3">By Sport & Market</h4>
                    <div className="space-y-3">
                        {sportPerformance.map(sport => (
                            <div key={sport.sport} className="text-xs">
                                <div className="flex justify-between items-center mb-1">
                                    <span className="text-white font-bold">{sport.sport}</span>
                                    <span className={`font-bold ${sport.winRate >= 55 ? 'text-green-400' : sport.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                        {sport.winRate.toFixed(0)}%
                                    </span>
                                </div>
                                <div className="text-slate-500">{sport.wins}-{sport.losses} ({sport.roi >= 0 ? '+' : ''}{sport.roi.toFixed(1)}% ROI)</div>
                            </div>
                        ))}
                        {marketPerformance.length > 0 && (
                            <>
                                <div className="border-t border-slate-700 pt-2 mt-2"></div>
                                {marketPerformance.map(market => (
                                    <div key={market.market} className="text-xs">
                                        <div className="flex justify-between items-center">
                                            <span className="text-slate-400">{market.market}:</span>
                                            <span className={`font-bold ${market.winRate >= 55 ? 'text-green-400' : market.winRate >= 50 ? 'text-yellow-400' : 'text-red-400'}`}>
                                                {market.wins}-{market.losses} ({market.winRate.toFixed(0)}%)
                                            </span>
                                        </div>
                                    </div>
                                ))}
                            </>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default ModelPerformanceAnalytics;
