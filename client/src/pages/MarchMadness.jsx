import React from 'react';

const templateHighlights = [
    {
        title: 'Snapshot + Rankings',
        bullets: [
            'Team, seed, conference, coach and overall/conference records are captured up front so every profile starts with the same baseline.',
            'KenPom, Torvik, NET and RPI numbers plus schedule strength (SOS, NCSOS) provide the efficiency and context metrics for the betting lens.',
            'Resume includes quadrant wins, road/neutral/home splits, recent form, streak and injury notes so the narrative always ties back to tangible data.'
        ]
    },
    {
        title: 'Playing Style & Best Players',
        bullets: [
            'Tempo, shot-profile and offensive/defensive tendencies (creation, turnovers, rebounding, fouling) explain how the team wins and what matchup types favor them.',
            'A structured best-player block adds both offense (usage + efficiency) and defense (rim protection + disruption) so the “who matters” snapshot is front and center.',
            'Weighting rubrics (efficiency, shot quality, turnovers, rebounding, tempo, personnel, best-player factor) ensure every profile ties subjective narrative back to quantifiable signals.'
        ]
    },
    {
        title: 'Matchup Lens & Betting Signals',
        bullets: [
            'Each matchup card includes directional drivers (moneyline/spread/total) plus the top risks, tempo tags and steam/variance notes so writing a read isn’t guesswork.',
            'The betting lens explicitly calls out green/red flags across ML, spread and total paths and summarizes keys to win, upset vulnerabilities and tournament fit.',
            'A quick “20-second summary” with 3 keys and 2 red flags makes it easy to remember why a profile matters during live decision-making.'
        ]
    },
    {
        title: 'Upset & Volatility Intelligence',
        bullets: [
            'Upset risk/potential scores (0–100) plus volatility flags (tempo extremes, 3P variance, turnover trouble, foul/rotation risk) highlight when a seed is vulnerable.',
            'Best-player edge sections compare the top offensive/defensive creator to typical opponents, including how they can be contained or how they tilt crunch-time possessions.',
            'The template keeps situational notes—depth, foul trouble, neutral-court travel and variance profile—in one place so nothing surprises the betting team.'
        ]
    }
];

const modelingHighlights = [
    {
        title: 'Data Sources & Blending',
        detail: 'Torvik Time Machine exports (2024 + 2025) plus KenPom preseason snapshots are merged with a DanVK seed+round historical prior to give us both efficiency and bracket-aware baselines.',
        bullets: [
            'Torvik ingest creates per-game snapshots and maintains the tempo/efficiency inputs for every matchup.',
            'KenPom adds adjO/adjD/adjT and the optional summary snapshots widen the feature set.',
            'A seed+round prior trained on 1985–2017 DanVK brackets produces `seed_prior.json`, which is blended with the efficiency model (`p_blend`).'
        ]
    },
    {
        title: 'Model Pipeline',
        detail: 'The pipeline builds a training set, trains ML/spread/total models, evaluates them and exports metrics for dashboards.',
        bullets: [
            'Availability sheets (with status, notes and injury counts) are normalized and cached so roster flux influences spread/total projections.',
            '`build_training_set.py` assembles Torvik + availability + KenPom features into `march_madness_training.parquet`.',
            '`train_models.py` fits the ML, spread and total `joblib` files, and `evaluate_models.py` produces holdout metrics (Brier, bias).',
            'A dashboard export (`dashboard/dashboard.json`) summarizes toss-ups, highest totals and daily evaluation snapshots for the UI/reports.'
        ]
    },
    {
        title: 'Operating Constraints (March Madness Dashboard)',
        detail: 'When the tournament is live we enforce the rules listed in the March Madness Dashboard so the edges stay controlled.',
        bullets: [
            'Max spend/day: $100, max spend/game: $25, parlays capped at 0.5u and total parlay odds between -120 and +300.',
            'Freeze window is 30 minutes to tip (stronger than the standard 10-minute persistence lock) and multi-market bets per game are allowed inside the $25/game cap.',
            'Confidence must be ≥50 to show a recommendation; below that we display PASS while still tracking the internal lean, and we tag every pick with movement/variance risks.'
        ]
    }
];

const pipelineSteps = [
    'Download Torvik Time Machine JSONs (2024/25) and ingest via `pipeline/torvik_time_machine_ingest.py` so both games + snapshots are available.',
    'Feed fresh availability sheets through `pipeline/availability_ingest.py` (status/notes) so injuries and coach tweaks enter the feature set.',
    'Run `pipeline/build_training_set.py` with the processed games, snapshots and availability to create `march_madness_training.parquet`.',
    'Train the three models (`ml_model.joblib`, `spread_model.joblib`, `total_model.joblib`) using `pipeline/train_models.py` and evaluate them with `pipeline/evaluate_models.py` against holdout data.',
    'Blend efficiency outputs with the seed+round prior (`pipeline/blend_predictions.py`) and generate dashboard payloads via `pipeline/dashboard_export.py` for the UI.'
];

export default function MarchMadness() {
    return (
        <div className="space-y-6">
            <div className="flex flex-col gap-2">
                <p className="text-xs uppercase tracking-widest text-blue-300">March Madness</p>
                <h1 className="text-3xl font-semibold">Template & Modeling Review</h1>
                <p className="text-slate-300 max-w-3xl">
                    This tab captures the latest team-profile template, the March Madness model pipeline and the operational guardrails that keep the tournament slate disciplined.
                    Use it as the single source of truth when you need a refresh on how we produce predictions and what the scouting card should include.
                </p>
            </div>

            <section className="grid gap-4 md:grid-cols-2">
                {templateHighlights.map((card) => (
                    <article key={card.title} className="p-5 bg-slate-900 border border-slate-800 rounded-2xl shadow-inner">
                        <h2 className="text-lg font-semibold text-white mb-2">{card.title}</h2>
                        <ul className="list-disc list-inside text-slate-300 space-y-2 text-sm">
                            {card.bullets.map((bullet) => (
                                <li key={bullet}>{bullet}</li>
                            ))}
                        </ul>
                    </article>
                ))}
            </section>

            <section className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-xl font-semibold text-white">Modeling Pulse & Pipeline</h2>
                <div className="grid gap-4 md:grid-cols-3">
                    {modelingHighlights.map((block) => (
                        <div key={block.title} className="bg-slate-950/50 border border-slate-800 rounded-2xl p-4">
                            <h3 className="text-base font-semibold text-slate-200">{block.title}</h3>
                            <p className="text-slate-400 text-sm mt-1">{block.detail}</p>
                            <ul className="mt-3 space-y-2 text-xs text-slate-300 list-disc list-inside">
                                {block.bullets.map((item) => (
                                    <li key={item}>{item}</li>
                                ))}
                            </ul>
                        </div>
                    ))}
                </div>
            </section>

            <section className="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <h2 className="text-xl font-semibold text-white">Pipeline Steps</h2>
                <ol className="list-decimal list-inside space-y-2 text-sm text-slate-200">
                    {pipelineSteps.map((step) => (
                        <li key={step}>{step}</li>
                    ))}
                </ol>
            </section>
        </div>
    );
}
