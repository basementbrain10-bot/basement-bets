import React from 'react';

// Extracted from TransactionView.jsx. Keep behavior identical.
export default function SportAuditorModal({
  show,
  items,
  auditLoading,
  onClose,
  onRerun,
  onApply,
}) {
  if (!show) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div className="w-full max-w-4xl bg-slate-900 border border-slate-700 rounded-xl shadow-2xl p-5">
        <div className="flex items-center justify-between mb-3">
          <div>
            <div className="text-white font-bold">Sport Auditor</div>
            <div className="text-xs text-slate-400">
              Flags bets where sport disagrees with matched event league (teams + date window).
            </div>
          </div>
          <button type="button" className="text-gray-400 hover:text-white" onClick={() => onClose?.()}>
            ✕
          </button>
        </div>

        <div className="mb-3 flex items-center justify-between">
          <div className="text-xs text-slate-400">
            Found <span className="text-white font-bold">{(items || []).length}</span> potential mismatches.
          </div>
          <button
            type="button"
            onClick={() => onRerun?.()}
            disabled={auditLoading}
            className={`text-xs font-medium px-3 py-1.5 rounded-lg border transition ${
              auditLoading
                ? 'text-gray-500 border-gray-800 bg-gray-900/40 animate-pulse'
                : 'text-amber-300 hover:text-amber-200 border-amber-900/40 hover:bg-amber-900/20'
            }`}
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
              {(items || []).length === 0 ? (
                <tr>
                  <td className="px-3 py-6 text-slate-500" colSpan={7}>
                    No mismatches found.
                  </td>
                </tr>
              ) : (
                (items || []).map((it) => (
                  <tr key={it.bet_id} className="hover:bg-slate-800/40">
                    <td className="px-3 py-2 font-mono text-[11px] text-slate-300 whitespace-nowrap">{it.date}</td>
                    <td className="px-3 py-2 text-slate-200">
                      {it.matchup || '—'}
                      {it.is_bonus ? (
                        <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded border border-yellow-800 bg-yellow-900/30 text-yellow-200">
                          BONUS
                        </span>
                      ) : null}
                    </td>
                    <td className="px-3 py-2 text-slate-400">{it.provider || '—'}</td>
                    <td className="px-3 py-2 text-slate-400">{it.bet_type || '—'}</td>
                    <td className="px-3 py-2">
                      <span className="text-[10px] px-2 py-1 rounded border border-slate-700 bg-slate-950 text-slate-200 font-bold">
                        {it.sport}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <span className="text-[10px] px-2 py-1 rounded border border-amber-900/40 bg-amber-900/20 text-amber-200 font-bold">
                        {it.suggested_sport}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <button
                        type="button"
                        onClick={() => onApply?.(it)}
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
  );
}
