import React from 'react';
import { Trash } from 'lucide-react';

// Extracted from TransactionView.jsx. Keep behavior identical.
export default function EditBetModal({
  show,
  editBet,
  setEditBet,
  editNote,
  setEditNote,
  isUpdating,
  onSave,
  onDelete,
  onClose,
}) {
  if (!show || !editBet) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div className="w-full max-w-2xl bg-slate-900 border border-slate-700 rounded-xl shadow-2xl p-5">
        <div className="flex items-center justify-between mb-4">
          <div className="text-white font-bold">Edit Bet</div>
          <button
            type="button"
            className="text-gray-400 hover:text-white"
            onClick={() => {
              onClose?.();
            }}
          >
            ✕
          </button>
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
            <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Account</div>
            <select
              className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
              value={editBet.account_id || 'Main'}
              onChange={(e) => setEditBet({ ...editBet, account_id: e.target.value })}
            >
              <option value="Main">Primary</option>
              <option value="User2">Secondary</option>
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
            <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Profit / Loss</div>
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
            <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Description</div>
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
            <input
              type="text"
              className="w-full bg-gray-900 border border-gray-700 rounded px-2 py-2 text-sm text-white"
              value={editNote}
              onChange={(e) => setEditNote(e.target.value)}
              placeholder="e.g. corrected profit from settlement"
            />
          </div>
        </div>

        <div className="mt-5 flex items-center justify-between">
          <button
            type="button"
            className="px-3 py-2 bg-red-900/30 hover:bg-red-900/60 text-red-400 hover:text-red-300 rounded-lg text-sm font-bold border border-red-900/40 flex items-center gap-1.5 transition"
            onClick={() => {
              onDelete?.(editBet.id);
              onClose?.();
            }}
            disabled={isUpdating}
          >
            <Trash size={14} /> Delete
          </button>
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-bold"
              onClick={() => onClose?.()}
              disabled={isUpdating}
            >
              Cancel
            </button>
            <button
              type="button"
              className="px-4 py-2 bg-green-600 hover:bg-green-500 text-black rounded-lg text-sm font-black"
              onClick={() => onSave?.()}
              disabled={isUpdating}
            >
              {isUpdating ? 'Saving…' : 'Save'}
            </button>
          </div>
        </div>
        <div className="mt-2 text-[10px] text-slate-500">Saves directly to the database (persists for history + analytics).</div>
      </div>
    </div>
  );
}
