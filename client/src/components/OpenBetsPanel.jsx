import React, { useEffect, useState } from 'react';
import api from '../api/axios';
import { Pencil } from 'lucide-react';
import EditBetModal from './EditBetModal';

export default function OpenBetsPanel({ formatCurrency, formatDateMDY, title = 'Open Bets' }) {
  const [openBets, setOpenBets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const [showEdit, setShowEdit] = useState(false);
  const [editBet, setEditBet] = useState(null);
  const [editNote, setEditNote] = useState('');
  const [isUpdating, setIsUpdating] = useState(false);

  const load = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await api.get('/api/bets/open');
      setOpenBets(res.data || []);
    } catch (e) {
      setError('Failed to load open bets');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const computeEventText = (bet) => {
    const sources = [bet?.raw_text, bet?.description, bet?.selection]
      .filter(Boolean)
      .map((s) => String(s));
    const cleanSide = (x) => {
      let s = String(x || '').trim();
      s = s.replace(/\s+[+\-−–]\d+(?:\.\d+)?\b.*$/, '').trim();
      s = s.replace(/\s+(over|under)\s*\d+(?:\.\d+)?\b.*$/i, '').trim();
      s = s.replace(/\b([A-Za-z]{3,})\s+\1\b/gi, '$1').trim();
      s = s.split('|')[0].trim();
      return s;
    };

    for (const src of sources) {
      for (const ln of String(src)
        .split(/\n/)
        .map((l) => l.trim())
        .filter(Boolean)) {
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
        event_text: computeEventText(editBet) || undefined,
        update_note: editNote,
      };

      await api.patch(`/api/bets/${editBet.id}`, payload);
      setShowEdit(false);
      setEditBet(null);
      setEditNote('');
      await load();
    } catch (e) {
      alert('Failed to update bet.');
    } finally {
      setIsUpdating(false);
    }
  };

  const handleDelete = async (betId) => {
    if (!confirm('Are you sure you want to delete this bet?')) return;
    setIsUpdating(true);
    try {
      await api.delete(`/api/bets/${betId}`);
      setShowEdit(false);
      setEditBet(null);
      setEditNote('');
      await load();
    } catch (e) {
      alert('Failed to delete bet.');
    } finally {
      setIsUpdating(false);
    }
  };

  const totalOpenStake = (openBets || []).reduce((sum, b) => sum + (Number(b.wager || 0) || 0), 0);

  return (
    <>
      <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden shadow-xl">
        <div className="p-4 border-b border-slate-800 flex items-center justify-between">
          <div>
            <div className="text-sm font-black text-slate-100 uppercase tracking-wider">{title}</div>
            <div className="text-[11px] text-slate-500">All books • Click a row to edit/settle</div>
          </div>
          <div className="text-xs text-slate-400">
            {loading ? 'Loading…' : `${(openBets || []).length} open • ${formatCurrency ? formatCurrency(totalOpenStake) : totalOpenStake.toFixed(2)} staked`}
          </div>
        </div>

        <div className="p-4">
          {error && <div className="text-xs text-red-300">{error}</div>}
          {!loading && (!openBets || openBets.length === 0) && <div className="text-xs text-slate-500">No open bets.</div>}

          {!loading && openBets && openBets.length > 0 && (
            <div className="space-y-2">
              {openBets.slice(0, 25).map((b) => (
                <div
                  key={b.id}
                  className="flex items-center justify-between gap-3 p-3 rounded-lg border border-slate-800 bg-slate-950/20 hover:bg-slate-950/40 transition cursor-pointer"
                  onClick={() => {
                    setEditBet({
                      ...b,
                      date: String(b.date_et || b.date || '').slice(0, 10),
                      wager: b.wager ?? b.stake ?? b.amount,
                    });
                    setEditNote('');
                    setShowEdit(true);
                  }}
                >
                  <div className="min-w-0">
                    <div className="text-xs font-black text-slate-100 truncate flex items-center gap-2">
                      <span>{b.provider} • {(b.account_id === 'User2' ? 'Secondary' : 'Primary')}</span>
                      <span className="inline-flex items-center gap-1 text-[10px] text-slate-400 border border-slate-700/60 rounded px-1.5 py-0.5">
                        <Pencil size={12} /> Edit
                      </span>
                    </div>
                    <div className="text-xs text-slate-300 truncate" title={b.selection || b.description}>{b.selection || b.description}</div>
                    <div className="text-[11px] text-slate-500">{formatDateMDY ? formatDateMDY(b.date_et || b.date) : String(b.date_et || b.date)} • {String(b.status || 'PENDING').toUpperCase()}</div>
                  </div>
                  <div className="text-right shrink-0">
                    <div className="text-xs font-mono font-black text-slate-200">{formatCurrency ? formatCurrency(Number(b.wager || 0)) : Number(b.wager || 0).toFixed(2)}</div>
                    <div className="text-[11px] text-slate-500 font-mono">{b.odds ? (Number(b.odds) > 0 ? `+${b.odds}` : String(b.odds)) : '—'}</div>
                  </div>
                </div>
              ))}
              {openBets.length > 25 && <div className="text-[11px] text-slate-500">Showing first 25 open bets.</div>}
            </div>
          )}
        </div>
      </div>

      <EditBetModal
        show={showEdit}
        editBet={editBet}
        setEditBet={setEditBet}
        editNote={editNote}
        setEditNote={setEditNote}
        isUpdating={isUpdating}
        onSave={handleEditSave}
        onDelete={handleDelete}
        onClose={() => {
          setShowEdit(false);
          setEditBet(null);
          setEditNote('');
        }}
      />
    </>
  );
}
