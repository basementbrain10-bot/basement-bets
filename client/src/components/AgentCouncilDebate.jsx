import React from 'react';
import { Search, Brain, Gavel, FileText, Activity } from 'lucide-react';

const getAgentIcon = (agentName) => {
    const name = agentName.toLowerCase();
    if (name.includes('stat') || name.includes('quant')) return <Activity size={18} className="text-blue-400 shrink-0" />;
    if (name.includes('news') || name.includes('injury') || name.includes('qualitative')) return <FileText size={18} className="text-orange-400 shrink-0" />;
    if (name.includes('memory') || name.includes('rag')) return <Brain size={18} className="text-purple-400 shrink-0" />;
    if (name.includes('executive') || name.includes('summary')) return <Gavel size={18} className="text-violet-400 shrink-0" />;
    if (name.includes('contrarian') || name.includes('auditor') || name.includes('critique')) return <Gavel size={18} className="text-rose-400 shrink-0" />;
    return <Search size={18} className="text-slate-400 shrink-0" />;
};

const AgentCouncilDebate = ({ debate }) => {
    if (!debate || !Array.isArray(debate)) return null;

    return (
        <div className="space-y-6">
            {debate.map((msg, idx) => (
                <div key={idx} className="flex gap-4">
                    <div className="w-10 h-10 rounded-full bg-slate-800 flex items-center justify-center border border-slate-700 shrink-0">
                        {getAgentIcon(msg.agent || msg.role)}
                    </div>
                    <div className="flex-1 bg-slate-900 border border-slate-800 rounded-2xl rounded-tl-sm p-4">
                        <div className="font-bold text-sm text-slate-300 mb-1">{msg.agent || msg.role}</div>
                        <div className="text-slate-400 text-sm leading-relaxed whitespace-pre-wrap">{msg.message || msg.content}</div>
                    </div>
                </div>
            ))}
        </div>
    );
};

export default AgentCouncilDebate;
export { getAgentIcon };
