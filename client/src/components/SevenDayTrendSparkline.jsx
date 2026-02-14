import React from 'react';
import { ResponsiveContainer, AreaChart, Area, Tooltip } from 'recharts';

// Extracted from TransactionView.jsx. Keep behavior identical.
export default function SevenDayTrendSparkline({ sevenDayData, formatCurrency }) {
  if (!sevenDayData || sevenDayData.length === 0) return null;

  return (
    <div className="h-10 w-32 hidden md:block opacity-80 hover:opacity-100 transition-opacity">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={sevenDayData}>
          <defs>
            <linearGradient id="trendGradient" x1="0" y1="0" x2="0" y2="1">
              <stop
                offset="5%"
                stopColor={sevenDayData[6]?.profit >= 0 ? '#4ade80' : '#f87171'}
                stopOpacity={0.3}
              />
              <stop
                offset="95%"
                stopColor={sevenDayData[6]?.profit >= 0 ? '#4ade80' : '#f87171'}
                stopOpacity={0}
              />
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
            stroke={sevenDayData[6]?.profit >= 0 ? '#4ade80' : '#f87171'}
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
              7D Trend • End <span className="text-slate-200">{formatCurrency(end)}</span> • Δ{' '}
              <span className={dcls}>{formatCurrency(delta)}</span>
            </span>
          );
        })()}
      </div>
    </div>
  );
}
