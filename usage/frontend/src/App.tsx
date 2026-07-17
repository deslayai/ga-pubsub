import { useState, useEffect, useCallback } from 'react';
import type { DemoResult, FeatureKey } from './types.ts';
import { FEATURES } from './types.ts';
import { io, Socket } from 'socket.io-client';
import axios from 'axios';
import clsx from 'clsx';
import { format } from 'date-fns';
import { Activity, ChevronRight, CheckCircle2, XCircle, Loader2, Zap } from 'lucide-react';

const API = 'http://localhost:3001';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const colorBorder: Record<string, string> = {
  indigo: 'border-indigo-500', blue: 'border-blue-500', cyan: 'border-cyan-500',
  yellow: 'border-yellow-500', orange: 'border-orange-500', teal: 'border-teal-500',
  green: 'border-green-500', purple: 'border-purple-500', pink: 'border-pink-500',
  gray: 'border-gray-500', lime: 'border-lime-500', red: 'border-red-500',
};
const colorBg: Record<string, string> = {
  indigo: 'bg-indigo-500/10', blue: 'bg-blue-500/10', cyan: 'bg-cyan-500/10',
  yellow: 'bg-yellow-500/10', orange: 'bg-orange-500/10', teal: 'bg-teal-500/10',
  green: 'bg-green-500/10', purple: 'bg-purple-500/10', pink: 'bg-pink-500/10',
  gray: 'bg-gray-500/10', lime: 'bg-lime-500/10', red: 'bg-red-500/10',
};
const colorText: Record<string, string> = {
  indigo: 'text-indigo-400', blue: 'text-blue-400', cyan: 'text-cyan-400',
  yellow: 'text-yellow-400', orange: 'text-orange-400', teal: 'text-teal-400',
  green: 'text-green-400', purple: 'text-purple-400', pink: 'text-pink-400',
  gray: 'text-gray-400', lime: 'text-lime-400', red: 'text-red-400',
};

// ─── App ─────────────────────────────────────────────────────────────────────

export default function App() {
  const [socket, setSocket] = useState<Socket | null>(null);
  const [connected, setConnected] = useState(false);
  const [running, setRunning] = useState<FeatureKey | null>(null);
  const [results, setResults] = useState<Record<string, DemoResult>>({});
  const [log, setLog] = useState<string[]>([]);

  useEffect(() => {
    const s = io(API);
    s.on('connect',    () => setConnected(true));
    s.on('disconnect', () => setConnected(false));
    s.on('demo:result', (r: DemoResult) => {
      setResults(prev => ({ ...prev, [r.feature]: r }));
      setLog(prev => [`[${format(new Date(),'HH:mm:ss')}] ${r.feature}: ${r.status}`, ...prev].slice(0, 100));
    });
    setSocket(s);
    return () => { s.disconnect(); };
  }, []);

  const run = useCallback(async (key: FeatureKey) => {
    if (running) return;
    setRunning(key);
    try {
      const { data } = await axios.post(`${API}/api/demo/${key}`);
      setResults(prev => ({ ...prev, [data.feature]: data }));
    } catch (e: unknown) {
      const msg = (e as { response?: { data?: { error?: string } } }).response?.data?.error ?? (e as Error).message;
      setResults(prev => ({ ...prev, [key]: { feature: key, status: 'error', steps: [], error: msg, timestamp: Date.now() } }));
    } finally {
      setRunning(null);
    }
  }, [running]);

  const runAll = async () => {
    for (const f of FEATURES) await run(f.key);
  };

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 font-mono">
      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Activity className="w-6 h-6 text-indigo-400" />
          <div>
            <h1 className="text-lg font-bold">GA-PubSub <span className="text-indigo-400">Core</span> Demo</h1>
            <p className="text-xs text-gray-500">ga-pubsub-core · free · MIT</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className={clsx('flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border',
            connected ? 'text-green-400 border-green-500/30 bg-green-500/10' : 'text-gray-500 border-gray-700 bg-gray-800')}>
            <span className={clsx('w-1.5 h-1.5 rounded-full', connected ? 'bg-green-400 animate-pulse' : 'bg-gray-500')} />
            {connected ? 'Connected' : 'Disconnected'}
          </span>
          <button onClick={runAll} className="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-medium transition-colors">
            <Zap className="w-4 h-4" /> Run All
          </button>
        </div>
      </header>

      <div className="p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Feature Grid */}
        <div className="lg:col-span-2 grid grid-cols-1 sm:grid-cols-2 gap-4">
          {FEATURES.map(f => {
            const result = results[f.label];
            const isRunning = running === f.key;
            return (
              <div key={f.key}
                className={clsx('rounded-xl border bg-gray-900 p-4 cursor-pointer transition-all hover:bg-gray-800',
                  colorBorder[f.color] + '/40',
                  isRunning && 'ring-2 ring-indigo-500'
                )}
                onClick={() => run(f.key)}
              >
                <div className="flex items-start justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span className={clsx('p-1.5 rounded-lg text-lg', colorBg[f.color])}>{f.icon}</span>
                    <span className={clsx('font-semibold text-sm', colorText[f.color])}>{f.label}</span>
                  </div>
                  {isRunning ? <Loader2 className="w-4 h-4 animate-spin text-indigo-400" /> :
                   result?.status === 'ok' ? <CheckCircle2 className="w-4 h-4 text-green-400" /> :
                   result?.status === 'error' ? <XCircle className="w-4 h-4 text-red-400" /> :
                   <ChevronRight className="w-4 h-4 text-gray-600" />}
                </div>
                <p className="text-xs text-gray-500 mb-3">{f.description}</p>
                {result && (
                  <div className="space-y-1 max-h-32 overflow-y-auto">
                    {result.steps.map((s, i) => (
                      <p key={i} className="text-xs text-gray-400 leading-relaxed">{s}</p>
                    ))}
                    {result.status === 'error' && (
                      <p className="text-xs text-red-400">❌ {result.error}</p>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Event Log */}
        <div className="rounded-xl border border-gray-800 bg-gray-900 p-4 h-fit sticky top-6">
          <h2 className="text-sm font-semibold text-gray-400 mb-3 flex items-center gap-2">
            <Activity className="w-4 h-4" /> Live Event Log
          </h2>
          <div className="space-y-1 max-h-96 overflow-y-auto">
            {log.length === 0 && <p className="text-xs text-gray-600">Run a demo to see events…</p>}
            {log.map((l, i) => <p key={i} className="text-xs text-gray-400 font-mono">{l}</p>)}
          </div>
        </div>
      </div>
    </div>
  );
}
