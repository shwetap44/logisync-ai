/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import { useState, useEffect, useRef, FormEvent } from 'react';
import { 
  Activity, 
  Database, 
  Layers, 
  Cpu, 
  Terminal, 
  RefreshCw, 
  Send, 
  AlertTriangle, 
  CheckCircle2, 
  XCircle, 
  Clock, 
  Github, 
  FileCode, 
  PlusCircle, 
  ToggleLeft, 
  ToggleRight, 
  ArrowRight,
  Shield,
  Search,
  BookOpen,
  User,
  ExternalLink
} from 'lucide-react';


interface Workflow {
  id: string;
  title: string;
  text: string;
  status: 'pending' | 'queued' | 'processing' | 'completed' | 'failed' | 'dlq';
  currentStage: 'ingestion' | 'summarization' | 'classification' | 'audit';
  summary?: string;
  sentiment?: string;
  entities?: string[];
  error?: string;
  timestamp: number;
}

interface KafkaPartition {
  id: number;
  messages: any[];
  offset: number;
  committedOffsets: Record<string, number>;
}

interface Consumer {
  id: string;
  groupId: string;
  assignedPartitions: number[];
  status: 'idle' | 'processing' | 'paused';
  currentMessageId?: string;
}

interface DatabaseLog {
  id: string;
  query: string;
  executionTimeMs: number;
  indexUsed: boolean;
  timestamp: number;
}

interface SystemLog {
  id: string;
  timestamp: number;
  level: 'info' | 'warn' | 'error' | 'success';
  component: 'GATEWAY' | 'KAFKA' | 'REDIS' | 'DATABASE' | 'WORKER';
  message: string;
}

const SAMPLE_PRESETS = [
  {
    title: 'Cache Stampede Mitigation',
    text: 'A Cache Stampede occurs when high-concurrency requests experience a cache miss simultaneously, causing a storm of expensive database read queries. To mitigate this, we can implement probabilistic early expiration (XFetch) or distributed mutual-exclusion locks (using Redlock on Redis) to allow only a single worker to refresh the cache while others read the old cache value.'
  },
  {
    title: 'Kafka Log Compaction Policies',
    text: 'Kafka Log Compaction ensures that within a log partition, the broker always retains at least the last known value for each message key. This is critical for restorative state-store scenarios (such as Event Sourcing) because it prevents the retention log from growing indefinitely while preserving state updates.'
  },
  {
    title: 'PostgreSQL GIN Indexes for Tag Search',
    text: 'When indexing array or JSONB fields, standard B-Tree indexes fall short as they index the entire structure. A Generalized Inverted Index (GIN) maps individual internal tokens (like tags) to lists of matching Row IDs (TIDs). This allows fast multi-tag searches using postgres operators like @> in sub-millisecond query execution plans.'
  }
];

export default function App() {
  const [metrics, setMetrics] = useState<any>({
    totalThroughput: 142.8,
    p99Latency: '12.4ms',
    nodesOnline: '2 / 2',
    dbSizeMb: 124.5,
    indexHits: '98.2%',
    sequentialScans: 42,
    avgQueryLatencyMs: 12.4
  });

  const [partitions, setPartitions] = useState<KafkaPartition[]>([]);
  const [consumers, setConsumers] = useState<Consumer[]>([]);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [databaseLogs, setDatabaseLogs] = useState<DatabaseLog[]>([]);
  const [systemLogs, setSystemLogs] = useState<SystemLog[]>([]);
  
  // Form states
  const [workflowTitle, setWorkflowTitle] = useState('');
  const [workflowText, setWorkflowText] = useState('');
  const [selectedPresetIndex, setSelectedPresetIndex] = useState(-1);
  const [formError, setFormError] = useState('');
  const [rateLimitMessage, setRateLimitMessage] = useState('');
  const [submitting, setSubmitting] = useState(false);
  
  // Navigation / Tabs
  const [activeTab, setActiveTab] = useState<'overview'>('overview');

  const logsEndRef = useRef<HTMLDivElement>(null);

  // Fetch telemetry from simulated backend
  const fetchTelemetry = async () => {
    try {
      const res = await fetch('/api/telemetry');
      if (res.ok) {
        const data = await res.json();
        setMetrics(data.metrics);
        setPartitions(data.partitions);
        setConsumers(data.consumers);
        setWorkflows(data.workflows);
        setDatabaseLogs(data.databaseLogs);
        setSystemLogs(data.systemLogs);
      }
    } catch (err) {
      console.error('Failed to fetch telemetry from server:', err);
    }
  };

  useEffect(() => {
    fetchTelemetry();
    const interval = setInterval(fetchTelemetry, 3000);
    return () => clearInterval(interval);
  }, []);

  // Submit a new workflow to backend
  const handleFormSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setFormError('');
    setRateLimitMessage('');
    
    if (!workflowTitle.trim() || !workflowText.trim()) {
      setFormError('Please fill in both the title and text fields.');
      return;
    }

    setSubmitting(true);
    try {
      const res = await fetch('/api/workflows', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: workflowTitle, text: workflowText })
      });

      const data = await res.json();
      
      if (res.status === 429) {
        setRateLimitMessage(data.message || 'Rate Limit Exceeded!');
        addTemporarySystemLog('error', 'REDIS', 'Rate Limit Triggered (429 Too Many Requests). Request rejected.');
      } else if (!res.ok) {
        setFormError(data.error || 'Failed to trigger workflow pipeline.');
      } else {
        // Success
        setWorkflowTitle('');
        setWorkflowText('');
        setSelectedPresetIndex(-1);
        fetchTelemetry();
      }
    } catch (err) {
      setFormError('Connection error to EventFlow orchestrator.');
    } finally {
      setSubmitting(false);
    }
  };

  const handleApplyPreset = (index: number) => {
    setSelectedPresetIndex(index);
    setWorkflowTitle(SAMPLE_PRESETS[index].title);
    setWorkflowText(SAMPLE_PRESETS[index].text);
  };

  const handleToggleConsumer = async (consumerId: string) => {
    try {
      const res = await fetch('/api/consumers/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ consumerId })
      });
      if (res.ok) {
        fetchTelemetry();
      }
    } catch (err) {
      console.error('Failed to toggle consumer:', err);
    }
  };

  const handleAddConsumer = async () => {
    try {
      const res = await fetch('/api/consumers/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      if (res.ok) {
        fetchTelemetry();
      } else {
        const data = await res.json();
        alert(data.error || 'Cannot add consumer');
      }
    } catch (err) {
      console.error('Failed to add consumer:', err);
    }
  };

  const addTemporarySystemLog = (level: 'info' | 'warn' | 'error' | 'success', component: any, message: string) => {
    const tempLog: SystemLog = {
      id: Math.random().toString(),
      timestamp: Date.now(),
      level,
      component,
      message
    };
    setSystemLogs(prev => [tempLog, ...prev]);
  };

  const getStatusBadge = (status: Workflow['status']) => {
    switch (status) {
      case 'completed':
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">COMPLETED</span>;
      case 'processing':
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-sky-500/10 text-sky-400 border border-sky-500/20 animate-pulse">PROCESSING</span>;
      case 'queued':
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">QUEUED</span>;
      case 'pending':
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-slate-500/10 text-slate-400 border border-slate-500/20">PENDING</span>;
      case 'dlq':
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/20 flex items-center gap-1"><AlertTriangle className="w-3 h-3 text-rose-400" /> DEAD LETTER QUEUE</span>;
      default:
        return <span className="px-2 py-0.5 rounded text-[10px] font-mono font-semibold bg-slate-500/10 text-slate-400">{status}</span>;
    }
  };

  const getStageLabel = (stage: Workflow['currentStage']) => {
    switch (stage) {
      case 'ingestion': return 'Kafka Ingestion';
      case 'summarization': return 'Gemini Core (Summary)';
      case 'classification': return 'Gemini Classifier (JSON)';
      case 'audit': return 'PostgreSQL Indexing';
      default: return stage;
    }
  };



  return (
    <div id="root" className="flex h-screen w-full overflow-hidden font-sans text-slate-300 bg-slate-950">
      
      {/* SIDEBAR */}
      <aside className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col h-full shrink-0">
        <div className="p-5 border-b border-slate-800">
          <div className="flex items-center gap-2.5 mb-1.5">
            <div className="w-3.5 h-3.5 rounded-full bg-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.6)]"></div>
            <h1 className="font-bold tracking-tight text-white text-base">LogiSync Engine</h1>
          </div>
          <p className="text-[10px] uppercase tracking-widest text-slate-500 font-semibold">V2.4.0 Event Core</p>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-3 space-y-1">
          <button 
            onClick={() => setActiveTab('overview')}
            className={`w-full text-left px-3 py-2.5 rounded text-xs font-semibold flex items-center justify-between transition-colors ${
              activeTab === 'overview' 
                ? 'bg-slate-800 text-emerald-400' 
                : 'text-slate-400 hover:text-slate-200 hover:bg-slate-850'
            }`}
          >
            <span className="flex items-center gap-2">
              <Activity className="w-4 h-4" />
              Live Dashboard
            </span>
            <span className="text-[9px] bg-slate-700 px-1.5 py-0.5 rounded font-mono text-emerald-400 font-bold tracking-wider">LIVE</span>
          </button>



          <div className="pt-6 px-3">
            <p className="text-[10px] uppercase tracking-widest text-slate-500 font-bold mb-2">INFRASTRUCTURE STACK</p>
            <div className="space-y-2 text-[11px] text-slate-400">
              <div className="flex items-center justify-between border-b border-slate-800/60 pb-1.5">
                <span className="flex items-center gap-1.5"><Layers className="w-3 h-3 text-sky-400" /> Kafka Broker</span>
                <span className="font-mono text-slate-500">Confluent 7.3</span>
              </div>
              <div className="flex items-center justify-between border-b border-slate-800/60 pb-1.5">
                <span className="flex items-center gap-1.5"><Shield className="w-3 h-3 text-red-400" /> Redis Cache</span>
                <span className="font-mono text-slate-500">v7.0 Lua Engine</span>
              </div>
              <div className="flex items-center justify-between border-b border-slate-800/60 pb-1.5">
                <span className="flex items-center gap-1.5"><Database className="w-3 h-3 text-emerald-400" /> PostgreSQL</span>
                <span className="font-mono text-slate-500">v15 GIN / B-Tree</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="flex items-center gap-1.5"><Cpu className="w-3 h-3 text-violet-400" /> AI Core</span>
                <span className="font-mono text-violet-400 font-bold">Gemini 2.5</span>
              </div>
            </div>
          </div>
        </nav>

        {/* Developer Profile card highlighting 7 years of exp */}
        <div className="p-4 bg-slate-950/80 border-t border-slate-800/80 mt-auto">
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-2.5">ENGINEER PORTFOLIO</div>
          <div className="flex items-start gap-2.5">
            <div className="w-8 h-8 rounded bg-gradient-to-br from-emerald-500 to-indigo-600 flex items-center justify-center text-xs font-mono font-bold text-white shadow-md">
              SP
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-xs font-bold text-white flex items-center justify-between">
                Shweta P.
                <a 
                  href="https://github.com/shwetap44" 
                  target="_blank" 
                  rel="noreferrer" 
                  className="text-slate-500 hover:text-emerald-400 transition-colors"
                >
                  <ExternalLink className="w-3 h-3" />
                </a>
              </div>
              <div className="text-[10px] text-slate-400">Senior Backend Architect</div>
              <div className="text-[9px] text-emerald-400 font-medium font-mono mt-0.5">7+ Years Systems Experience</div>
            </div>
          </div>
        </div>
      </aside>

      {/* MAIN CONTAINER */}
      <div className="flex-1 flex flex-col h-full overflow-hidden">
        
        {/* HEADER */}
        <header className="h-16 border-b border-slate-800 flex items-center justify-between px-6 bg-slate-900/40 shrink-0">
          <div className="flex items-center gap-6 divide-x divide-slate-800">
            <div className="flex flex-col">
              <span className="text-[9px] text-slate-500 font-bold uppercase tracking-wider">Simulated Influx Rate</span>
              <span className="text-sm font-mono font-bold text-emerald-400 flex items-center gap-1.5">
                {metrics.totalThroughput} <span className="text-[10px] text-slate-500 font-normal">msg/sec</span>
              </span>
            </div>
            <div className="flex flex-col pl-6">
              <span className="text-[9px] text-slate-500 font-bold uppercase tracking-wider">Avg Latency (P99)</span>
              <span className="text-sm font-mono font-bold text-white">{metrics.p99Latency}</span>
            </div>
            <div className="flex flex-col pl-6">
              <span className="text-[9px] text-slate-500 font-bold uppercase tracking-wider">Live Workers</span>
              <span className="text-sm font-mono font-bold text-sky-400">{metrics.nodesOnline}</span>
            </div>
            <div className="flex flex-col pl-6">
              <span className="text-[9px] text-slate-500 font-bold uppercase tracking-wider">Relational Indexes</span>
              <span className="text-sm font-mono font-bold text-emerald-400">{metrics.indexHits} Hits</span>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <a 
              href="https://github.com/shwetap44"
              target="_blank"
              rel="noreferrer"
              className="px-3.5 py-1.5 bg-slate-800 hover:bg-slate-700 text-xs text-white font-bold rounded border border-slate-700 flex items-center gap-2 transition-all cursor-pointer hover:shadow-[0_0_12px_rgba(16,185,129,0.15)]"
            >
              <Github className="w-4 h-4" />
              FORK ON GITHUB
            </a>
          </div>
        </header>

        {/* CONTENT CHANGER VIEW */}
        {activeTab === 'overview' ? (
          <main className="p-5 flex-1 overflow-y-auto space-y-5">
            
            {/* Top Row: System pipeline controls + Interactive submission */}
            <div className="grid grid-cols-1 lg:grid-cols-12 gap-5">
              
              {/* Submission panel (Rate Limiter controlled) */}
              <div className="lg:col-span-7 bg-slate-900 border border-slate-800 rounded-lg p-5 flex flex-col justify-between">
                <div>
                  <div className="flex justify-between items-center mb-4">
                    <h2 className="text-xs font-bold uppercase tracking-widest text-slate-400 flex items-center gap-2">
                      <Send className="w-4 h-4 text-emerald-400" />
                      Job Submission Pipeline (Ingress)
                    </h2>
                    <span className="text-[10px] font-mono text-emerald-400 bg-emerald-500/10 px-2.5 py-0.5 rounded-full border border-emerald-500/20">
                      Redis Protected
                    </span>
                  </div>

                  {/* Preset system design topics */}
                  <div className="mb-4">
                    <span className="text-[10px] text-slate-500 uppercase tracking-widest font-bold block mb-2">
                      APPLY TECHNICAL SYSTEM DESIGN PRESETS:
                    </span>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                      {SAMPLE_PRESETS.map((preset, idx) => (
                        <button
                          key={idx}
                          type="button"
                          onClick={() => handleApplyPreset(idx)}
                          className={`p-2.5 text-left rounded text-xs border transition-all ${
                            selectedPresetIndex === idx 
                              ? 'bg-emerald-950/40 border-emerald-500/40 text-emerald-300'
                              : 'bg-slate-950/40 border-slate-850 hover:border-slate-700 hover:bg-slate-900'
                          }`}
                        >
                          <div className="font-bold flex items-center gap-1">
                            <BookOpen className="w-3.5 h-3.5 opacity-70" />
                            {preset.title.split(' ')[0]} {preset.title.split(' ')[1] || ''}
                          </div>
                          <p className="text-[10px] text-slate-500 line-clamp-1 mt-1 font-mono">{preset.text}</p>
                        </button>
                      ))}
                    </div>
                  </div>

                  <form onSubmit={handleFormSubmit} className="space-y-3">
                    <div>
                      <label className="text-[10px] text-slate-500 uppercase font-bold tracking-wider block mb-1">
                        Workflow Title
                      </label>
                      <input 
                        type="text"
                        placeholder="e.g. Redis Cache stampede mitigation with locks"
                        value={workflowTitle}
                        onChange={(e) => setWorkflowTitle(e.target.value)}
                        className="w-full bg-slate-950/80 border border-slate-800 rounded px-3 py-2 text-xs font-semibold focus:outline-none focus:border-emerald-500"
                      />
                    </div>

                    <div>
                      <label className="text-[10px] text-slate-500 uppercase font-bold tracking-wider block mb-1">
                        Deep System Technical Description
                      </label>
                      <textarea
                        rows={4}
                        placeholder="Provide complex technical architecture details for standard processing..."
                        value={workflowText}
                        onChange={(e) => setWorkflowText(e.target.value)}
                        className="w-full bg-slate-950/80 border border-slate-800 rounded px-3 py-2 text-xs font-mono focus:outline-none focus:border-emerald-500"
                      />
                    </div>

                    {formError && (
                      <div className="text-xs text-rose-400 bg-rose-950/20 border border-rose-900 px-3 py-2 rounded flex items-center gap-2">
                        <AlertTriangle className="w-4 h-4 shrink-0" />
                        {formError}
                      </div>
                    )}

                    {rateLimitMessage && (
                      <div className="text-xs text-red-400 bg-red-950/30 border border-red-900/50 p-3 rounded space-y-1">
                        <div className="font-bold flex items-center gap-1.5 uppercase text-[10px]">
                          <Shield className="w-4 h-4 text-red-500" />
                          Redis Sliding Window Active
                        </div>
                        <p className="font-mono text-[11px] leading-relaxed">{rateLimitMessage}</p>
                        <p className="text-[10px] text-slate-500">Atomic Lua script rejected requests to maintain cluster stability.</p>
                      </div>
                    )}

                    <div className="flex justify-end pt-1">
                      <button
                        type="submit"
                        disabled={submitting}
                        className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 disabled:bg-emerald-800 text-xs font-bold rounded text-white shadow-md flex items-center gap-1.5 cursor-pointer transition-colors"
                      >
                        {submitting ? 'Streaming...' : 'Inject into Kafka Stream'}
                        <ArrowRight className="w-4 h-4" />
                      </button>
                    </div>
                  </form>
                </div>
              </div>

              {/* Kafka Cluster visualizer and Rebalance dashboard */}
              <div className="lg:col-span-5 bg-slate-900 border border-slate-800 rounded-lg p-5 flex flex-col justify-between">
                <div>
                  <div className="flex justify-between items-center mb-4">
                    <h2 className="text-xs font-bold uppercase tracking-widest text-slate-400 flex items-center gap-2">
                      <Layers className="w-4 h-4 text-sky-400" />
                      Kafka Partition & Broker Topology
                    </h2>
                    <span className="text-[10px] font-mono text-sky-400 bg-sky-500/10 px-2 py-0.5 rounded border border-sky-500/20">
                      Rebalancing Active
                    </span>
                  </div>

                  <p className="text-[11px] text-slate-400 mb-4 leading-relaxed font-mono">
                    Manual commits ensure zero loss. Messages are routed via MD5 hash keying.
                  </p>

                  {/* Partitions layout */}
                  <div className="space-y-3 mb-4">
                    {partitions.map((partition) => (
                      <div key={partition.id} className="bg-slate-950 border border-slate-850 p-3 rounded">
                        <div className="flex justify-between items-center mb-2">
                          <span className="text-[11px] font-mono font-bold text-white flex items-center gap-1">
                            <span className="w-2 h-2 rounded-full bg-sky-500 animate-pulse"></span>
                            Partition #{partition.id}
                          </span>
                          <span className="text-[10px] font-mono text-slate-500">
                            Log-End Offset: <strong className="text-slate-300">{partition.offset}</strong>
                          </span>
                        </div>
                        
                        {/* Progress bar visualizer offset */}
                        <div className="h-1.5 w-full bg-slate-800 rounded-full overflow-hidden flex">
                          <div 
                            style={{ width: `${Math.min(100, (partition.committedOffsets['agent-workers'] / partition.offset) * 100)}%` }} 
                            className="bg-sky-500 shadow-[0_0_8px_rgba(56,189,248,0.5)] h-full"
                          ></div>
                        </div>

                        <div className="flex justify-between items-center text-[9px] font-mono text-slate-500 mt-1.5">
                          <span>Committed: {partition.committedOffsets['agent-workers'] || 0}</span>
                          <span>Lag: {partition.offset - (partition.committedOffsets['agent-workers'] || 0)} msg</span>
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Active Consumers cluster list */}
                  <div className="border-t border-slate-800 pt-3">
                    <div className="flex justify-between items-center mb-3">
                      <span className="text-[10px] text-slate-500 uppercase tracking-widest font-bold">
                        ACTIVE CONSUMER AGENTS (GROUP: agent-workers)
                      </span>
                      <button 
                        onClick={handleAddConsumer}
                        className="text-[10px] text-emerald-400 flex items-center gap-1 hover:underline cursor-pointer"
                      >
                        <PlusCircle className="w-3.5 h-3.5" /> ADD NODE
                      </button>
                    </div>

                    <div className="space-y-2">
                      {consumers.map((consumer) => (
                        <div key={consumer.id} className="flex items-center justify-between bg-slate-950/40 border border-slate-850 p-2.5 rounded text-xs">
                          <div className="flex items-center gap-2">
                            <span className={`w-2 h-2 rounded-full ${consumer.status === 'paused' ? 'bg-slate-600' : 'bg-emerald-500'}`}></span>
                            <div>
                              <div className="font-mono font-bold text-slate-300">{consumer.id}</div>
                              <div className="text-[9px] text-slate-500">
                                {consumer.status === 'paused' ? 'OFFLINE' : `Assigned Partitions: [${consumer.assignedPartitions.join(', ')}]`}
                              </div>
                            </div>
                          </div>
                          
                          <button
                            onClick={() => handleToggleConsumer(consumer.id)}
                            className="text-[10px] flex items-center gap-1.5 px-2 py-1 rounded bg-slate-800/80 hover:bg-slate-700 font-mono transition-colors border border-slate-750"
                          >
                            {consumer.status === 'paused' ? (
                              <>
                                <ToggleLeft className="w-4 h-4 text-slate-500" />
                                BOOT UP
                              </>
                            ) : (
                              <>
                                <ToggleRight className="w-4 h-4 text-emerald-400" />
                                SHUTDOWN
                              </>
                            )}
                          </button>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Middle Row: Active Workflows List in real-time */}
            <div className="bg-slate-900 border border-slate-800 rounded-lg overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-800 flex justify-between items-center">
                <h2 className="text-xs font-bold uppercase tracking-widest text-slate-400 flex items-center gap-2">
                  <Terminal className="w-4 h-4 text-emerald-400" />
                  Real-time Distributed Ingestion pipeline
                </h2>
                <div className="flex gap-4 text-[10px] font-mono">
                  <span className="flex items-center gap-1.5"><div className="w-2 h-2 rounded-full bg-emerald-500"></div> Success</span>
                  <span className="flex items-center gap-1.5"><div className="w-2 h-2 rounded-full bg-sky-500"></div> Active Workers</span>
                  <span className="flex items-center gap-1.5"><div className="w-2 h-2 rounded-full bg-rose-500"></div> DLQ</span>
                </div>
              </div>

              <div className="overflow-x-auto">
                {workflows.length === 0 ? (
                  <div className="p-8 text-center text-slate-500 text-xs">
                    No active processes in stream. Use the pipeline submission to inject technical blocks.
                  </div>
                ) : (
                  <table className="w-full text-left text-xs">
                    <thead>
                      <tr className="text-slate-500 uppercase text-[9px] tracking-wider border-b border-slate-800/80 bg-slate-950/20">
                        <th className="px-5 py-3 font-bold font-mono">UUID Trace ID</th>
                        <th className="px-5 py-3 font-bold">Topic Name</th>
                        <th className="px-5 py-3 font-bold">Workflow Title / Tech Pipeline</th>
                        <th className="px-5 py-3 font-bold">Current Stage</th>
                        <th className="px-5 py-3 font-bold">Status</th>
                        <th className="px-5 py-3 font-bold">Extracted Tech Tags (Postgres GIN mapped)</th>
                        <th className="px-5 py-3 font-bold text-right">Age</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/30">
                      {workflows.map((wf) => (
                        <tr key={wf.id} className="hover:bg-slate-800/30 font-sans transition-all">
                          <td className="px-5 py-3 text-slate-500 font-mono text-[10px] select-all">{wf.id.slice(0, 13)}...</td>
                          <td className="px-5 py-3 text-slate-400 font-mono text-[11px]">workflow-pipeline</td>
                          <td className="px-5 py-3 max-w-sm">
                            <div className="font-bold text-white text-[12px] truncate">{wf.title}</div>
                            {wf.summary ? (
                              <p className="text-[11px] text-slate-400 mt-1 font-mono leading-relaxed line-clamp-2 bg-slate-950/40 p-2 rounded border border-slate-850">
                                {wf.summary}
                              </p>
                            ) : (
                              <p className="text-[10px] text-slate-500 mt-0.5 line-clamp-1 italic">{wf.text}</p>
                            )}
                            {wf.error && (
                              <div className="text-[10px] text-rose-400 mt-1 font-mono bg-rose-950/10 p-1.5 rounded border border-rose-900/30">
                                Fail Log: {wf.error}
                              </div>
                            )}
                          </td>
                          <td className="px-5 py-3">
                            <span className="text-[11px] font-mono text-slate-300">
                              {getStageLabel(wf.currentStage)}
                            </span>
                          </td>
                          <td className="px-5 py-3">{getStatusBadge(wf.status)}</td>
                          <td className="px-5 py-3">
                            <div className="flex flex-wrap gap-1 max-w-xs">
                              {wf.entities && wf.entities.map((tag, i) => (
                                <span 
                                  key={i} 
                                  className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-indigo-950/40 text-indigo-300 border border-indigo-900/40"
                                >
                                  {tag}
                                </span>
                              ))}
                              {wf.sentiment && (
                                <span className={`px-1.5 py-0.5 rounded text-[10px] font-mono ${
                                  wf.sentiment === 'POSITIVE' ? 'bg-emerald-950/30 text-emerald-400 border border-emerald-900/30' :
                                  wf.sentiment === 'NEGATIVE' ? 'bg-rose-950/30 text-rose-400 border border-rose-900/30' :
                                  'bg-slate-800 text-slate-400'
                                }`}>
                                  Sentiment: {wf.sentiment}
                                </span>
                              )}
                              {!wf.entities && <span className="text-[10px] text-slate-500 italic">Extracting...</span>}
                            </div>
                          </td>
                          <td className="px-5 py-3 text-right text-slate-500 font-mono text-[11px]">
                            {Math.round((Date.now() - Number(wf.timestamp)) / 1000)}s ago
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>

            {/* Bottom Row: PostgreSQL Database Query Logger & System trace logger */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
              
              {/* PostgreSQL Audit Logger & Index monitor */}
              <div className="bg-slate-900 border border-slate-800 rounded-lg p-5 flex flex-col justify-between">
                <div>
                  <div className="flex justify-between items-center mb-3">
                    <h2 className="text-xs font-bold uppercase tracking-widest text-slate-400 flex items-center gap-2">
                      <Database className="w-4 h-4 text-emerald-400" />
                      PostgreSQL Real-Time Trace Logger (Index Audit)
                    </h2>
                    <span className="text-[10px] font-mono text-emerald-400 bg-emerald-500/10 px-2.5 py-0.5 rounded-full border border-emerald-500/20">
                      GIN / B-Tree Online
                    </span>
                  </div>

                  <p className="text-[11px] text-slate-400 mb-4 leading-relaxed font-mono">
                    Monitors query latency in microseconds. Highlights index optimizations matching GIN tags.
                  </p>

                  <div className="space-y-2 max-h-56 overflow-y-auto">
                    {databaseLogs.map((log) => (
                      <div key={log.id} className="bg-slate-950/80 border border-slate-850 p-3 rounded font-mono text-[11px]">
                        <div className="flex justify-between items-start mb-1.5">
                          <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${
                            log.indexUsed ? 'bg-emerald-950 text-emerald-400 border border-emerald-900/40' : 'bg-red-950 text-red-400 border border-red-900/40'
                          }`}>
                            {log.indexUsed ? 'INDEX SCAN (GIN)' : 'SEQUENTIAL TABLE SCAN'}
                          </span>
                          <span className="text-slate-500 font-bold">{log.executionTimeMs} ms</span>
                        </div>
                        <p className="text-slate-300 break-all bg-slate-950 p-2 rounded border border-slate-900/60 leading-relaxed font-semibold">
                          {log.query}
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Distributed System Telemetry Log */}
              <div className="bg-slate-900 border border-slate-800 rounded-lg p-5 flex flex-col justify-between">
                <div>
                  <div className="flex justify-between items-center mb-3">
                    <h2 className="text-xs font-bold uppercase tracking-widest text-slate-400 flex items-center gap-2">
                      <Terminal className="w-4 h-4 text-emerald-400" />
                      Distributed Mesh Telemetry Log
                    </h2>
                    <span className="text-[10px] font-mono text-slate-500">
                      Standard Out (STDOUT)
                    </span>
                  </div>

                  <p className="text-[11px] text-slate-400 mb-4 leading-relaxed font-mono">
                    Output from independent microservices, workers, rate-limiters, and gateway logs.
                  </p>

                  <div className="bg-slate-950 border border-slate-850 p-3 rounded-lg h-56 overflow-y-auto font-mono text-[11px] space-y-2.5">
                    {systemLogs.map((log) => (
                      <div key={log.id} className="flex items-start gap-2 leading-relaxed">
                        <span className={`text-[10px] px-1 py-0.5 rounded font-bold shrink-0 ${
                          log.level === 'success' ? 'bg-emerald-950/40 text-emerald-400' :
                          log.level === 'warn' ? 'bg-amber-950/40 text-amber-400' :
                          log.level === 'error' ? 'bg-rose-950/40 text-rose-400' :
                          'bg-slate-800 text-slate-400'
                        }`}>
                          {log.component}
                        </span>
                        <div className="flex-1 min-w-0">
                          <span className="text-slate-300 font-semibold">{log.message}</span>
                          <span className="text-[9px] text-slate-500 ml-2">
                            {new Date(Number(log.timestamp)).toISOString().split('T')[1].slice(0, 8)}
                          </span>
                        </div>
                      </div>
                    ))}
                    <div ref={logsEndRef}></div>
                  </div>
                </div>
              </div>

            </div>

          </main>
        ) : null}

      </div>

    </div>
  );
}
