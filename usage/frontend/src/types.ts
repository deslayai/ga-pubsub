export interface BusEvent {
  id:        string;
  event:     string;
  payload:   Record<string, unknown>;
  timestamp: number;
  source:    string;
  ttl?:      number;
  correlationId?: string;
}

export interface BusError {
  code:      string;
  message:   string;
  event:     string;
  phase:     string;
  timestamp: number;
}

export interface BusMetrics {
  publishCount:         number;
  subscribeCount:       number;
  unsubscribeCount:     number;
  failedDeliveries:     number;
  middlewareRejections: number;
  validationFailures:   number;
  replayCount:          number;
  requestCount:         number;
  requestTimeouts:      number;
  activeSubscriptions:  number;
  historySize:          number;
  p95LatencyMs:         number;
}

export interface DemoResult {
  feature:   string;
  status:    'ok' | 'error';
  steps:     string[];
  data?:     unknown;
  error?:    string;
  timestamp: number;
}

export type FeatureKey =
  | 'basic' | 'wildcard-single' | 'wildcard-multi'
  | 'priority' | 'once' | 'middleware' | 'validation'
  | 'replay' | 'rpc' | 'cancel' | 'ttl' | 'metrics';

export interface FeatureDef {
  key:         FeatureKey;
  label:       string;
  icon:        string;
  description: string;
  color:       string;
}

export const FEATURES: FeatureDef[] = [
  { key: 'basic',          label: 'Basic Pub/Sub',     icon: '📡', description: 'Publish events and receive them in real-time subscribers',            color: 'indigo'  },
  { key: 'wildcard-single',label: 'Wildcard *',         icon: '🎯', description: 'Match exactly one segment: payments.* → payments.created',             color: 'blue'    },
  { key: 'wildcard-multi', label: 'Wildcard **',        icon: '🌐', description: 'Match deep paths: payments.** → payments.invoice.line.added',           color: 'cyan'    },
  { key: 'priority',       label: 'Priority Ordering', icon: '⚡', description: 'High-priority handlers always dispatched before low-priority ones',    color: 'yellow'  },
  { key: 'once',           label: 'Subscribe Once',    icon: '1️⃣', description: 'Auto-unsubscribing handler fires exactly once then removes itself',    color: 'orange'  },
  { key: 'middleware',     label: 'Middleware',         icon: '🔧', description: 'Enrich payment events with processing fees in a composable pipeline', color: 'teal'    },
  { key: 'validation',     label: 'Schema Validation', icon: '✅', description: 'Reject malformed payment payloads before they reach subscribers',     color: 'green'   },
  { key: 'replay',         label: 'Replay Engine',     icon: '⏮️', description: 'Late subscribers receive full event history automatically',           color: 'purple'  },
  { key: 'rpc',            label: 'RPC Request/Reply', icon: '🔄', description: 'Query payment status via request-response over the event bus',        color: 'pink'    },
  { key: 'cancel',         label: 'Request Cancel',    icon: '🚫', description: 'Cancel in-flight RPC requests cleanly before timeout fires',          color: 'gray'    },
  { key: 'ttl',            label: 'TTL Expiry',        icon: '⏳', description: 'Session events expire — never replayed to late subscribers',          color: 'lime'    },
  { key: 'metrics',        label: 'Live Metrics',      icon: '📊', description: 'Real-time publish count, latency p95, and subscription stats',        color: 'indigo'  },
];
