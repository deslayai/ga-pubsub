import { useEffect, useState, useCallback } from 'react';
import { getSocket } from '../api/client.ts';
import type { BusEvent, BusError, BusMetrics, DemoResult } from '../types.ts';

const MAX_EVENTS = 200;

export function useSocket() {
  const [connected,   setConnected]   = useState(false);
  const [events,      setEvents]      = useState<BusEvent[]>([]);
  const [errors,      setErrors]      = useState<BusError[]>([]);
  const [metrics,     setMetrics]     = useState<BusMetrics | null>(null);
  const [demoResults, setDemoResults] = useState<DemoResult[]>([]);

  const clearEvents = useCallback(() => setEvents([]), []);
  const clearErrors = useCallback(() => setErrors([]), []);

  useEffect(() => {
    const socket = getSocket();

    socket.on('connect',    () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));

    socket.on('bus:event', (e: BusEvent) => {
      setEvents(prev => [e, ...prev].slice(0, MAX_EVENTS));
    });

    socket.on('bus:error', (e: BusError) => {
      setErrors(prev => [e, ...prev].slice(0, 50));
    });

    socket.on('bus:metrics', (m: BusMetrics) => {
      setMetrics(m);
    });

    socket.on('demo:result', (r: DemoResult) => {
      setDemoResults(prev => [r, ...prev].slice(0, 20));
    });

    return () => {
      socket.off('connect');
      socket.off('disconnect');
      socket.off('bus:event');
      socket.off('bus:error');
      socket.off('bus:metrics');
      socket.off('demo:result');
    };
  }, []);

  return { connected, events, errors, metrics, demoResults, clearEvents, clearErrors };
}
