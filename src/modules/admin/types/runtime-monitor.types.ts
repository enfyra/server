import type { RuntimeMonitorService } from '../services/runtime-monitor.service';

export type RuntimeMetricsSnapshot = Awaited<ReturnType<RuntimeMonitorService['getSnapshot']>>;
