export interface AutoScaleConfig {
  minProcesses: number;
  maxProcesses: number;
  configMax: number;
  scaleUpRatio: number;
  scaleUpMinPending: number;
  scaleDownUtilizationThreshold: number;
  scaleDownDuration: number;
  scaleUpCooldown: number;
  scaleDownCooldown: number;
}

export interface ScaleDecision {
  shouldScale: boolean;
  direction: 'up' | 'down' | 'none';
  reason: string;
  currentSize: number;
  targetSize: number;
}

export interface PoolMetrics {
  size: number;
  available: number;
  borrowed: number;
  pending: number;
  utilization: number;
  pressureRatio: number;
}

export const AUTO_SCALE_CONFIG: AutoScaleConfig = {
  minProcesses: 2,
  maxProcesses: 16,
  configMax: 4,
  scaleUpRatio: 0.5,
  scaleUpMinPending: 3,
  scaleDownUtilizationThreshold: 0.3,
  scaleDownDuration: 60000,
  scaleUpCooldown: 500,
  scaleDownCooldown: 30000,
};