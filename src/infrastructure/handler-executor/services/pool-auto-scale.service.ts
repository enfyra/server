import { Pool } from 'generic-pool';
import { ChildProcess } from 'child_process';
import {
  AutoScaleConfig,
  ScaleDecision,
  PoolMetrics,
  AUTO_SCALE_CONFIG,
} from '../types/auto-scale.types';

export class PoolAutoScaleService {
  private config: AutoScaleConfig;
  private lastScaleUpAt: number = 0;
  private lastScaleDownAt: number = 0;
  private lowUtilizationSince: number | null = null;
  private currentMaxSize: number;
  private configMax: number;

  constructor(configMax: number, config: Partial<AutoScaleConfig> = {}) {
    this.configMax = configMax;
    const maxProcesses = configMax * 2;
    this.config = { ...AUTO_SCALE_CONFIG, ...config, configMax, maxProcesses };
    this.currentMaxSize = this.config.minProcesses;
  }

  getMetrics(pool: Pool<ChildProcess>): PoolMetrics {
    const size = pool.size;
    const available = pool.available;
    const borrowed = pool.borrowed;
    const pending = pool.pending;
    const utilization = size > 0 ? borrowed / size : 0;

    // Pressure combines queue pressure and utilization pressure
    // Queue pressure: pending requests waiting for available process
    // Utilization pressure: how much of the pool is being used
    // Formula: pressure = queuePressure + utilizationWeight
    // - If pending > 0: urgent (scale up needed immediately)
    // - If utilization high: moderate (might need scale up soon)
    const queuePressure = borrowed > 0 ? pending / borrowed : pending > 0 ? Infinity : 0;
    const utilizationPressure = utilization;
    const pressureRatio = queuePressure + utilizationPressure;

    return {
      size,
      available,
      borrowed,
      pending,
      utilization,
      pressureRatio,
    };
  }

  evaluate(pool: Pool<ChildProcess>): ScaleDecision {
    const metrics = this.getMetrics(pool);
    const now = Date.now();

    if (metrics.size >= this.currentMaxSize && this.currentMaxSize < this.config.maxProcesses) {
      this.currentMaxSize = Math.min(metrics.size, this.config.maxProcesses);
    }

    const scaleUpDecision = this.checkScaleUp(metrics, now);
    if (scaleUpDecision) return scaleUpDecision;

    const scaleDownDecision = this.checkScaleDown(metrics, now);
    if (scaleDownDecision) return scaleDownDecision;

    this.lowUtilizationSince = null;

    return {
      shouldScale: false,
      direction: 'none',
      reason: `utilization=${(metrics.utilization * 100).toFixed(0)}%, pending=${metrics.pending}`,
      currentSize: metrics.size,
      targetSize: metrics.size,
    };
  }

  private checkScaleUp(metrics: PoolMetrics, now: number): ScaleDecision | null {
    if (metrics.size >= this.config.maxProcesses) {
      return null;
    }

    if (now - this.lastScaleUpAt < this.config.scaleUpCooldown) {
      return null;
    }

    // Scale up conditions:
    // 1. High pressure from queue (pending requests waiting)
    // 2. Or sustained high utilization (approaching capacity)
    const hasQueuePressure = metrics.pending >= this.config.scaleUpMinPending && metrics.pressureRatio >= this.config.scaleUpRatio;
    const hasHighUtilization = metrics.utilization >= 0.9 && metrics.pending > 0;

    if (hasQueuePressure || hasHighUtilization) {
      const targetSize = Math.min(metrics.size + 1, this.config.maxProcesses);
      this.lastScaleUpAt = now;
      this.lowUtilizationSince = null;

      const reason = hasQueuePressure
        ? `pressure=${metrics.pressureRatio.toFixed(2)}, pending=${metrics.pending}`
        : `utilization=${(metrics.utilization * 100).toFixed(0)}%, pending=${metrics.pending}`;

      return {
        shouldScale: true,
        direction: 'up',
        reason,
        currentSize: metrics.size,
        targetSize,
      };
    }

    return null;
  }

  private checkScaleDown(metrics: PoolMetrics, now: number): ScaleDecision | null {
    if (metrics.size <= this.config.minProcesses) {
      return null;
    }

    if (now - this.lastScaleDownAt < this.config.scaleDownCooldown) {
      return null;
    }

    if (metrics.utilization > this.config.scaleDownUtilizationThreshold) {
      this.lowUtilizationSince = null;
      return null;
    }

    if (this.lowUtilizationSince === null) {
      this.lowUtilizationSince = now;
      return null;
    }

    const lowUtilDuration = now - this.lowUtilizationSince;
    if (lowUtilDuration < this.config.scaleDownDuration) {
      return null;
    }

    if (metrics.available < 1) {
      return null;
    }

    const targetSize = Math.max(metrics.size - 1, this.config.minProcesses);
    this.lastScaleDownAt = now;
    this.lowUtilizationSince = null;

    return {
      shouldScale: true,
      direction: 'down',
      reason: `utilization=${(metrics.utilization * 100).toFixed(0)}%<${this.config.scaleDownUtilizationThreshold * 100}% for ${lowUtilDuration}ms`,
      currentSize: metrics.size,
      targetSize,
    };
  }

  getConfig(): AutoScaleConfig {
    return { ...this.config };
  }

  getCurrentMaxSize(): number {
    return this.currentMaxSize;
  }

  setMaxSize(size: number): void {
    this.currentMaxSize = Math.min(Math.max(size, this.config.minProcesses), this.config.maxProcesses);
  }

  getConfigMax(): number {
    return this.configMax;
  }

  getAutoScaleMax(): number {
    return this.config.maxProcesses;
  }
}