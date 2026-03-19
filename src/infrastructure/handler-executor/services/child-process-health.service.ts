import { Injectable } from '@nestjs/common';
import { ChildProcess } from 'child_process';
import {
  HealthStatus,
  HealthCheckStats,
  ChildProcessMetadata,
  HEALTH_CHECK_CONFIG,
} from '../types/health.types';
import { ExecutorHealthLogger } from '../utils/executor-health.logger';

@Injectable()
export class ChildProcessHealthService {
  private processMetadata = new Map<ChildProcess, ChildProcessMetadata>();

  constructor(private readonly logger: ExecutorHealthLogger) {}

  registerProcess(child: ChildProcess): void {
    this.processMetadata.set(child, {
      pid: child.pid ?? 0,
      createdAt: Date.now(),
      executionCount: 0,
      errorCount: 0,
      totalErrors: 0,
      lastExecutionAt: 0,
      lastExecutionTimeMs: 0,
      avgExecutionTimeMs: 0,
      lastError: null,
      lastErrorAt: null,
    });
    this.logger.logProcessCreated(child.pid ?? 0);
  }

  unregisterProcess(child: ChildProcess): void {
    this.processMetadata.delete(child);
  }

  recordExecutionStart(child: ChildProcess): void {
    const metadata = this.processMetadata.get(child);
    if (metadata) {
      metadata.executionCount++;
      metadata.lastExecutionAt = Date.now();
    }
  }

  recordExecutionEnd(child: ChildProcess, durationMs: number, success: boolean): void {
    const metadata = this.processMetadata.get(child);
    if (metadata) {
      metadata.lastExecutionTimeMs = durationMs;
      metadata.avgExecutionTimeMs = metadata.avgExecutionTimeMs * 0.9 + durationMs * 0.1;
      if (success) {
        metadata.errorCount = 0;
      }
    }
  }

  recordError(child: ChildProcess, error: string): void {
    const metadata = this.processMetadata.get(child);
    if (metadata) {
      metadata.errorCount++;
      metadata.totalErrors++;
      metadata.lastError = error;
      metadata.lastErrorAt = Date.now();
      this.logger.logExecutionError(metadata.pid, metadata.errorCount, error);
    }
  }

  checkAndDecide(child: ChildProcess): { shouldRecycle: boolean; reasons: string[] } {
    const metadata = this.processMetadata.get(child);
    const reasons: string[] = [];

    if (!metadata) {
      return { shouldRecycle: true, reasons: ['not_registered'] };
    }

    if (!child.connected || child.killed) {
      return { shouldRecycle: true, reasons: ['disconnected'] };
    }

    if (metadata.executionCount >= HEALTH_CHECK_CONFIG.maxExecutionCount) {
      reasons.push(`execution_count(${metadata.executionCount})`);
    }

    if (metadata.errorCount >= HEALTH_CHECK_CONFIG.maxConsecutiveErrors) {
      reasons.push(`consecutive_errors(${metadata.errorCount})`);
    }

    const ageMs = Date.now() - metadata.createdAt;
    if (ageMs > HEALTH_CHECK_CONFIG.maxAgeMs) {
      reasons.push(`age(${Math.round(ageMs / 60000)}min)`);
    }

    return {
      shouldRecycle: reasons.length > 0,
      reasons,
    };
  }

  shouldRecycle(child: ChildProcess): boolean {
    const metadata = this.processMetadata.get(child);
    if (!metadata) return true;
    if (!child.connected || child.killed) return true;
    if (metadata.errorCount >= HEALTH_CHECK_CONFIG.maxConsecutiveErrors) return true;
    if (metadata.executionCount >= HEALTH_CHECK_CONFIG.maxExecutionCount) return true;
    return false;
  }

  getAllMetadata(): Map<ChildProcess, ChildProcessMetadata> {
    return new Map(this.processMetadata);
  }

  getStats(): HealthCheckStats {
    let healthyCount = 0;
    let unhealthyCount = 0;
    let totalExecutions = 0;
    let totalErrors = 0;

    for (const [child, metadata] of this.processMetadata) {
      totalExecutions += metadata.executionCount;
      totalErrors += metadata.totalErrors;

      if (!child.connected || child.killed || metadata.errorCount >= HEALTH_CHECK_CONFIG.maxConsecutiveErrors) {
        unhealthyCount++;
      } else {
        healthyCount++;
      }
    }

    return {
      totalProcesses: this.processMetadata.size,
      healthyCount,
      degradedCount: 0,
      unhealthyCount,
      totalExecutions,
      totalErrors,
    };
  }

  logRecycle(pid: number, reasons: string[], metadata: ChildProcessMetadata | null): void {
    if (metadata) {
      this.logger.logRecycle(pid, reasons, {
        executionCount: metadata.executionCount,
        errorCount: metadata.errorCount,
        ageMs: Date.now() - metadata.createdAt,
        lastError: metadata.lastError,
      });
    }
  }
}