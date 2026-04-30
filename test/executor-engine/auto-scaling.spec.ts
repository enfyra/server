import { computeEngineTuning } from '@enfyra/kernel';
import {
  computeCoordinatedPoolMax,
  splitSqlPoolAcrossReplication,
} from '../../src/engines/knex';
import {
  WORKER_RSS_HIGH,
  WORKER_RSS_LOW,
  WORKER_CPU_HIGH,
  WORKER_CPU_LOW,
  WORKER_FLOOR,
  WORKER_HYSTERESIS_TICKS,
  SQL_COORD_RESERVE_MIN,
  SQL_COORD_RESERVE_RATIO,
} from '../../src/shared/utils/auto-scaling.constants';

const MB = 1024 * 1024;
const GB = 1024 * MB;

// ────────────────────────────────────────────────────────────────
// Section 1: Handler tuning — exhaustive CPU × RAM sweep (238 combos)
// ────────────────────────────────────────────────────────────────

const ALL_CPUS = [1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128];
const ALL_RAMS_MB = [
  256, 512, 1024, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768,
  49152, 65536, 98304, 131072, 262144,
];

describe('computeEngineTuning — exhaustive CPU×RAM sweep', () => {
  const results: Array<{
    cpus: number;
    ramMb: number;
    workers: number;
    memMb: number;
  }> = [];

  for (const cpus of ALL_CPUS) {
    for (const ramMb of ALL_RAMS_MB) {
      const label = `${cpus} CPU / ${ramMb >= 1024 ? ramMb / 1024 + 'GB' : ramMb + 'MB'}`;
      it(label, () => {
        const r = computeEngineTuning({
          logicalCpuCount: cpus,
          totalMemoryBytes: ramMb * MB,
        });
        results.push({
          cpus,
          ramMb,
          workers: r.maxConcurrentWorkers,
          memMb: r.isolateMemoryLimitMb,
        });

        expect(r.maxConcurrentWorkers).toBeGreaterThanOrEqual(WORKER_FLOOR);
        expect(r.maxConcurrentWorkers).toBeLessThanOrEqual(96);
        expect(r.isolateMemoryLimitMb).toBeGreaterThanOrEqual(16);
        expect(r.isolateMemoryLimitMb).toBeLessThanOrEqual(128);
      });
    }
  }

  it('monotonicity: more RAM (same CPU) → workers >= previous', () => {
    for (const cpus of ALL_CPUS) {
      let prev = 0;
      for (const ramMb of ALL_RAMS_MB) {
        const r = computeEngineTuning({
          logicalCpuCount: cpus,
          totalMemoryBytes: ramMb * MB,
        });
        expect(r.maxConcurrentWorkers).toBeGreaterThanOrEqual(prev);
        prev = r.maxConcurrentWorkers;
      }
    }
  });

  it('monotonicity: more CPU (same RAM) → workers >= previous', () => {
    for (const ramMb of ALL_RAMS_MB) {
      let prev = 0;
      for (const cpus of ALL_CPUS) {
        const r = computeEngineTuning({
          logicalCpuCount: cpus,
          totalMemoryBytes: ramMb * MB,
        });
        expect(r.maxConcurrentWorkers).toBeGreaterThanOrEqual(prev);
        prev = r.maxConcurrentWorkers;
      }
    }
  });

  it('monotonicity: more RAM → isolateMemoryLimitMb >= previous', () => {
    let prev = 0;
    for (const ramMb of ALL_RAMS_MB) {
      const r = computeEngineTuning({
        logicalCpuCount: 4,
        totalMemoryBytes: ramMb * MB,
      });
      expect(r.isolateMemoryLimitMb).toBeGreaterThanOrEqual(prev);
      prev = r.isolateMemoryLimitMb;
    }
  });
});

// ────────────────────────────────────────────────────────────────
// Section 2: SQL Pool Coordination — multi-instance, multi-DB
// ────────────────────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────────
// Section 2: SQL Pool — exhaustive max_connections × instances sweep
// ────────────────────────────────────────────────────────────────

const ALL_MAX_CONN = [
  10, 20, 30, 50, 75, 100, 150, 200, 300, 500, 1000, 2000, 5000,
];
const ALL_INSTANCES = [1, 2, 3, 4, 5, 8, 10, 15, 20, 30, 50];

describe('computeCoordinatedPoolMax — exhaustive sweep', () => {
  function reserve(serverMax: number) {
    return Math.max(
      SQL_COORD_RESERVE_MIN,
      Math.floor(serverMax * SQL_COORD_RESERVE_RATIO),
    );
  }

  for (const serverMax of ALL_MAX_CONN) {
    for (const instances of ALL_INSTANCES) {
      const label = `max_conn=${serverMax}, instances=${instances}`;
      it(label, () => {
        const res = reserve(serverMax);
        const perInstance = computeCoordinatedPoolMax({
          serverMaxConnections: serverMax,
          activeInstanceCount: instances,
          reserveConnections: res,
        });

        expect(perInstance).toBeGreaterThanOrEqual(2);
        expect(perInstance).toBeLessThanOrEqual(serverMax);

        if (serverMax >= instances * 2 + res) {
          const totalUsed = perInstance * instances + res;
          expect(totalUsed).toBeLessThanOrEqual(serverMax + instances);
        }
      });
    }
  }

  it('monotonicity: more instances → per-instance pool <= previous', () => {
    for (const serverMax of ALL_MAX_CONN) {
      const res = reserve(serverMax);
      let prev = Infinity;
      for (const instances of ALL_INSTANCES) {
        const r = computeCoordinatedPoolMax({
          serverMaxConnections: serverMax,
          activeInstanceCount: instances,
          reserveConnections: res,
        });
        expect(r).toBeLessThanOrEqual(prev);
        prev = r;
      }
    }
  });
});

// ────────────────────────────────────────────────────────────────
// Section 2b: Replication split — exhaustive totalMax × replicas sweep
// ────────────────────────────────────────────────────────────────

const ALL_TOTAL_MAX = [2, 3, 5, 8, 10, 15, 20, 30, 50, 80, 100, 200, 500];
const ALL_REPLICAS = [0, 1, 2, 3, 4, 5, 8];

describe('splitSqlPoolAcrossReplication — exhaustive sweep', () => {
  for (const totalMax of ALL_TOTAL_MAX) {
    for (const replicas of ALL_REPLICAS.filter((r) => r <= totalMax - 1)) {
      const label = `totalMax=${totalMax}, replicas=${replicas}`;
      it(label, () => {
        const s = splitSqlPoolAcrossReplication({
          totalMax,
          totalMin: Math.min(2, totalMax),
          replicaCount: replicas,
        });

        expect(s.masterMax).toBeGreaterThanOrEqual(1);
        expect(s.masterMin).toBeGreaterThanOrEqual(1);
        expect(s.masterMin).toBeLessThanOrEqual(s.masterMax);

        if (replicas === 0) {
          expect(s.masterMax).toBe(totalMax);
        } else {
          expect(s.replicaMax).toBeGreaterThanOrEqual(1);
          expect(s.replicaMin).toBeGreaterThanOrEqual(1);
          expect(s.replicaMin).toBeLessThanOrEqual(s.replicaMax);
          const sum = s.masterMax + s.replicaMax * replicas;
          expect(sum).toBeLessThanOrEqual(totalMax);
          expect(sum).toBeGreaterThanOrEqual(1 + replicas);
        }
      });
    }
  }

  it('monotonicity: more totalMax (same replicas) → masterMax >= previous', () => {
    for (const replicas of [1, 2, 3]) {
      let prev = 0;
      for (const totalMax of ALL_TOTAL_MAX.filter((t) => t > replicas)) {
        const s = splitSqlPoolAcrossReplication({
          totalMax,
          totalMin: 1,
          replicaCount: replicas,
        });
        expect(s.masterMax).toBeGreaterThanOrEqual(prev);
        prev = s.masterMax;
      }
    }
  });
});

// ────────────────────────────────────────────────────────────────
// Section 3: Feedback loop simulation (pool resize via autoTune logic)
// ────────────────────────────────────────────────────────────────

describe('Feedback loop simulation', () => {
  function simulateTune(params: {
    ceiling: number;
    initialMax: number;
    ticks: Array<{ rssRatio: number; cpuRatio: number; queueDepth: number }>;
  }): { history: number[]; finalMax: number } {
    const ceiling = params.ceiling;
    let poolMax = params.initialMax;
    let pressureTicks = 0;
    let recoveryTicks = 0;
    const history: number[] = [poolMax];

    for (const tick of params.ticks) {
      const current = poolMax;
      const underPressure =
        tick.rssRatio > WORKER_RSS_HIGH || tick.cpuRatio > WORKER_CPU_HIGH;
      const resourcesOk =
        tick.rssRatio < WORKER_RSS_LOW && tick.cpuRatio < WORKER_CPU_LOW;
      const hasDemand = tick.queueDepth > 0;

      let next = current;

      if (underPressure) {
        recoveryTicks = 0;
        pressureTicks++;
        if (pressureTicks >= WORKER_HYSTERESIS_TICKS) {
          next = Math.max(WORKER_FLOOR, current - 1);
        }
      } else if (resourcesOk && hasDemand) {
        pressureTicks = 0;
        recoveryTicks++;
        if (recoveryTicks >= WORKER_HYSTERESIS_TICKS) {
          next = Math.min(ceiling, current + 2);
        }
      } else {
        pressureTicks = 0;
        recoveryTicks = 0;
      }

      if (next !== current) {
        poolMax = Math.max(1, next);
        pressureTicks = 0;
        recoveryTicks = 0;
      }

      history.push(poolMax);
    }

    return { history, finalMax: poolMax };
  }

  it('steady idle: no changes', () => {
    const { history } = simulateTune({
      ceiling: 14,
      initialMax: 14,
      ticks: Array(20).fill({ rssRatio: 0.4, cpuRatio: 0.2, queueDepth: 0 }),
    });
    expect(new Set(history).size).toBe(1);
    expect(history[0]).toBe(14);
  });

  it('sustained pressure: scales down after 3 ticks', () => {
    const { history } = simulateTune({
      ceiling: 10,
      initialMax: 10,
      ticks: Array(6).fill({ rssRatio: 0.9, cpuRatio: 0.3, queueDepth: 0 }),
    });
    expect(history[0]).toBe(10);
    expect(history[1]).toBe(10);
    expect(history[2]).toBe(10);
    expect(history[3]).toBe(9);
    expect(history[4]).toBe(9);
    expect(history[5]).toBe(9);
    expect(history[6]).toBe(8);
  });

  it('bursty pressure: no oscillation (hysteresis prevents it)', () => {
    const ticks = [];
    for (let i = 0; i < 20; i++) {
      ticks.push(
        i % 2 === 0
          ? { rssRatio: 0.9, cpuRatio: 0.3, queueDepth: 0 }
          : { rssRatio: 0.5, cpuRatio: 0.2, queueDepth: 0 },
      );
    }
    const { history } = simulateTune({
      ceiling: 10,
      initialMax: 10,
      ticks,
    });
    expect(new Set(history).size).toBe(1);
  });

  it('sustained pressure then recovery with demand', () => {
    const ticks = [
      ...Array(5).fill({ rssRatio: 0.9, cpuRatio: 0.3, queueDepth: 0 }),
      ...Array(5).fill({ rssRatio: 0.5, cpuRatio: 0.3, queueDepth: 5 }),
    ];
    const { history, finalMax } = simulateTune({
      ceiling: 10,
      initialMax: 10,
      ticks,
    });
    const minVal = Math.min(...history);
    expect(minVal).toBeLessThan(10);
    expect(finalMax).toBeGreaterThan(minVal);
  });

  it('no demand, no recovery: stays at reduced level', () => {
    const ticks = [
      ...Array(5).fill({ rssRatio: 0.9, cpuRatio: 0.3, queueDepth: 0 }),
      ...Array(10).fill({ rssRatio: 0.4, cpuRatio: 0.2, queueDepth: 0 }),
    ];
    const { history } = simulateTune({
      ceiling: 10,
      initialMax: 10,
      ticks,
    });
    const afterPressure = history.slice(6);
    expect(new Set(afterPressure).size).toBe(1);
  });

  it('CPU pressure on single core: threshold works', () => {
    const { finalMax } = simulateTune({
      ceiling: 8,
      initialMax: 8,
      ticks: Array(6).fill({ rssRatio: 0.5, cpuRatio: 0.8, queueDepth: 0 }),
    });
    expect(finalMax).toBeLessThan(8);
  });

  it('CPU pressure on multi-core: ratio > 1.0 still triggers', () => {
    const { finalMax } = simulateTune({
      ceiling: 14,
      initialMax: 14,
      ticks: Array(6).fill({ rssRatio: 0.5, cpuRatio: 2.5, queueDepth: 0 }),
    });
    expect(finalMax).toBeLessThan(14);
  });

  it('floor: never goes below WORKER_FLOOR', () => {
    const { finalMax } = simulateTune({
      ceiling: 10,
      initialMax: 10,
      ticks: Array(60).fill({
        rssRatio: 0.95,
        cpuRatio: 0.95,
        queueDepth: 0,
      }),
    });
    expect(finalMax).toBe(WORKER_FLOOR);
  });

  it('ceiling: recovery never exceeds ceiling', () => {
    const ticks = [
      ...Array(30).fill({
        rssRatio: 0.95,
        cpuRatio: 0.95,
        queueDepth: 0,
      }),
      ...Array(30).fill({
        rssRatio: 0.3,
        cpuRatio: 0.2,
        queueDepth: 10,
      }),
    ];
    const { finalMax } = simulateTune({
      ceiling: 8,
      initialMax: 8,
      ticks,
    });
    expect(finalMax).toBeLessThanOrEqual(8);
  });

  it('demand-driven scale up: +2 per action', () => {
    const { history } = simulateTune({
      ceiling: 14,
      initialMax: 4,
      ticks: Array(10).fill({
        rssRatio: 0.4,
        cpuRatio: 0.2,
        queueDepth: 5,
      }),
    });
    const firstJump = history.findIndex((v, i) => i > 0 && v > history[i - 1]);
    expect(firstJump).toBe(WORKER_HYSTERESIS_TICKS);
    const jumpSize = history[firstJump] - history[firstJump - 1];
    expect(jumpSize).toBe(2);
  });

  it('realistic traffic pattern: morning ramp → peak → cool down', () => {
    const ticks = [
      ...Array(5).fill({ rssRatio: 0.3, cpuRatio: 0.1, queueDepth: 0 }),
      ...Array(5).fill({ rssRatio: 0.4, cpuRatio: 0.2, queueDepth: 3 }),
      ...Array(5).fill({ rssRatio: 0.6, cpuRatio: 0.4, queueDepth: 8 }),
      ...Array(10).fill({ rssRatio: 0.88, cpuRatio: 0.75, queueDepth: 15 }),
      ...Array(5).fill({ rssRatio: 0.6, cpuRatio: 0.4, queueDepth: 5 }),
      ...Array(10).fill({ rssRatio: 0.35, cpuRatio: 0.15, queueDepth: 0 }),
    ];
    const { history } = simulateTune({
      ceiling: 14,
      initialMax: 14,
      ticks,
    });

    const peakPhaseStart = 15;
    const postPeakValues = history.slice(peakPhaseStart + 3);
    const minDuringPeak = Math.min(...postPeakValues.slice(0, 10));
    expect(minDuringPeak).toBeLessThan(14);

    const idlePhase = history.slice(-5);
    for (const v of idlePhase) {
      expect(v).toBeLessThanOrEqual(14);
      expect(v).toBeGreaterThanOrEqual(WORKER_FLOOR);
    }
  });
});

// ────────────────────────────────────────────────────────────────
// Section 4: End-to-end scenario — multi-instance DB coordination
// ────────────────────────────────────────────────────────────────

describe('Multi-instance coordination scenarios', () => {
  function reserve(serverMax: number) {
    return Math.max(
      SQL_COORD_RESERVE_MIN,
      Math.floor(serverMax * SQL_COORD_RESERVE_RATIO),
    );
  }

  it('scale out 1→5 instances: each instance pool shrinks', () => {
    const serverMax = 200;
    const res = reserve(serverMax);
    const pools: number[] = [];
    for (let i = 1; i <= 5; i++) {
      pools.push(
        computeCoordinatedPoolMax({
          serverMaxConnections: serverMax,
          activeInstanceCount: i,
          reserveConnections: res,
        }),
      );
    }
    for (let i = 1; i < pools.length; i++) {
      expect(pools[i]).toBeLessThanOrEqual(pools[i - 1]);
    }
    expect(pools[4] * 5 + res).toBeLessThanOrEqual(serverMax);
  });

  it('scale in 5→1: pool grows back', () => {
    const serverMax = 200;
    const res = reserve(serverMax);
    const at5 = computeCoordinatedPoolMax({
      serverMaxConnections: serverMax,
      activeInstanceCount: 5,
      reserveConnections: res,
    });
    const at1 = computeCoordinatedPoolMax({
      serverMaxConnections: serverMax,
      activeInstanceCount: 1,
      reserveConnections: res,
    });
    expect(at1).toBeGreaterThan(at5);
  });

  it('replication split + coordination: full pipeline', () => {
    const serverMax = 300;
    const res = reserve(serverMax);
    const perInstance = computeCoordinatedPoolMax({
      serverMaxConnections: serverMax,
      activeInstanceCount: 3,
      reserveConnections: res,
    });

    const split = splitSqlPoolAcrossReplication({
      totalMax: perInstance,
      totalMin: 2,
      replicaCount: 2,
    });

    expect(split.masterMax + split.replicaMax * 2).toBeLessThanOrEqual(
      perInstance,
    );
    expect(split.masterMax).toBeGreaterThan(0);
    expect(split.replicaMax).toBeGreaterThan(0);

    const totalUsedAcrossCluster =
      (split.masterMax + split.replicaMax * 2) * 3 + res;
    expect(totalUsedAcrossCluster).toBeLessThanOrEqual(serverMax + 3);
  });

  it('combined: handler tuning + pool coordination on same hardware', () => {
    const hw = { cpus: 4, ram: 8 * GB };
    const handler = computeEngineTuning({
      logicalCpuCount: hw.cpus,
      totalMemoryBytes: hw.ram,
    });

    const serverMax = 200;
    const pool = computeCoordinatedPoolMax({
      serverMaxConnections: serverMax,
      activeInstanceCount: 3,
      reserveConnections: reserve(serverMax),
    });

    expect(handler.maxConcurrentWorkers).toBeGreaterThanOrEqual(2);
    expect(handler.maxConcurrentWorkers).toBeLessThanOrEqual(16);
    expect(pool).toBeGreaterThanOrEqual(2);
    expect(pool).toBeLessThanOrEqual(200);

    const workerRamMb =
      handler.maxConcurrentWorkers * (handler.isolateMemoryLimitMb + 100);
    const totalRamMb = hw.ram / MB;
    expect(workerRamMb).toBeLessThan(totalRamMb * 0.25);
  });
});

// ────────────────────────────────────────────────────────────────
// Section 7: Overhead / performance
// ────────────────────────────────────────────────────────────────

describe('Overhead checks', () => {
  it('computeEngineTuning is fast (< 1ms)', () => {
    const start = process.hrtime.bigint();
    for (let i = 0; i < 10000; i++) {
      computeEngineTuning({
        logicalCpuCount: 8,
        totalMemoryBytes: 16 * GB,
      });
    }
    const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
    expect(elapsed / 10000).toBeLessThan(1);
  });

  it('computeCoordinatedPoolMax is fast (< 1ms)', () => {
    const start = process.hrtime.bigint();
    for (let i = 0; i < 10000; i++) {
      computeCoordinatedPoolMax({
        serverMaxConnections: 500,
        activeInstanceCount: 5,
        reserveConnections: 25,
      });
    }
    const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
    expect(elapsed / 10000).toBeLessThan(1);
  });

  it('splitSqlPoolAcrossReplication is fast (< 1ms)', () => {
    const start = process.hrtime.bigint();
    for (let i = 0; i < 10000; i++) {
      splitSqlPoolAcrossReplication({
        totalMax: 100,
        totalMin: 2,
        replicaCount: 3,
      });
    }
    const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
    expect(elapsed / 10000).toBeLessThan(1);
  });
});
