import {
  SQL_MASTER_RATIO,
  SQL_COORD_SAFETY_BUFFER_RATIO,
  SQL_COORD_SAFETY_BUFFER_MIN,
} from '../../../shared/utils/auto-scaling.constants';

export function computeCoordinatedPoolMax(params: {
  serverMaxConnections: number;
  activeInstanceCount: number;
  reserveConnections: number;
  externalConnectionsUsed?: number;
  ownConnectionsUsed?: number;
}): number {
  const n = Math.max(1, Math.trunc(params.activeInstanceCount) || 1);
  const serverMax = Math.max(1, Math.trunc(params.serverMaxConnections) || 1);
  const reserve = Math.max(0, Math.trunc(params.reserveConnections) || 0);

  const externalUsed = Math.max(0, Math.trunc(params.externalConnectionsUsed ?? 0));
  const ownUsed = Math.max(0, Math.trunc(params.ownConnectionsUsed ?? 0));
  const othersUsed = Math.max(0, externalUsed - ownUsed);

  const safetyBuffer = Math.max(
    SQL_COORD_SAFETY_BUFFER_MIN,
    Math.floor(serverMax * SQL_COORD_SAFETY_BUFFER_RATIO),
  );

  const effectiveReserve = Math.max(reserve, othersUsed + safetyBuffer);
  const budget = Math.max(2, serverMax - effectiveReserve);
  return Math.max(2, Math.floor(budget / n));
}

export function splitSqlPoolAcrossReplication(params: {
  totalMax: number;
  totalMin: number;
  replicaCount: number;
  masterRatio?: number;
}): {
  masterMin: number;
  masterMax: number;
  replicaMin: number;
  replicaMax: number;
} {
  const totalMax = Math.max(1, Math.trunc(params.totalMax));
  const totalMin = Math.max(1, Math.min(Math.trunc(params.totalMin) || 1, totalMax));
  const replicaCount = Math.max(0, Math.trunc(params.replicaCount));
  const mr = params.masterRatio ?? SQL_MASTER_RATIO;

  if (replicaCount === 0) {
    return {
      masterMin: Math.min(totalMin, totalMax),
      masterMax: totalMax,
      replicaMin: 1,
      replicaMax: 1,
    };
  }

  let masterMax = Math.max(1, Math.floor(totalMax * mr));
  let replicaMax = Math.max(1, Math.floor((totalMax * (1 - mr)) / replicaCount));
  let sum = masterMax + replicaMax * replicaCount;
  while (sum > totalMax) {
    if (replicaMax > 1) {
      replicaMax--;
    } else if (masterMax > 1) {
      masterMax--;
    } else {
      break;
    }
    sum = masterMax + replicaMax * replicaCount;
  }
  while (sum < totalMax) {
    masterMax++;
    sum = masterMax + replicaMax * replicaCount;
  }
  masterMax = Math.min(masterMax, totalMax - replicaMax * replicaCount);

  const masterMin = Math.max(1, Math.min(Math.max(1, Math.floor(totalMin * mr)), masterMax));
  const replicaMin = Math.max(
    1,
    Math.min(Math.max(1, Math.floor((totalMin * (1 - mr)) / replicaCount)), replicaMax),
  );

  return { masterMin, masterMax, replicaMin, replicaMax };
}
