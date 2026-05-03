import { SQL_MASTER_RATIO } from '../../../shared/utils/auto-scaling.constants';

export function computeCoordinatedPoolMax(params: {
  serverMaxConnections: number;
  activeInstanceCount: number;
  reserveConnections: number;
  maxPoolPerInstance?: number;
}): number {
  const n = Math.max(1, Math.trunc(params.activeInstanceCount) || 1);
  const serverMax = Math.max(1, Math.trunc(params.serverMaxConnections) || 1);
  const reserve = Math.max(0, Math.trunc(params.reserveConnections) || 0);
  const budget = Math.max(1, serverMax - reserve);
  const cap =
    params.maxPoolPerInstance == null
      ? Number.POSITIVE_INFINITY
      : Math.max(1, Math.trunc(params.maxPoolPerInstance));
  return Math.max(1, Math.min(cap, Math.floor(budget / n)));
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
  const requestedTotalMax = Math.max(1, Math.trunc(params.totalMax));
  const replicaCount = Math.max(0, Math.trunc(params.replicaCount));
  const totalMax =
    replicaCount === 0
      ? requestedTotalMax
      : Math.max(requestedTotalMax, 1 + replicaCount);
  const requestedMin = Math.trunc(params.totalMin);
  const totalMin = Math.max(
    0,
    Math.min(Number.isFinite(requestedMin) ? requestedMin : 0, totalMax),
  );
  const mr = params.masterRatio ?? SQL_MASTER_RATIO;

  if (replicaCount === 0) {
    return {
      masterMin: Math.min(totalMin, totalMax),
      masterMax: totalMax,
      replicaMin: 0,
      replicaMax: 1,
    };
  }

  let masterMax = Math.max(1, Math.floor(totalMax * mr));
  let replicaMax = Math.max(
    1,
    Math.floor((totalMax * (1 - mr)) / replicaCount),
  );
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

  const masterMin = Math.min(Math.floor(totalMin * mr), masterMax);
  const remainingMin = Math.max(0, totalMin - masterMin);
  const replicaMin = Math.min(
    Math.floor(remainingMin / replicaCount),
    replicaMax,
  );

  return { masterMin, masterMax, replicaMin, replicaMax };
}
