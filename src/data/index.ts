import type { BootstrapSourceArtifacts } from '../engines/bootstrap/types';
import dataMigration from './data-migration';
import defaultData from './default-data';
import snapshot from './snapshot';
import snapshotMigration from './snapshot-migration';

export { dataMigration, defaultData, snapshot, snapshotMigration };

export const bootstrapSourceArtifacts = {
  snapshot,
  migration: snapshotMigration,
  defaultData,
  dataMigration,
} satisfies BootstrapSourceArtifacts;
