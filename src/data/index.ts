import type {
  VersionedDataMigration,
  VersionedSchemaMigration,
} from '../shared/types/schema-migration.types';
import type { BootstrapSourceArtifacts } from '../engines/bootstrap/types';
import dataMigration, { standingDataCorrections } from './data-migration';
import defaultData from './default-data';
import snapshot from './snapshot';
import snapshotMigration from './snapshot-migration';

export const bootstrapSourceArtifacts: BootstrapSourceArtifacts = {
  snapshot,
  migrations: snapshotMigration satisfies VersionedSchemaMigration[],
  defaultData,
  dataCorrections: standingDataCorrections satisfies Record<string, any>,
  dataMigrations: dataMigration satisfies VersionedDataMigration[],
};
