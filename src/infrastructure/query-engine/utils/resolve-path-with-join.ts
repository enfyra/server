import { EntityMetadata } from 'typeorm';
import { lookupFieldOrRelation } from './lookup-field-or-relation';

export function resolvePathWithJoin({
  meta,
  path,
  rootAlias,
  addJoin,
}: {
  meta: EntityMetadata;
  path: string[];
  rootAlias: string;
  addJoin: (path: string[]) => any;
}):
  | {
      alias: string;
      parentAlias: string;
      lastMeta: EntityMetadata;
      lastField: {
        kind: 'field' | 'relation';
        propertyName: string;
        type: string;
        relationType?: string;
      };
    }
  | undefined {
  let currentMeta = meta;
  let currentAlias = rootAlias;
  let parentAlias = rootAlias;

  for (let i = 0; i < path.length; i++) {
    const segment = path[i];
    const found = lookupFieldOrRelation(currentMeta, segment);
    if (!found) return undefined;

    if (found.kind === 'field') {
      if (i !== path.length - 1) {
        throw new Error(
          `Invalid path: "${segment}" is a field on table "${currentMeta.tableName}", but path continues.`,
        );
      }

      return {
        alias: currentAlias,
        parentAlias,
        lastMeta: currentMeta,
        lastField: found,
      };
    }

    const joinPath = path.slice(0, i + 1);
    addJoin(joinPath);

    parentAlias = currentAlias;
    currentAlias = `${currentAlias}_${segment}`;
    currentMeta = currentMeta.connection.getMetadata(found.type); // call getMetadata externally if needed
  }

  return {
    alias: currentAlias,
    parentAlias,
    lastMeta: currentMeta,
    lastField: lookupFieldOrRelation(currentMeta, 'id'),
  };
}
