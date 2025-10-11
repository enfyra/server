import { QueryEngine } from '../services/query-engine.service';

export async function resolveDeepRelations(options: {
  queryEngine: QueryEngine;
  rows: any[];
  metaData: any;
  deep: Record<string, any>;
  log?: string[];
}) {
  const { queryEngine, rows, metaData, deep, log = [] } = options;

  const metaDeep: Record<string, any[]> = {};

  await Promise.all(
    Object.entries(deep).map(async ([relationName, deepOptions]) => {
      const relationMeta = metaData.relations?.find(
        (r: any) => r.propertyName === relationName,
      );
      if (!relationMeta) return;

      const childTable = relationMeta.targetTableName;
      
      // Determine foreign key based on relation type
      let foreignKey: string;
      if (relationMeta.type === 'many-to-one' || relationMeta.type === 'one-to-one') {
        // Parent has FK - use it
        foreignKey = relationMeta.foreignKeyColumn;
      } else if (relationMeta.type === 'one-to-many') {
        // Child has FK pointing back - need to find inverse relation
        foreignKey = relationMeta.propertyName; // This will be used as the relation name in filter
      } else {
        log.push(
          `! Deep relation "${relationName}" skipped: many-to-many not supported in deep`,
        );
        return;
      }

      if (!foreignKey) {
        log.push(
          `! Deep relation "${relationName}" skipped due to unable to determine foreignKey`,
        );
        return;
      }

      const fields: string[] = Array.isArray(deepOptions?.fields)
        ? [...deepOptions.fields]
        : typeof deepOptions?.fields === 'string'
          ? deepOptions.fields.split(',')
          : ['*'];

      const metaList: any[] = [];

      await Promise.all(
        rows.map(async (row) => {
          try {
            const res = await queryEngine.find({
              tableName: childTable,
              filter: {
                [foreignKey]: {
                  id: { _eq: row.id },
                },
              },
              fields,
              sort: deepOptions?.sort,
              page: deepOptions?.page,
              limit: deepOptions?.limit,
              meta: deepOptions?.meta,
              deep: deepOptions?.deep,
            });

            row[relationName] = res.data ?? [];

            if (res.meta) {
              metaList.push({ id: row.id, ...res.meta });
            }
          } catch (error) {
            row[relationName] = [];
            log.push(
              `! Deep relation "${relationName}" failed with id ${row.id}: ${error.message}`,
            );
          }
        }),
      );

      if (metaList.length > 0) {
        metaDeep[relationName] = metaList;
      }
    }),
  );

  return Object.keys(metaDeep).length ? metaDeep : undefined;
}
