import { WhereCondition } from '../../../../../shared/types/query-builder.types';
import { getMongoFoldTextSearchJs } from '../../../../../shared/utils/mongo-fold-text-search';
import { convertValueByType } from './type-converter';

const MONGO_FOLD_PENDING = '__mongoFoldTextTriples';

type FoldTriple = {
  ref: string;
  needle: string;
  mode: 'contains' | 'starts' | 'ends';
};

function mongoFieldRefForFoldExpr(fieldName: string): string {
  return fieldName === '_id' ? '$_id' : `$${fieldName}`;
}

function appendFoldTextSearchExpr(
  container: Record<string, unknown>,
  fieldName: string,
  needle: string,
  mode: 'contains' | 'starts' | 'ends',
): void {
  const ref = mongoFieldRefForFoldExpr(fieldName);
  const list =
    (container[MONGO_FOLD_PENDING] as FoldTriple[] | undefined) ?? [];
  list.push({ ref, needle, mode });
  container[MONGO_FOLD_PENDING] = list;
}

function flushFoldTextSearchExpr(container: Record<string, unknown>): void {
  const list = container[MONGO_FOLD_PENDING] as FoldTriple[] | undefined;
  delete container[MONGO_FOLD_PENDING];
  if (!list?.length) {
    return;
  }
  const args: unknown[] = [];
  for (const t of list) {
    args.push(t.ref, t.needle, t.mode);
  }
  const expr = {
    $eq: [
      {
        $function: {
          body: getMongoFoldTextSearchJs(),
          args,
          lang: 'js',
        },
      },
      true,
    ],
  };
  const prev = container.$expr;
  if (prev) {
    container.$expr = { $and: [prev, expr] };
  } else {
    container.$expr = expr;
  }
}

export function whereToMongoFilter(
  metadata: any,
  conditions: WhereCondition[],
  tableName?: string,
  dbType?: string,
): any {
  const filter: any = {};
  const isMongo = dbType === 'mongodb' || !dbType;

  for (const condition of conditions) {
    let fieldName = condition.field.includes('.')
      ? (condition.field.split('.').pop() ?? condition.field)
      : condition.field;
    const tableNameForConversion =
      tableName || condition.field.split('.')[0] || '';

    if (isMongo && fieldName === 'id') {
      fieldName = '_id';
    }

    const value = convertValueByType(
      metadata,
      tableNameForConversion,
      fieldName,
      condition.value,
    );

    switch (condition.operator) {
      case '=':
        filter[fieldName] = value;
        break;
      case '!=':
        filter[fieldName] = { $ne: value };
        break;
      case '>':
        filter[fieldName] = { $gt: value };
        break;
      case '<':
        filter[fieldName] = { $lt: value };
        break;
      case '>=':
        filter[fieldName] = { $gte: value };
        break;
      case '<=':
        filter[fieldName] = { $lte: value };
        break;
      case 'like':
        filter[fieldName] = {
          $regex: value.replace(/%/g, '.*'),
          $options: 'i',
        };
        break;
      case 'in':
        let inValues = condition.value;
        if (!Array.isArray(inValues)) {
          inValues =
            typeof inValues === 'string' && inValues.includes(',')
              ? inValues.split(',').map((v: string) => v.trim())
              : [inValues];
        }
        const convertedInValues = inValues.map((v: any) =>
          convertValueByType(metadata, tableNameForConversion, fieldName, v),
        );
        filter[fieldName] = { $in: convertedInValues };
        break;
      case 'not in':
        let notInValues = condition.value;
        if (!Array.isArray(notInValues)) {
          notInValues =
            typeof notInValues === 'string' && notInValues.includes(',')
              ? notInValues.split(',').map((v: string) => v.trim())
              : [notInValues];
        }
        const convertedNotInValues = notInValues.map((v: any) =>
          convertValueByType(metadata, tableNameForConversion, fieldName, v),
        );
        filter[fieldName] = { $nin: convertedNotInValues };
        break;
      case 'is null':
        filter[fieldName] = null;
        break;
      case 'is not null':
        filter[fieldName] = { $ne: null };
        break;
      case '_contains':
        appendFoldTextSearchExpr(filter, fieldName, String(value), 'contains');
        break;
      case '_starts_with':
        appendFoldTextSearchExpr(filter, fieldName, String(value), 'starts');
        break;
      case '_ends_with':
        appendFoldTextSearchExpr(filter, fieldName, String(value), 'ends');
        break;
      case '_between':
        let betweenValues = condition.value;
        if (typeof betweenValues === 'string') {
          betweenValues = betweenValues.split(',').map((v) => v.trim());
        }
        if (Array.isArray(betweenValues) && betweenValues.length === 2) {
          const val0 = convertValueByType(
            metadata,
            tableNameForConversion,
            fieldName,
            betweenValues[0],
          );
          const val1 = convertValueByType(
            metadata,
            tableNameForConversion,
            fieldName,
            betweenValues[1],
          );
          filter[fieldName] = { $gte: val0, $lte: val1 };
        }
        break;
      case '_is_null':
        const isNullBool = value === true || value === 'true';
        filter[fieldName] = isNullBool ? { $eq: null } : { $ne: null };
        break;
      case '_is_not_null':
        const isNotNullBool = value === true || value === 'true';
        filter[fieldName] = isNotNullBool ? { $ne: null } : { $eq: null };
        break;
    }
  }

  flushFoldTextSearchExpr(filter);
  return filter;
}

function isNullableColumn(
  metadata: any,
  tableName: string,
  field: string,
): boolean {
  const tableMeta = metadata?.tables?.get?.(tableName);
  const col = tableMeta?.columns?.find((c: any) => c.name === field);
  if (!col) {
    return true;
  }
  return col.isNullable !== false;
}

export function applyOperatorToMatch(
  metadata: any,
  matchCondition: any,
  tableName: string,
  field: string,
  op: string,
  val: any,
): void {
  switch (op) {
    case '_contains':
      appendFoldTextSearchExpr(matchCondition, field, String(val), 'contains');
      break;
    case '_starts_with':
      appendFoldTextSearchExpr(matchCondition, field, String(val), 'starts');
      break;
    case '_ends_with':
      appendFoldTextSearchExpr(matchCondition, field, String(val), 'ends');
      break;
    case '_eq':
      matchCondition[field] = convertValueByType(
        metadata,
        tableName,
        field,
        val,
      );
      break;
    case '_neq': {
      const v = convertValueByType(metadata, tableName, field, val);
      if (isNullableColumn(metadata, tableName, field)) {
        if (!matchCondition.$and) {
          matchCondition.$and = [];
        }
        matchCondition.$and.push({ [field]: { $ne: null } });
        matchCondition.$and.push({ [field]: { $ne: v } });
      } else {
        matchCondition[field] = { $ne: v };
      }
      break;
    }
    case '_in': {
      let inValues = val;
      if (!Array.isArray(inValues)) {
        inValues =
          typeof inValues === 'string' && inValues.includes(',')
            ? inValues.split(',').map((v: string) => v.trim())
            : [inValues];
      }
      const convertedInValues = inValues.map((v: any) =>
        convertValueByType(metadata, tableName, field, v),
      );
      matchCondition[field] = { $in: convertedInValues };
      break;
    }
    case '_not_in':
    case '_nin':
      let notInValuesNin = val;
      if (!Array.isArray(notInValuesNin)) {
        notInValuesNin =
          typeof notInValuesNin === 'string' && notInValuesNin.includes(',')
            ? notInValuesNin.split(',').map((v: string) => v.trim())
            : [notInValuesNin];
      }
      const convertedNotInVals = notInValuesNin.map((v: any) =>
        convertValueByType(metadata, tableName, field, v),
      );
      if (isNullableColumn(metadata, tableName, field)) {
        if (!matchCondition.$and) {
          matchCondition.$and = [];
        }
        matchCondition.$and.push({ [field]: { $ne: null } });
        matchCondition.$and.push({ [field]: { $nin: convertedNotInVals } });
      } else {
        matchCondition[field] = { $nin: convertedNotInVals };
      }
      break;
    case '_gt':
      matchCondition[field] = {
        $gt: convertValueByType(metadata, tableName, field, val),
      };
      break;
    case '_gte':
      matchCondition[field] = {
        $gte: convertValueByType(metadata, tableName, field, val),
      };
      break;
    case '_lt':
      matchCondition[field] = {
        $lt: convertValueByType(metadata, tableName, field, val),
      };
      break;
    case '_lte':
      matchCondition[field] = {
        $lte: convertValueByType(metadata, tableName, field, val),
      };
      break;
    case '_is_null':
      const isNullMatch = val === true || val === 'true';
      matchCondition[field] = isNullMatch ? { $eq: null } : { $ne: null };
      break;
    case '_is_not_null':
      const isNotNullMatch = val === true || val === 'true';
      matchCondition[field] = isNotNullMatch ? { $ne: null } : { $eq: null };
      break;
    case '_between':
      let betweenVals = val;
      if (typeof betweenVals === 'string') {
        betweenVals = betweenVals.split(',').map((v) => v.trim());
      }
      if (Array.isArray(betweenVals) && betweenVals.length === 2) {
        const val0 = convertValueByType(
          metadata,
          tableName,
          field,
          betweenVals[0],
        );
        const val1 = convertValueByType(
          metadata,
          tableName,
          field,
          betweenVals[1],
        );
        matchCondition[field] = { $gte: val0, $lte: val1 };
      }
      break;
  }
}
