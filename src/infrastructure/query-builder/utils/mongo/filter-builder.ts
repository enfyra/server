import { WhereCondition } from '../../../../shared/types/query-builder.types';
import { convertValueByType } from './type-converter';

export function whereToMongoFilter(
  metadata: any,
  conditions: WhereCondition[],
  tableName?: string,
  dbType?: string
): any {
  const filter: any = {};
  const isMongo = dbType === 'mongodb' || !dbType;

  for (const condition of conditions) {
    let fieldName = condition.field.includes('.') ? condition.field.split('.').pop() : condition.field;
    const tableNameForConversion = tableName || condition.field.split('.')[0];

    if (isMongo && fieldName === 'id') {
      fieldName = '_id';
    }

    let value = convertValueByType(metadata, tableNameForConversion, fieldName, condition.value);

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
        filter[fieldName] = { $regex: value.replace(/%/g, '.*'), $options: 'i' };
        break;
      case 'in':
        let inValues = condition.value;
        if (!Array.isArray(inValues)) {
          inValues = typeof inValues === 'string' && inValues.includes(',')
            ? inValues.split(',').map(v => v.trim())
            : [inValues];
        }
        const convertedInValues = inValues.map(v => convertValueByType(metadata, tableNameForConversion, fieldName, v));
        filter[fieldName] = { $in: convertedInValues };
        break;
      case 'not in':
        let notInValues = condition.value;
        if (!Array.isArray(notInValues)) {
          notInValues = typeof notInValues === 'string' && notInValues.includes(',')
            ? notInValues.split(',').map(v => v.trim())
            : [notInValues];
        }
        const convertedNotInValues = notInValues.map(v => convertValueByType(metadata, tableNameForConversion, fieldName, v));
        filter[fieldName] = { $nin: convertedNotInValues };
        break;
      case 'is null':
        filter[fieldName] = null;
        break;
      case 'is not null':
        filter[fieldName] = { $ne: null };
        break;
      case '_contains':
        const escapedContains = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        filter[fieldName] = { $regex: escapedContains, $options: 'i' };
        break;
      case '_starts_with':
        const escapedStarts = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        filter[fieldName] = { $regex: `^${escapedStarts}`, $options: 'i' };
        break;
      case '_ends_with':
        const escapedEnds = String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        filter[fieldName] = { $regex: `${escapedEnds}$`, $options: 'i' };
        break;
      case '_between':
        let betweenValues = condition.value;
        if (typeof betweenValues === 'string') {
          betweenValues = betweenValues.split(',').map(v => v.trim());
        }
        if (Array.isArray(betweenValues) && betweenValues.length === 2) {
          const val0 = convertValueByType(metadata, tableNameForConversion, fieldName, betweenValues[0]);
          const val1 = convertValueByType(metadata, tableNameForConversion, fieldName, betweenValues[1]);
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

  return filter;
}

export function convertLogicalFilterToMongo(
  metadata: any,
  filter: any,
  tableName?: string,
  dbType?: string
): any {
  if (!filter || typeof filter !== 'object') {
    return {};
  }

  const isMongo = dbType === 'mongodb' || !dbType;

  if (filter._and) {
    const conditions = Array.isArray(filter._and)
      ? filter._and
      : Object.values(filter._and);
    return {
      $and: conditions.map((condition: any) => convertLogicalFilterToMongo(metadata, condition, tableName, dbType))
    };
  }

  if (filter._or) {
    const conditions = Array.isArray(filter._or)
      ? filter._or
      : Object.values(filter._or);
    return {
      $or: conditions.map((condition: any) => convertLogicalFilterToMongo(metadata, condition, tableName, dbType))
    };
  }

  if (filter._not) {
    return {
      $nor: [convertLogicalFilterToMongo(metadata, filter._not, tableName, dbType)]
    };
  }

  const mongoFilter: any = {};
  for (const [field, value] of Object.entries(filter)) {
    if (field === '_and' || field === '_or' || field === '_not') {
      continue;
    }

    let fieldName = field;
    if (isMongo && fieldName === 'id') {
      fieldName = '_id';
    }

    if (typeof value === 'object' && value !== null) {
      const firstKey = Object.keys(value)[0];
      const isOperator = firstKey?.startsWith('_');

      if (isOperator) {
        for (const [op, val] of Object.entries(value)) {
          applyOperatorToMatch(metadata, mongoFilter, tableName || '', fieldName, op, val);
        }
      } else {
        mongoFilter[fieldName] = value;
      }
    } else {
      mongoFilter[fieldName] = value;
    }
  }

  return mongoFilter;
}

export function applyOperatorToMatch(
  metadata: any,
  matchCondition: any,
  tableName: string,
  field: string,
  op: string,
  val: any
): void {
  let value = convertValueByType(metadata, tableName, field, val);

  switch (op) {
    case '_contains':
      const escapedContains = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      matchCondition[field] = { $regex: escapedContains, $options: 'i' };
      break;
    case '_starts_with':
      const escapedStarts = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      matchCondition[field] = { $regex: `^${escapedStarts}`, $options: 'i' };
      break;
    case '_ends_with':
      const escapedEnds = String(val).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      matchCondition[field] = { $regex: `${escapedEnds}$`, $options: 'i' };
      break;
    case '_eq':
      matchCondition[field] = value;
      break;
    case '_neq':
      matchCondition[field] = { $ne: value };
      break;
    case '_in':
      let inValues = value;
      if (!Array.isArray(inValues)) {
        inValues = typeof inValues === 'string' && inValues.includes(',')
          ? inValues.split(',').map(v => v.trim())
          : [inValues];
      }
      const convertedInValues = inValues.map(v => convertValueByType(metadata, tableName, field, v));
      matchCondition[field] = { $in: convertedInValues };
      break;
    case '_not_in':
      let notInValues = value;
      if (!Array.isArray(notInValues)) {
        notInValues = typeof notInValues === 'string' && notInValues.includes(',')
          ? notInValues.split(',').map(v => v.trim())
          : [notInValues];
      }
      const convertedNotInValues = notInValues.map(v => convertValueByType(metadata, tableName, field, v));
      matchCondition[field] = { $nin: convertedNotInValues };
      break;
    case '_gt':
      matchCondition[field] = { $gt: value };
      break;
    case '_gte':
      matchCondition[field] = { $gte: value };
      break;
    case '_lt':
      matchCondition[field] = { $lt: value };
      break;
    case '_lte':
      matchCondition[field] = { $lte: value };
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
        betweenVals = betweenVals.split(',').map(v => v.trim());
      }
      if (Array.isArray(betweenVals) && betweenVals.length === 2) {
        const val0 = convertValueByType(metadata, tableName, field, betweenVals[0]);
        const val1 = convertValueByType(metadata, tableName, field, betweenVals[1]);
        matchCondition[field] = { $gte: val0, $lte: val1 };
      }
      break;
  }
}
