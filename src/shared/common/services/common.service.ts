// @nestjs packages
import { Injectable } from '@nestjs/common';

// Import helper functions
import * as CommonHelper from '../helpers/common.helper';

@Injectable()
export class CommonService {
  capitalize = CommonHelper.capitalize;
  lowerFirst = CommonHelper.lowerFirst;
  delay = CommonHelper.delay;
  dbTypeToTSType = CommonHelper.dbTypeToTSType;
  tsTypeToDBType = CommonHelper.tsTypeToDBType;
  mapToGraphQLType = CommonHelper.mapToGraphQLType;
  isRouteMatched = CommonHelper.isRouteMatched;
  inverseRelationType = CommonHelper.inverseRelationType;
  assertNoSystemFlagDeep = CommonHelper.assertNoSystemFlagDeep;
  assertNoSystemFlagDeepRecursive = CommonHelper.assertNoSystemFlagDeepRecursive;
  parseRouteParams = CommonHelper.parseRouteParams;
  normalizeRoutePath = CommonHelper.normalizeRoutePath;
  validateIdentifier = CommonHelper.validateIdentifier;
  sanitizeInput = CommonHelper.sanitizeInput;
}