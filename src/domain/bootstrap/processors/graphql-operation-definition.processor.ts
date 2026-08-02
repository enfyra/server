import { BaseTableProcessor } from './base-table-processor';
import { DatabaseConfigService } from '../../../shared/services';
import {
  GRAPHQL_OPERATION_NAMES,
  normalizeGraphqlOperation,
} from '../../../modules/graphql/utils/graphql-access.util';

export class GraphQLOperationDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    return records.map((record, index) => {
      const name = normalizeGraphqlOperation(record.name);
      if (!GRAPHQL_OPERATION_NAMES.includes(name)) {
        throw new Error(`Unsupported canonical GraphQL operation: ${name}`);
      }

      const transformed = {
        ...record,
        name,
        isSystem: true,
        order: record.order ?? index * 10,
      };

      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
        if (!transformed.publicGraphqlConfigs)
          transformed.publicGraphqlConfigs = [];
        if (!transformed.graphqlPermissions)
          transformed.graphqlPermissions = [];
      }

      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object {
    return { name: normalizeGraphqlOperation(record.name) };
  }

  protected getCompareFields(): string[] {
    return ['name', 'label', 'description', 'order', 'isSystem'];
  }
}
