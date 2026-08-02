export interface SettingData {
  maxQueryDepth: number;
  maxUploadFileSize: number;
  maxRequestBodySize: number;
  [key: string]: any;
}

import type {
  GraphqlOperationName,
  GraphqlPermissionGrant,
} from '../../../modules/graphql/utils/graphql-access.util';

export interface TGqlDefinition {
  id: string;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  metadata: Record<string, any> | null;
  tableName: string;
  publicOperations: GraphqlOperationName[];
  permissions: GraphqlPermissionGrant[];
}

export interface FolderNode {
  id: string;
  parentId: string | null;
  name: string;
  slug: string;
  order: number;
  icon: string;
  description: string | null;
  children?: FolderNode[];
}

export interface FolderTreeCache {
  folders: Map<string, FolderNode>;
  tree: FolderNode[];
}
