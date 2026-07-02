export interface SettingData {
  maxQueryDepth: number;
  maxUploadFileSize: number;
  maxRequestBodySize: number;
  [key: string]: any;
}

export interface TGqlDefinition {
  id: number;
  isEnabled: boolean;
  isSystem: boolean;
  description: string | null;
  metadata: Record<string, any> | null;
  tableName: string;
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
