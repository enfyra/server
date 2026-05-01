export interface TCreateColumnBody {
  id?: number;
  _id?: any;
  name: string;
  type: string;
  description?: string;
  values?: string[];
  isPrimary?: boolean;
  isGenerated?: boolean;
  isNullable?: boolean;
  default?: any;
  defaultValue?: any;
  index?: boolean;
  isUnique?: boolean;
  isPublished?: boolean;
  isUpdatable?: boolean;
  isIndex?: boolean;
  isSystem?: boolean;
  options?: any;
  metadata?: any;
  placeholder?: string;
}

export interface TRelationIdRef {
  id: number;
  _id?: any;
  name?: string;
}

export interface TCreateRelationBody {
  id?: number;
  _id?: any;
  description?: string;
  targetTable: TRelationIdRef;
  type: 'one-to-one' | 'one-to-many' | 'many-to-one' | 'many-to-many';
  propertyName: string;
  mappedBy?: string;
  inversePropertyName?: string;
  isEager?: boolean;
  isInverseEager?: boolean;
  isNullable?: boolean;
  index?: boolean;
  onDelete?: 'CASCADE' | 'RESTRICT' | 'SET NULL';
  foreignKeyColumn?: string;
  referencedColumn?: string;
  constraintName?: string;
  isIndex?: boolean;
  isSystem?: boolean;
  isPublished?: boolean;
  isUpdatable?: boolean;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
}

export interface TCreateIndexBody {
  value: string[];
}

export interface TCreateUniqueBody {
  value: string[];
}

export interface TCreateTableBody {
  id?: number;
  name: string;
  alias?: string;
  description?: string;
  isSystem?: boolean;
  isSingleRecord?: boolean;
  graphqlEnabled?: boolean;
  validateBody?: boolean;
  indexes?: TCreateIndexBody[];
  uniques?: TCreateUniqueBody[];
  columns: TCreateColumnBody[];
  relations?: TCreateRelationBody[];
}
