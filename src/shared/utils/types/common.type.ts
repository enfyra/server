export type TEntitySchemaIn = {
  id: number | string;
  name: string;
  columns: {
    id: number | string;
    name: string;
    type: string;
    isGenerated: boolean;
    isPrimary: boolean;
    isNullable: boolean;
  }[];
  relations: {
    id: number;
    sourceColumn: string;
    targetColumn: string;
    targetTable: string;
    inverseProperty?: string;
    type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
    propertyName: string;
  }[];
};

export type DBToTSTypeMap = {
  int: 'number';
  integer: 'number';
  smallint: 'number';
  bigint: 'number';
  decimal: 'number';
  numeric: 'number';
  float: 'number';
  real: 'number';
  double: 'number';

  varchar: 'string';
  text: 'string';
  char: 'string';
  uuid: 'string';

  boolean: 'boolean';
  bool: 'boolean';

  date: 'Date';
  timestamp: 'Date';
  timestamptz: 'Date';
  time: 'Date';
  json: 'string';
  jsonb: 'string';
};

export type TSToDBTypeMap = {
  number: 'int';
  string: 'varchar';
  boolean: 'boolean';
  Date: 'timestamp';
  any: 'simple-json';
};

export type TInverseRelation = {
  propertyName: string;
  type: string;
  isEager?: boolean;
  isNullable?: boolean;
  isIndex?: boolean;
  inversePropertyName: string;
  targetClass: string;
  targetGraphQLType?: string;
};

export type TInverseRelationMap = Map<string, TInverseRelation[]>;

export type TReloadSchema = {
  node_name: string;
  sourceInstanceId: string;
  event: 'schema-updated';
  version: number;
};
