import type {
  SnapshotColumnDefinition,
  SnapshotJunctionOptions,
  SnapshotOnDelete,
  SnapshotRelationDefinition,
  SnapshotRelationType,
  SnapshotTableDefinition,
  SnapshotTableOptions,
} from '../types';
import type { ColumnTypeDeclaration } from '../../../shared/types/column-type.types';
import {
  MONGO_PRIMARY_KEY_TYPE,
  toMongoTypeForSqlType,
} from '../../../shared/types/column-type.types';

export class SnapshotColumnBuilder {
  private readonly definition: Omit<SnapshotColumnDefinition, 'name'>;
  private mongoTypeDeclared = false;

  constructor(sqlType: ColumnTypeDeclaration) {
    this.definition = {
      sqlType,
      mongoType: {
        type: toMongoTypeForSqlType(sqlType.type),
        ...(sqlType.options !== undefined ? { options: sqlType.options } : {}),
      },
    };
  }

  primary(value = true): this {
    this.definition.isPrimary = value;
    // MongoDB has no integer primary key: identity is always an ObjectId. An
    // explicit `.mongoType()` still wins, since the author stated it outright.
    if (value && !this.mongoTypeDeclared) {
      this.definition.mongoType = { type: MONGO_PRIMARY_KEY_TYPE };
    }
    return this;
  }

  generated(value = true): this {
    this.definition.isGenerated = value;
    return this;
  }

  nullable(): this {
    this.definition.isNullable = true;
    return this;
  }

  notNull(): this {
    this.definition.isNullable = false;
    return this;
  }

  system(value = true): this {
    this.definition.isSystem = value;
    return this;
  }

  updatable(value = true): this {
    this.definition.isUpdatable = value;
    return this;
  }

  immutable(): this {
    return this.updatable(false);
  }

  published(value = true): this {
    this.definition.isPublished = value;
    return this;
  }

  private(): this {
    return this.published(false);
  }

  encrypted(value = true): this {
    this.definition.isEncrypted = value;
    return this;
  }

  default(value: unknown): this {
    this.definition.defaultValue = value;
    return this;
  }

  description(value: string): this {
    this.definition.description = value;
    return this;
  }

  placeholder(value: string): this {
    this.definition.placeholder = value;
    return this;
  }

  mongoType(declaration: ColumnTypeDeclaration): this {
    this.definition.mongoType = declaration;
    this.mongoTypeDeclared = true;
    return this;
  }

  build(name: string): SnapshotColumnDefinition {
    return { name, ...this.definition };
  }
}

export class SnapshotRelationBuilder {
  private readonly definition: Omit<SnapshotRelationDefinition, 'propertyName'>;

  constructor(type: SnapshotRelationType, targetTable: string) {
    this.definition = { type, targetTable };
  }

  inverse(propertyName: string): this {
    this.definition.inversePropertyName = propertyName;
    return this;
  }

  nullable(): this {
    this.definition.isNullable = true;
    return this;
  }

  notNull(): this {
    this.definition.isNullable = false;
    return this;
  }

  system(value = true): this {
    this.definition.isSystem = value;
    return this;
  }

  generated(value = true): this {
    this.definition.isGenerated = value;
    return this;
  }

  updatable(value = true): this {
    this.definition.isUpdatable = value;
    return this;
  }

  immutable(): this {
    return this.updatable(false);
  }

  published(value = true): this {
    this.definition.isPublished = value;
    return this;
  }

  onDelete(value: SnapshotOnDelete): this {
    this.definition.onDelete = value;
    return this;
  }

  description(value: string): this {
    this.definition.description = value;
    return this;
  }

  foreignKey(column: string, referencedColumn?: string): this {
    this.definition.foreignKeyColumn = column;
    if (referencedColumn !== undefined) {
      this.definition.referencedColumn = referencedColumn;
    }
    return this;
  }

  constraint(name: string): this {
    this.definition.constraintName = name;
    return this;
  }

  junction(options: SnapshotJunctionOptions): this {
    this.definition.junctionTableName = options.table;
    this.definition.junctionSourceColumn = options.source;
    this.definition.junctionTargetColumn = options.target;
    return this;
  }

  metadata(value: unknown): this {
    this.definition.metadata = value;
    return this;
  }

  build(propertyName: string): SnapshotRelationDefinition {
    return { propertyName, ...this.definition };
  }
}

export class SnapshotTableBuilder {
  constructor(private readonly definition: SnapshotTableDefinition) {}

  columns(
    definitions: Record<string, SnapshotColumnBuilder>,
  ): SnapshotTableBuilder {
    this.definition.columns = Object.entries(definitions).map(
      ([name, definition]) => definition.build(name),
    );
    return this;
  }

  relations(
    definitions: Record<string, SnapshotRelationBuilder>,
  ): SnapshotTableBuilder {
    this.definition.relations = Object.entries(definitions).map(
      ([propertyName, definition]) => definition.build(propertyName),
    );
    return this;
  }

  uniques(groups: string[][]): SnapshotTableBuilder {
    this.definition.uniques = groups;
    return this;
  }

  indexes(groups: string[][]): SnapshotTableBuilder {
    this.definition.indexes = groups;
    return this;
  }
}

export class SnapshotDefinition {
  private readonly definitions: Record<string, SnapshotTableDefinition> = {};

  table(
    name: string,
    options: SnapshotTableOptions = {},
  ): SnapshotTableBuilder {
    if (this.definitions[name]) {
      throw new Error(`Snapshot table ${name} is already defined`);
    }
    const definition: SnapshotTableDefinition = {
      name,
      columns: [],
    };
    if (options.description !== undefined) {
      definition.description = options.description;
    }
    if (options.system !== undefined) definition.isSystem = options.system;
    if (options.singleRecord !== undefined) {
      definition.isSingleRecord = options.singleRecord;
    }
    if (options.alias !== undefined) definition.alias = options.alias;
    if (options.metadata !== undefined) definition.metadata = options.metadata;
    if (options.validateBody !== undefined) {
      definition.validateBody = options.validateBody;
    }
    this.definitions[name] = definition;
    return new SnapshotTableBuilder(definition);
  }

  build(): Record<string, SnapshotTableDefinition> {
    return this.definitions;
  }
}

function column(sqlType: ColumnTypeDeclaration) {
  return new SnapshotColumnBuilder(sqlType);
}

function relation(type: SnapshotRelationType, targetTable: string) {
  return new SnapshotRelationBuilder(type, targetTable);
}

export const col = {
  sqlType: (declaration: ColumnTypeDeclaration) => column(declaration),
};

export const rel = {
  oneToOne: (targetTable: string) => relation('one-to-one', targetTable),
  manyToOne: (targetTable: string) => relation('many-to-one', targetTable),
  oneToMany: (targetTable: string) => relation('one-to-many', targetTable),
  manyToMany: (targetTable: string) => relation('many-to-many', targetTable),
};
