import { ClassDeclaration } from 'ts-morph';

interface RelationWriterContext {
  classDeclaration: ClassDeclaration;
  rel: Partial<Record<string, any>>;
  isInverse?: boolean;
  usedImports: Set<string>;
  usedEntityImports: Set<string>;
  helpers: {
    capitalize: (s: string) => string;
  };
}

export function addRelationToClass({
  classDeclaration,
  rel,
  isInverse = false,
  usedImports,
  usedEntityImports,
  helpers,
}: RelationWriterContext): void {
  const typeMap = {
    'many-to-many': 'ManyToMany',
    'one-to-one': 'OneToOne',
    'many-to-one': 'ManyToOne',
    'one-to-many': 'OneToMany',
  };
  const relationType = typeMap[rel.type] || 'ManyToOne';
  usedImports.add(relationType);

  const target = helpers.capitalize(
    rel.targetTable?.name || rel.targetClass || '',
  );
  // No need to import entity since we use string literals in decorators
  // if (target && target !== classDeclaration.getName()) {
  //   usedEntityImports.add(target);
  // }

  const decorators = [];

  // ✅ Auto index for many-to-one only
  // Không thêm @Index cho OneToOne vì @JoinColumn đã tự tạo unique index
  // Note: Field-level @Index() đã được loại bỏ, chỉ sử dụng class-level @Index([...])
  const shouldAddIndex = rel.type === 'many-to-one';
  if (shouldAddIndex) {
    // Thêm field name vào danh sách để generate class-level @Index
    // Logic này sẽ được handle ở entity-writer.ts
  }

  const options: string[] = [];
  if (rel.isEager) options.push('eager: true');
  if (rel.type !== 'one-to-many') {
    // Inverse relations are always nullable
    const nullable = isInverse ? true : (rel.isNullable ?? true);
    options.push(`nullable: ${nullable}`);
  }
  if (
    (rel.type === 'many-to-many' && !isInverse) ||
    rel.type === 'one-to-many' ||
    (rel.type === 'one-to-one' && !isInverse)
  ) {
    options.push('cascade: true');
  }

  // Only apply CASCADE DELETE for many-to-many (join table records)
  // For other relations, use SET NULL or RESTRICT based on nullable constraint
  if (rel.type === 'many-to-many') {
    options.push(`onDelete: 'CASCADE'`, `onUpdate: 'CASCADE'`);
  } else if (
    rel.type === 'many-to-one' ||
    (rel.type === 'one-to-one' && !isInverse)
  ) {
    // For foreign key relations:
    // - If nullable: SET NULL (allow deletion, set FK to null)
    // - If required: RESTRICT (prevent deletion to maintain data integrity)
    if (rel.isNullable === false) {
      options.push(`onDelete: 'RESTRICT'`, `onUpdate: 'CASCADE'`);
    } else {
      options.push(`onDelete: 'SET NULL'`, `onUpdate: 'CASCADE'`);
    }
  }
  // Note: one-to-many doesn't need onDelete/onUpdate as it doesn't have foreign key

  const args = [`'${target}'`];
  if (rel.inversePropertyName) {
    args.push(`(rel: any) => rel.${rel.inversePropertyName}`);
  } else if (rel.type === 'one-to-many') {
    throw new Error('One to many relation must have inversePropertyName');
  }
  if (options.length) {
    args.push(`{ ${options.join(', ')} }`);
  }

  decorators.push({ name: relationType, arguments: args });

  if (rel.type === 'many-to-many' && !isInverse) {
    decorators.push({ name: 'JoinTable', arguments: [] });
    usedImports.add('JoinTable');
  } else if (
    rel.type === 'many-to-one' ||
    (rel.type === 'one-to-one' && !isInverse)
  ) {
    decorators.push({ name: 'JoinColumn', arguments: [] });
    usedImports.add('JoinColumn');
  }

  classDeclaration.addProperty({
    name: rel.propertyName!,
    type: 'any',
    decorators,
  });
}
