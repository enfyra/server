import { Project, QuoteKind } from 'ts-morph';
import fs from 'fs';
import * as path from 'path';
const metadata = JSON.parse(
  fs.readFileSync(path.resolve(process.cwd(), 'data/snapshot.json'), 'utf8'),
);

const capitalize = (str) => str?.charAt(0).toUpperCase() + str?.slice(1);

const dbTypeToTSType = (dbType) => {
  const map = {
    int: 'number',
    integer: 'number',
    smallint: 'number',
    bigint: 'number',
    decimal: 'number',
    numeric: 'number',
    float: 'number',
    double: 'number',
    real: 'number',
    boolean: 'boolean',
    bool: 'boolean',
    varchar: 'string',
    text: 'string',
    uuid: 'string',
    enum: 'string',
    'simple-json': 'any',
  };
  return map[dbType] || 'any';
};

function buildInverseRelationMap() {
  const inverseMap = new Map();

  for (const [tableName, def] of Object.entries(metadata)) {
    for (const rel of def.relations || []) {
      if (!rel.inversePropertyName) continue;
      const target = rel.targetTable;
      if (!metadata[target]) continue;

      const targetList = inverseMap.get(target) || [];
      let inverseType = 'one-to-one';
      if (rel.type === 'many-to-many') inverseType = 'many-to-many';
      else if (rel.type === 'many-to-one') inverseType = 'one-to-many';
      else if (rel.type === 'one-to-many') inverseType = 'many-to-one';

      targetList.push({
        type: inverseType,
        targetClass: capitalize(tableName),
        propertyName: rel.inversePropertyName,
        inversePropertyName: rel.propertyName,
      });

      inverseMap.set(target, targetList);
    }
  }

  return inverseMap;
}

async function writeEntitiesWithTsMorph() {
  const inverseMap = buildInverseRelationMap();
  const project = new Project({
    useInMemoryFileSystem: true,
    manipulationSettings: {
      quoteKind: QuoteKind.Single,
    },
  });

  for (const [name, def] of Object.entries(metadata)) {
    const className = capitalize(name);
    const sourceFile = project.createSourceFile(`${name}.entity.ts`, '', {
      overwrite: true,
    });

    const usedImports = new Set([
      'Entity',
      'Column',
      'CreateDateColumn',
      'UpdateDateColumn',
    ]);

    const classDecorators = [{ name: 'Entity', arguments: [`'${name}'`] }];
    const classDeclaration = sourceFile.addClass({
      name: className,
      isExported: true,
      decorators: classDecorators,
    });

    for (const col of def.columns) {
      const decorators = [];

      if (col.isPrimary && col.isGenerated) {
        decorators.push({
          name: 'PrimaryGeneratedColumn',
          arguments: [col.type === 'uuid' ? "'uuid'" : "'increment'"],
        });
        usedImports.add('PrimaryGeneratedColumn');
      } else {
        const options = [`type: '${col.type}'`];
        if (col.isNullable !== undefined)
          options.push(`nullable: ${col.isNullable}`);
        if (col.isUnique) options.push(`unique: true`);
        if (col.default !== undefined && col.default !== null) {
          options.push(
            typeof col.default === 'string'
              ? `default: '${col.default}'`
              : `default: ${col.default}`,
          );
        }
        if (col.type === 'enum' && Array.isArray(col.enumValues)) {
          options.push(
            `enum: [${col.enumValues.map((v) => `'${v}'`).join(', ')}]`,
          );
        }
        if (col.isUpdatable === false) options.push(`update: false`);
        decorators.push({
          name: 'Column',
          arguments: [`{ ${options.join(', ')} }`],
        });
        usedImports.add('Column');
      }

      classDeclaration.addProperty({
        name: col.name,
        type: dbTypeToTSType(col.type),
        decorators,
      });
    }

    const allRelations = [
      ...(def.relations || []),
      ...(inverseMap.get(name) || []),
    ];

    for (const rel of allRelations) {
      const target = rel.targetTable
        ? capitalize(rel.targetTable)
        : rel.targetClass;
      const relType = {
        'many-to-one': 'ManyToOne',
        'one-to-one': 'OneToOne',
        'one-to-many': 'OneToMany',
        'many-to-many': 'ManyToMany',
      }[rel.type];

      usedImports.add(relType);

      const cascadeOpts = ['onDelete: "CASCADE"', 'onUpdate: "CASCADE"'];
      const isInverse = !!rel.targetClass;

      if (
        (!isInverse && ['many-to-many', 'many-to-one'].includes(rel.type)) ||
        (isInverse && ['many-to-many', 'one-to-many'].includes(rel.type))
      ) {
        cascadeOpts.unshift('cascade: true');
      }

      const opts = `{ ${cascadeOpts.join(', ')} }`;

      const args = [`() => ${target}`];
      if (rel.inversePropertyName) {
        args.push(`(rel) => rel.${rel.inversePropertyName}`);
      }
      args.push(opts);

      const decorators = [];

      if (
        rel.isIndex &&
        (rel.type === 'many-to-one' || rel.type === 'one-to-one') &&
        !isInverse
      ) {
        decorators.push({ name: 'Index', arguments: [] });
        usedImports.add('Index');
      }

      decorators.push({ name: relType, arguments: args });

      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        decorators.push({ name: 'JoinColumn', arguments: [] });
        usedImports.add('JoinColumn');
      }

      if (rel.type === 'many-to-many' && !isInverse) {
        decorators.push({ name: 'JoinTable', arguments: [] });
        usedImports.add('JoinTable');
      }

      classDeclaration.addProperty({
        name: rel.propertyName,
        type: ['many-to-many', 'one-to-many'].includes(rel.type)
          ? `${target}[]`
          : target,
        decorators,
      });
    }

    classDeclaration.addProperty({
      name: 'createdAt',
      type: 'Date',
      decorators: [{ name: 'CreateDateColumn', arguments: [] }],
    });

    classDeclaration.addProperty({
      name: 'updatedAt',
      type: 'Date',
      decorators: [{ name: 'UpdateDateColumn', arguments: [] }],
    });

    sourceFile.addImportDeclaration({
      namedImports: Array.from(usedImports).sort(),
      moduleSpecifier: 'typeorm',
    });
  }

  await Promise.all(project.getSourceFiles().map((file) => file.save()));
  console.log('✅ Entity generation completed.');
}

writeEntitiesWithTsMorph();
