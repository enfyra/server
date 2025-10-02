import 'reflect-metadata';
import { Project, QuoteKind } from 'ts-morph';
import * as fs from 'fs';
import * as path from 'path';
import { DataSource } from 'typeorm';
import * as dotenv from 'dotenv';
dotenv.config();

const metadata = JSON.parse(
  fs.readFileSync(path.resolve(process.cwd(), 'data/snapshot.json'), 'utf8'),
);

function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

const dbTypeToTSType = (dbType: string): string => {
  const map: Record<string, string> = {
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
    'array-select': 'any', // Maps to simple-json in DB, any[] in TS
  };
  return map[dbType] || 'any';
};

function buildInverseRelationMap() {
  const inverseMap = new Map<string, any[]>();

  for (const [tableName, def] of Object.entries(metadata)) {
    for (const rel of (def as any).relations || []) {
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

async function ensureDatabaseExists() {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT =
    Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  const tempDataSource = new DataSource({
    type: DB_TYPE as 'mysql',
    host: DB_HOST,
    port: DB_PORT,
    username: DB_USERNAME,
    password: DB_PASSWORD,
  });

  await tempDataSource.initialize();
  const queryRunner = tempDataSource.createQueryRunner();

  if (DB_TYPE === 'mysql') {
    const checkDb = await tempDataSource.query(`
        SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${DB_NAME}'
    `);
    if (checkDb.length === 0) {
      await tempDataSource.query(
        `CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\``,
      );
      console.log(`✅ MySQL: Created database ${DB_NAME}`);
    } else {
      console.log(`✅ MySQL: Database ${DB_NAME} already exists`);
    }
  } else if (DB_TYPE === 'postgres') {
    const checkDb = await tempDataSource.query(`
        SELECT 1 FROM pg_database WHERE datname = '${DB_NAME}'
    `);
    if (checkDb.length === 0) {
      await tempDataSource.query(
        `CREATE DATABASE "${DB_NAME}" WITH ENCODING 'UTF8'`,
      );
      console.log(`✅ Postgres: Created database ${DB_NAME}`);
    } else {
      console.log(`✅ Postgres: Database ${DB_NAME} already exists`);
    }
  } else {
    throw new Error(`Unsupported DB_TYPE: ${DB_NAME}`);
  }

  await queryRunner.release();
  await tempDataSource.destroy();
}

async function writeEntitiesFromSnapshot() {
  const inverseMap = buildInverseRelationMap();
  const project = new Project({
    manipulationSettings: { quoteKind: QuoteKind.Single },
  });

  // Map để lưu các field cần tạo index cho mỗi table
  const indexFields = new Map<string, string[]>();

  const entitiesDir = path.resolve(process.cwd(), 'src/core/database/entities');
  const distEntitiesDir = path.resolve(
    process.cwd(),
    'dist/src/core/database/entities',
  );

  if (!fs.existsSync(entitiesDir))
    fs.mkdirSync(entitiesDir, { recursive: true });
  if (!fs.existsSync(distEntitiesDir))
    fs.mkdirSync(distEntitiesDir, { recursive: true });

  for (const [tableName, def] of Object.entries(metadata)) {
    const className = capitalize(tableName);
    const sourceFile = project.createSourceFile(
      path.join(entitiesDir, `${tableName}.entity.ts`),
      '',
      { overwrite: true },
    );

    const usedImports = new Set([
      'Entity',
      'Column',
      'CreateDateColumn',
      'UpdateDateColumn',
    ]);

    const classDeclaration = sourceFile.addClass({
      name: className,
      isExported: true,
      decorators: [{ name: 'Entity', arguments: [`'${tableName}'`] }],
    });

    // Collect all unique constraint keys to prevent index duplicates
    const allUniqueKeys = new Set<string>();
    const allIndexKeys = new Set<string>();

    for (const uniqueGroup of (def as any).uniques || []) {
      if (Array.isArray(uniqueGroup) && uniqueGroup.length) {
        // Sort fields để xử lý đảo thứ tự
        const fields = uniqueGroup.slice().sort();
        const key = fields.join('|');

        classDeclaration.addDecorator({
          name: 'Unique',
          arguments: [`[${fields.map((f: string) => `'${f}'`).join(', ')}]`],
        });
        usedImports.add('Unique');

        // Lưu key để check conflict
        allUniqueKeys.add(key);
        allIndexKeys.add(key); // Unique constraints cũng act as indexes
      }
    }

    for (const indexGroup of (def as any).indexes || []) {
      if (Array.isArray(indexGroup) && indexGroup.length > 1) {
        // Sort fields để xử lý đảo thứ tự
        const fields = indexGroup.slice().sort();
        const key = fields.join('|');

        // Skip nếu duplicate index hoặc bị block bởi unique constraint
        if (!allIndexKeys.has(key)) {
          classDeclaration.addDecorator({
            name: 'Index',
            arguments: [`[${fields.map((f: string) => `'${f}'`).join(', ')}]`],
          });
          usedImports.add('Index');
          allIndexKeys.add(key);
        }
      }
    }

    // Thêm class-level @Index cho foreign key fields
    const tableIndexFields = indexFields.get(tableName) || [];
    for (const fieldName of tableIndexFields) {
      // Check xem field có bị block bởi unique constraint đơn lẻ không
      // (Unique cụm vẫn cho phép thêm index riêng lẻ)
      const isBlockedBySingleUnique = allUniqueKeys.has(fieldName);

      // Chỉ thêm nếu field không bị block bởi unique constraint đơn lẻ
      if (!isBlockedBySingleUnique) {
        classDeclaration.addDecorator({
          name: 'Index',
          arguments: [`['${fieldName}']`],
        });
        usedImports.add('Index');
      }
    }

    for (const col of (def as any).columns) {
      const decorators: any[] = [];

      if (col.isPrimary && col.isGenerated) {
        decorators.push({
          name: 'PrimaryGeneratedColumn',
          arguments: [col.type === 'uuid' ? `'uuid'` : `'increment'`],
        });
        usedImports.add('PrimaryGeneratedColumn');
      } else {
        const dbType =
          col.type === 'date'
            ? 'timestamp'
            : col.type === 'richtext' || col.type === 'code'
              ? 'text'
              : col.type === 'array-select'
                ? 'simple-json'
                : col.type;

        const opts = [
          `type: '${dbType}'`,
          `nullable: ${col.isNullable === false ? 'false' : 'true'}`,
        ];

        if (col.isUnique) opts.push('unique: true');

        if (col.defaultValue !== undefined && col.defaultValue !== null) {
          const defaultVal = col.defaultValue;
          const invalidDefault =
            (col.type === 'uuid' && defaultVal === '') ||
            (col.type === 'number' && isNaN(Number(defaultVal))) ||
            (col.type === 'boolean' &&
              typeof defaultVal !== 'boolean' &&
              defaultVal !== 'true' &&
              defaultVal !== 'false');

          if (invalidDefault) {
            console.warn(
              `⚠️ Bỏ qua defaultValue không hợp lệ cho cột "${col.name}"`,
            );
          } else if (
            typeof defaultVal === 'string' &&
            defaultVal.toLowerCase() === 'now'
          ) {
            opts.push(`default: () => 'now()'`);
          } else if (typeof defaultVal === 'string') {
            opts.push(`default: '${defaultVal}'`);
          } else {
            opts.push(`default: ${defaultVal}`);
          }
        }

        if (col.type === 'enum' && Array.isArray(col.options)) {
          opts.push(
            `enum: [${col.options.map((v: string) => `'${v}'`).join(', ')}]`,
          );
        }

        if (col.isUpdatable === false) opts.push('update: false');

        decorators.push({
          name: 'Column',
          arguments: [`{ ${opts.join(', ')} }`],
        });
        usedImports.add('Column');
      }

      classDeclaration.addProperty({
        name: col.name,
        type:
          col.type === 'date'
            ? 'Date'
            : col.type === 'richtext' || col.type === 'code'
              ? 'string'
              : col.type === 'array-select'
                ? 'any[]' // Array type for array-select
                : dbTypeToTSType(col.type),
        decorators,
      });
    }

    const allRelations = [
      ...((def as any).relations || []),
      ...(inverseMap.get(tableName) || []),
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

      const isInverse = !!rel.targetClass;
      const relationOpts = [];

      // Only apply CASCADE DELETE for many-to-many (join table records)
      // For other relations, use SET NULL or RESTRICT based on nullable constraint
      if (rel.type === 'many-to-many') {
        relationOpts.push(
          `onDelete: "${rel.onDelete || 'CASCADE'}"`,
          `onUpdate: "${rel.onUpdate || 'CASCADE'}"`,
        );
      } else if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !isInverse)
      ) {
        // For foreign key relations:
        // - If nullable: SET NULL (allow deletion, set FK to null)
        // - If required: RESTRICT (prevent deletion to maintain data integrity)
        const defaultDelete =
          rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
        relationOpts.push(
          `onDelete: "${rel.onDelete || defaultDelete}"`,
          `onUpdate: "${rel.onUpdate || 'CASCADE'}"`,
        );
      }
      // Note: one-to-many doesn't need onDelete/onUpdate as it doesn't have foreign key

      relationOpts.push(
        `nullable: ${rel.isNullable === false ? 'false' : 'true'}`,
      );

      // Thêm cascade cho ManyToMany và OneToMany
      if (
        (!isInverse && ['many-to-many'].includes(rel.type)) ||
        rel.type === 'one-to-many' ||
        (rel.type === 'one-to-one' && !isInverse)
      ) {
        relationOpts.unshift('cascade: true');
      }

      const args = [`'${target}'`];
      if (rel.inversePropertyName) {
        args.push(`(rel: any) => rel.${rel.inversePropertyName}`);
      }
      args.push(`{ ${relationOpts.join(', ')} }`);

      const decorators: any[] = [];

      // Chỉ thêm Index cho ManyToOne
      // Không thêm @Index cho OneToOne vì @JoinColumn đã tự tạo unique index
      const shouldAddIndex = rel.type === 'many-to-one';

      if (shouldAddIndex) {
        // Thêm field name vào danh sách để generate class-level @Index
        if (!indexFields.has(tableName)) {
          indexFields.set(tableName, []);
        }
        indexFields.get(tableName)!.push(rel.propertyName);
      }

      decorators.push({ name: relType, arguments: args });

      // Chỉ thêm JoinColumn cho owning side của relationship
      // - ManyToOne: luôn cần JoinColumn
      // - OneToOne: chỉ cần JoinColumn cho bên có foreign key (không phải inverse)
      if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !isInverse)
      ) {
        decorators.push({ name: 'JoinColumn', arguments: [] });
        usedImports.add('JoinColumn');
      }

      if (rel.type === 'many-to-many' && !isInverse) {
        decorators.push({ name: 'JoinTable', arguments: [] });
        usedImports.add('JoinTable');
      }

      classDeclaration.addProperty({
        name: rel.propertyName,
        type: 'any',
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

  // Save TypeScript files
  await Promise.all(project.getSourceFiles().map((file) => file.save()));
  console.log('✅ Entity generation completed.');

  // Compile to JavaScript using ts-morph
  await compileEntitiesToJS(project, distEntitiesDir);
  console.log('✅ Entities compiled to JavaScript.');
}

async function compileEntitiesToJS(project: Project, outputDir: string) {
  // Create a new project for compilation
  const compileProject = new Project({
    compilerOptions: {
      target: 3, // ES2020
      module: 1, // CommonJS
      strict: true,
      esModuleInterop: true,
      emitDecoratorMetadata: true,
      experimentalDecorators: true,
      skipLibCheck: true,
      declaration: false,
      outDir: outputDir,
    },
    useInMemoryFileSystem: true,
  });

  // Add all source files from the original project
  for (const sourceFile of project.getSourceFiles()) {
    const filePath = sourceFile.getFilePath();
    const content = sourceFile.getFullText();
    const relativePath = path.relative(
      path.resolve(process.cwd(), 'src/core/database/entities'),
      filePath,
    );
    compileProject.createSourceFile(relativePath, content);
  }

  // Emit compiled JavaScript
  const emitResult = compileProject.emitToMemory();

  // Write JS files to disk
  for (const outputFile of emitResult.getFiles()) {
    // Fix for Windows: outputFile.filePath might be absolute, so use basename
    const fileName = path.basename(outputFile.filePath);
    const jsFilePath = path.join(outputDir, fileName);
    const jsDir = path.dirname(jsFilePath);

    if (!fs.existsSync(jsDir)) {
      fs.mkdirSync(jsDir, { recursive: true });
    }

    fs.writeFileSync(jsFilePath, outputFile.text, 'utf8');
  }
}

export async function initializeDatabase() {
  const DB_TYPE = process.env.DB_TYPE as 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT = parseInt(process.env.DB_PORT || '3306');
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  await ensureDatabaseExists();

  const checkDS = new DataSource({
    type: DB_TYPE,
    host: DB_HOST,
    port: DB_PORT,
    username: DB_USERNAME,
    password: DB_PASSWORD,
    database: DB_NAME,
  });

  await checkDS.initialize();

  const queryRunner = checkDS.createQueryRunner();
  try {
    const [result] = await queryRunner.query(
      `SELECT isInit FROM setting_definition LIMIT 1`,
    );

    if (result?.isInit === true || result?.isInit === 1) {
      console.log('⚠️ Đã init trước đó, bỏ qua bước init.');
      await queryRunner.release();
      await checkDS.destroy();
      return;
    }
  } catch (err) {
    // Nếu bảng chưa tồn tại thì cứ tiếp tục init
    console.log(
      '🔄 Bảng setting_definition chưa tồn tại hoặc chưa có dữ liệu.',
    );
  }

  await queryRunner.release();
  await checkDS.destroy();

  // await writeEntitiesFromSnapshot();

  // await ensureDatabaseExists();

  // const dataSource = new DataSource({
  //   type: DB_TYPE,
  //   host: DB_HOST,
  //   port: DB_PORT,
  //   username: DB_USERNAME,
  //   password: DB_PASSWORD,
  //   database: DB_NAME,
  //   entities: [
  //     path.resolve(
  //       process.cwd(),
  //       'dist/src/core/database/entities/*.entity.js',
  //     ),
  //   ],
  //   synchronize: true,
  //   logging: false,
  // });

  // await dataSource.initialize();
  // console.log('✅ Database schema created from generated entities.');
  // await dataSource.destroy();
}

// For direct execution
if (require.main === module) {
  initializeDatabase().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
