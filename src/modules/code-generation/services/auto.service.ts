import { forwardRef, Inject, Injectable, Logger } from '@nestjs/common';
import * as path from 'path';
import * as fs from 'fs';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { CreateTableDto } from '../../table-management/dto/create-table.dto';
import { CommonService } from '../../../shared/common/services/common.service';
import {
  TInverseRelation,
  TInverseRelationMap,
} from '../../../shared/utils/types/common.type';
import { Project, QuoteKind } from 'ts-morph';
import { addColumnToClass } from '../builders/column-writer';
import { addRelationToClass } from '../builders/relation-writer';
import { wrapEntityClass } from '../builders/entity-wrapper';
import { writeEntityFile } from '../writers/entity-writer';
import { importMap } from '../utils/import-map';

@Injectable()
export class AutoService {
  private readonly logger = new Logger(AutoService.name);

  constructor(
    private commonService: CommonService,
    @Inject(forwardRef(() => DataSourceService))
    private dataSourceService: DataSourceService,
  ) {}

  async entityGenerate(
    payload: CreateTableDto,
    inverseRelationMap?: TInverseRelationMap,
  ) {
    const capitalize = this.commonService.capitalize.bind(this.commonService);
    const dbTypeToTSType = this.commonService.dbTypeToTSType.bind(
      this.commonService,
    );

    const className = capitalize(payload.name);
    const entityDir = path.resolve('src', 'core', 'database', 'entities');
    const entityPath = path.resolve(
      entityDir,
      `${payload.name.toLowerCase()}.entity.ts`,
    );
    if (!fs.existsSync(entityDir)) fs.mkdirSync(entityDir, { recursive: true });

    const project = new Project({
      manipulationSettings: {
        quoteKind: QuoteKind.Single,
      },
    });

    const usedImports = new Set<string>();
    const usedEntityImports = new Set<string>();

    const sourceFile = project.createSourceFile(entityPath, '', {
      overwrite: true,
    });

    // Extract all valid field names from entity definition (columns + relations)
    const columnFields = payload.columns.map((col) => col.name);
    const relationFields = (payload.relations || []).map(
      (rel) => rel.propertyName,
    );
    const validEntityFields = [...columnFields, ...relationFields];

    // Create set of actual fields that will be in the entity (columns + relations + system fields)
    const actualEntityFields = new Set([
      ...columnFields,
      ...relationFields,
      'id',
      'createdAt',
      'updatedAt', // System fields
    ]);

    // Transform uniques/indexes from simple-json array format to expected object format
    const transformedUniques = (payload.uniques || []).map((uniqueArray) => ({
      value: uniqueArray as unknown as string[],
    }));
    const transformedIndexes = (payload.indexes || []).map((indexArray) => ({
      value: indexArray as unknown as string[],
    }));

    const classDeclaration = wrapEntityClass({
      sourceFile,
      className,
      tableName: payload.name.toLowerCase(),
      uniques: transformedUniques,
      indexes: transformedIndexes,
      usedImports,
      validEntityFields,
      actualEntityFields,
      relations: payload.relations || [], // Truyền relations
    });

    for (const col of payload.columns) {
      addColumnToClass({
        classDeclaration,
        col,
        usedImports,
        helpers: { capitalize, dbTypeToTSType },
      });
    }

    // Add forward relations
    for (const rel of payload.relations || []) {
      addRelationToClass({
        classDeclaration,
        rel,
        usedImports,
        usedEntityImports,
        helpers: { capitalize },
      });
    }

    // Add inverse relations if available
    if (inverseRelationMap?.has(payload.name)) {
      for (const rel of inverseRelationMap.get(payload.name)!) {
        addRelationToClass({
          classDeclaration,
          rel,
          isInverse: true,
          usedImports,
          usedEntityImports,
          helpers: { capitalize },
        });
      }
      inverseRelationMap.delete(payload.name);
    }

    classDeclaration.addProperty({
      name: 'createdAt',
      type: 'Date',
      decorators: [{ name: 'CreateDateColumn', arguments: [] }],
    });
    usedImports.add('CreateDateColumn');

    classDeclaration.addProperty({
      name: 'updatedAt',
      type: 'Date',
      decorators: [{ name: 'UpdateDateColumn', arguments: [] }],
    });
    usedImports.add('UpdateDateColumn');

    // Add imports
    const groupedImports: Record<string, string[]> = {};
    for (const name of usedImports) {
      const path = importMap[name];
      if (!path) continue;
      if (!groupedImports[path]) groupedImports[path] = [];
      groupedImports[path].push(name);
    }

    for (const [moduleSpecifier, namedImports] of Object.entries(
      groupedImports,
    )) {
      sourceFile.addImportDeclaration({ namedImports, moduleSpecifier });
    }

    for (const entityName of usedEntityImports) {
      sourceFile.addImportDeclaration({
        namedImports: [entityName],
        moduleSpecifier: `./${entityName.toLowerCase()}.entity`,
      });
    }

    await writeEntityFile(sourceFile, entityPath);
  }

  async clearMigrationsTable() {
    const dataSource = this.dataSourceService.getDataSource();
    const queryRunner = dataSource.createQueryRunner();

    await queryRunner.connect();

    const dbType = dataSource.options.type;

    let checkTableSql: string;

    if (dbType === 'mysql') {
      checkTableSql = `
      SELECT COUNT(*) as count
      FROM information_schema.tables
      WHERE table_schema = DATABASE() AND table_name = 'migrations'
    `;
    } else if (dbType === 'postgres') {
      checkTableSql = `
      SELECT COUNT(*) as count
      FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = 'migrations'
    `;
    } else {
      await queryRunner.release();
      throw new Error(`Unsupported database type: ${dbType}`);
    }

    const result = await queryRunner.query(checkTableSql);
    const exists = Number(result[0]?.count) > 0;

    if (exists) {
      await queryRunner.query('DELETE FROM migrations;');
      this.logger.log('✅ Successfully cleared data in migrations table.');
    } else {
      this.logger.warn(
        '⚠️ Migrations table does not exist, skipping deletion.',
      );
    }

    await queryRunner.release();
  }

  buildInverseRelationMap(allTables: any[]): TInverseRelationMap {
    const capitalize = this.commonService.capitalize;
    const inverseRelationType = this.commonService.inverseRelationType;
    const map: TInverseRelationMap = new Map();

    for (const table of allTables) {
      const tableName = table.name;
      const relations = table.relations || [];

      for (const rel of relations) {
        if (!rel.inversePropertyName || !rel.targetTable?.name) continue;

        const targetName = rel.targetTable.name;
        const inverseEntry: TInverseRelation = {
          targetClass: `${capitalize(tableName)}`,
          targetGraphQLType: `${capitalize(tableName)}Type`,
          propertyName: rel.inversePropertyName,
          inversePropertyName: rel.propertyName,
          type: inverseRelationType(rel.type),
          isIndex: rel.isIndex,
        };

        if (!map.has(targetName)) map.set(targetName, []);
        map.get(targetName)!.push(inverseEntry);
      }
    }

    return map;
  }
}
