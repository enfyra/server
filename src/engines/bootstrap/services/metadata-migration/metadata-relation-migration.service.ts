import { QueryBuilderService } from '@enfyra/kernel';
import type { Db } from 'mongodb';
import type { RelationModifyDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import {
  buildRelationMetadataUpdate,
  hasRelationMetadataChanges,
} from '../../utils/metadata-migration.util';
import { MetadataPhysicalMigrationHelper } from '../../utils/metadata-physical-migration.util';
import { getMissingMetadataRowValues } from '../../utils/metadata-row-merge.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MetadataTableLocator } from './metadata-table-locator.service';

export class MetadataRelationMigrationService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly physicalMigration: MetadataPhysicalMigrationHelper;
  private readonly tableLocator: MetadataTableLocator;
  private readonly verbose: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    physicalMigration: MetadataPhysicalMigrationHelper;
    verbose: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.physicalMigration = deps.physicalMigration;
    this.tableLocator = new MetadataTableLocator(deps);
    this.verbose = deps.verbose;
  }

  private getMongoDb(): Db {
    return this.queryBuilderService.getMongoDb();
  }

  async modifyRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    modifications: RelationModifyDef[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const mod of modifications) {
      if (!hasRelationMetadataChanges(mod)) {
        continue;
      }

      const oldName = mod.from.propertyName;
      const newName = mod.to.propertyName;
      const coreNames = await this.systemCoreTableResolver.getNames();
      let relation: any;
      let targetRelation: any;

      if (isMongoDB) {
        const db = this.getMongoDb()!;
        relation = await db.collection(coreNames.relation).findOne({
          sourceTable: tableId,
          propertyName: oldName,
        });
        targetRelation =
          oldName === newName
            ? relation
            : await db.collection(coreNames.relation).findOne({
                sourceTable: tableId,
                propertyName: newName,
              });
      } else {
        const knex = this.queryBuilderService.getKnex();
        relation = await knex(coreNames.relation)
          .where(sourceTableField, tableId)
          .where('propertyName', oldName)
          .first();
        targetRelation =
          oldName === newName
            ? relation
            : await knex(coreNames.relation)
                .where(sourceTableField, tableId)
                .where('propertyName', newName)
                .first();
      }

      if (
        relation &&
        targetRelation &&
        String(isMongoDB ? relation._id : relation.id) !==
          String(isMongoDB ? targetRelation._id : targetRelation.id)
      ) {
        relation = await this.reconcileRelationMetadataOverlap(
          relation,
          targetRelation,
          isMongoDB,
        );
      } else {
        relation = relation ?? targetRelation;
      }
      if (!relation) continue;

      const relationId = isMongoDB ? relation._id : relation.id;
      const updateData = buildRelationMetadataUpdate(mod);
      if (
        mod.to.targetTable !== undefined &&
        mod.to.targetTable !== mod.from.targetTable
      ) {
        const targetTable = await this.tableLocator.findTable(
          mod.to.targetTable,
          isMongoDB,
        );
        if (!targetTable) {
          throw new Error(
            `Target table ${mod.to.targetTable} was not found for relation ${oldName}`,
          );
        }
        updateData[isMongoDB ? 'targetTable' : 'targetTableId'] =
          targetTable.tableId;
      }
      if (
        mod.to.mappedBy !== undefined &&
        mod.to.mappedBy !== mod.from.mappedBy
      ) {
        if (mod.to.mappedBy && isMongoDB) {
          const db = this.getMongoDb()!;
          const targetTableId = relation.targetTable;
          const owningRel = await db.collection(coreNames.relation).findOne({
            sourceTable: targetTableId,
            propertyName: mod.to.mappedBy,
          });
          updateData.mappedBy = owningRel?._id || null;
        } else if (mod.to.mappedBy && !isMongoDB) {
          const knex = this.queryBuilderService.getKnex();
          const targetTableId = relation.targetTableId;
          const owningRel = await knex(coreNames.relation)
            .where('sourceTableId', targetTableId)
            .where('propertyName', mod.to.mappedBy)
            .first();
          updateData.mappedById = owningRel?.id || null;
        } else {
          const mappedByField = isMongoDB ? 'mappedBy' : 'mappedById';
          updateData[mappedByField] = null;
        }
      }
      if (Object.keys(updateData).length > 0) {
        if (isMongoDB) {
          const db = this.getMongoDb()!;
          updateData.updatedAt = new Date();
          await db
            .collection(coreNames.relation)
            .updateOne({ _id: relationId }, { $set: updateData });
        } else {
          const knex = this.queryBuilderService.getKnex();
          await knex(coreNames.relation)
            .where('id', relationId)
            .update(updateData);
        }
        this.verbose(
          `  Modified relation metadata: ${oldName} → ${mod.to.propertyName}`,
        );
      }

      await this.updateInverseRelationMetadata(
        relation,
        relationId,
        mod,
        isMongoDB,
      );
    }
  }

  private async reconcileRelationMetadataOverlap(
    sourceRelation: any,
    targetRelation: any,
    isMongoDB: boolean,
  ): Promise<any> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    const fields = [
      ...new Set([
        ...Object.keys(sourceRelation),
        ...Object.keys(targetRelation),
      ]),
    ];
    const update = getMissingMetadataRowValues(
      sourceRelation,
      targetRelation,
      fields,
    );
    for (const field of [
      'propertyName',
      'sourceTable',
      'sourceTableId',
      'targetTable',
      'targetTableId',
      'mappedBy',
      'mappedById',
    ]) {
      delete update[field];
    }

    if (isMongoDB) {
      const db = this.getMongoDb()!;
      const sourceId = sourceRelation._id;
      const targetId = targetRelation._id;
      if (Object.keys(update).length > 0) {
        await db
          .collection(coreNames.relation)
          .updateOne(
            { _id: targetId },
            { $set: { ...update, updatedAt: new Date() } },
          );
        Object.assign(targetRelation, update);
      }
      if (
        await this.physicalMigration.mongoCollectionExists(
          SYSTEM_TABLES.fieldPermission,
        )
      ) {
        await db
          .collection(SYSTEM_TABLES.fieldPermission)
          .updateMany({ relation: sourceId }, { $set: { relation: targetId } });
      }
      const mappedDependents = await db
        .collection(coreNames.relation)
        .find({ mappedBy: sourceId })
        .toArray();
      for (const dependent of mappedDependents) {
        const existing = await db.collection(coreNames.relation).findOne({
          mappedBy: targetId,
          sourceTable: dependent.sourceTable,
          propertyName: dependent.propertyName,
        });
        if (existing) {
          if (
            await this.physicalMigration.mongoCollectionExists(
              SYSTEM_TABLES.fieldPermission,
            )
          ) {
            await db
              .collection(SYSTEM_TABLES.fieldPermission)
              .updateMany(
                { relation: dependent._id },
                { $set: { relation: existing._id } },
              );
          }
          await db
            .collection(coreNames.relation)
            .deleteOne({ _id: dependent._id });
        } else {
          await db
            .collection(coreNames.relation)
            .updateOne(
              { _id: dependent._id },
              { $set: { mappedBy: targetId, updatedAt: new Date() } },
            );
        }
      }
      await db.collection(coreNames.relation).deleteOne({ _id: sourceId });
      return targetRelation;
    }

    const knex = this.queryBuilderService.getKnex();
    const sourceId = sourceRelation.id;
    const targetId = targetRelation.id;
    if (Object.keys(update).length > 0) {
      await knex(coreNames.relation).where({ id: targetId }).update(update);
      Object.assign(targetRelation, update);
    }
    const hasFieldPermission = await knex.schema.hasTable(
      SYSTEM_TABLES.fieldPermission,
    );
    if (hasFieldPermission) {
      await knex(SYSTEM_TABLES.fieldPermission)
        .where({ relationId: sourceId })
        .update({ relationId: targetId });
    }
    const mappedDependents = await knex(coreNames.relation)
      .where({ mappedById: sourceId })
      .select('*');
    for (const dependent of mappedDependents) {
      const existing = await knex(coreNames.relation)
        .where({ mappedById: targetId })
        .where({ sourceTableId: dependent.sourceTableId })
        .where({ propertyName: dependent.propertyName })
        .first();
      if (existing) {
        if (hasFieldPermission) {
          await knex(SYSTEM_TABLES.fieldPermission)
            .where({ relationId: dependent.id })
            .update({ relationId: existing.id });
        }
        await knex(coreNames.relation).where({ id: dependent.id }).delete();
      } else {
        await knex(coreNames.relation)
          .where({ id: dependent.id })
          .update({ mappedById: targetId });
      }
    }
    await knex(coreNames.relation).where({ id: sourceId }).delete();
    return targetRelation;
  }

  private async updateInverseRelationMetadata(
    relation: any,
    relationId: any,
    mod: RelationModifyDef,
    isMongoDB: boolean,
  ): Promise<void> {
    if (
      mod.to.inversePropertyName === undefined ||
      mod.to.inversePropertyName === mod.from.inversePropertyName
    ) {
      return;
    }

    const coreNames = await this.systemCoreTableResolver.getNames();
    if (isMongoDB) {
      const db = this.getMongoDb()!;
      let counterpart: any = relation.mappedBy
        ? await db
            .collection(coreNames.relation)
            .findOne({ _id: relation.mappedBy })
        : await db
            .collection(coreNames.relation)
            .findOne({ mappedBy: relationId });
      if (!counterpart) return;

      if (mod.to.inversePropertyName) {
        const targetCounterpart = await db
          .collection(coreNames.relation)
          .findOne({
            sourceTable: counterpart.sourceTable,
            propertyName: mod.to.inversePropertyName,
          });
        if (
          targetCounterpart &&
          String(targetCounterpart._id) !== String(counterpart._id)
        ) {
          counterpart = await this.reconcileRelationMetadataOverlap(
            counterpart,
            targetCounterpart,
            true,
          );
        }
        await db.collection(coreNames.relation).updateOne(
          { _id: counterpart._id },
          {
            $set: {
              propertyName: mod.to.inversePropertyName,
              updatedAt: new Date(),
            },
          },
        );
        return;
      }

      if (
        await this.physicalMigration.mongoCollectionExists(
          SYSTEM_TABLES.fieldPermission,
        )
      ) {
        await db
          .collection(SYSTEM_TABLES.fieldPermission)
          .deleteMany({ relation: counterpart._id });
      }
      await db
        .collection(coreNames.relation)
        .deleteOne({ _id: counterpart._id });
      return;
    }

    const knex = this.queryBuilderService.getKnex();
    let counterpart = relation.mappedById
      ? await knex(coreNames.relation).where('id', relation.mappedById).first()
      : await knex(coreNames.relation).where('mappedById', relationId).first();
    if (!counterpart) return;

    if (mod.to.inversePropertyName) {
      const targetCounterpart = await knex(coreNames.relation)
        .where('sourceTableId', counterpart.sourceTableId)
        .where('propertyName', mod.to.inversePropertyName)
        .first();
      if (
        targetCounterpart &&
        String(targetCounterpart.id) !== String(counterpart.id)
      ) {
        counterpart = await this.reconcileRelationMetadataOverlap(
          counterpart,
          targetCounterpart,
          false,
        );
      }
      await knex(coreNames.relation)
        .where('id', counterpart.id)
        .update({ propertyName: mod.to.inversePropertyName });
      return;
    }

    if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
      await knex(SYSTEM_TABLES.fieldPermission)
        .where('relationId', counterpart.id)
        .delete();
    }
    await knex(coreNames.relation).where('id', counterpart.id).delete();
  }

  async removeRelationMetadata(
    tableId: any,
    isMongoDB: boolean,
    relations: string[],
  ): Promise<void> {
    const sourceTableField = isMongoDB ? 'sourceTable' : 'sourceTableId';

    for (const relName of relations) {
      const coreNames = await this.systemCoreTableResolver.getNames();
      if (isMongoDB) {
        const db = this.getMongoDb()!;
        const relation = await db.collection(coreNames.relation).findOne({
          sourceTable: tableId,
          propertyName: relName,
        });
        if (!relation) continue;

        const owningId = relation.mappedBy || relation._id;
        const relationRows = (
          await db.collection(coreNames.relation).find({}).toArray()
        ).filter(
          (row: any) =>
            String(row._id) === String(owningId) ||
            String(row.mappedBy) === String(owningId),
        );
        const relationIds = relationRows.map((row: any) => row._id);
        if (
          !relationIds.some((id: any) => String(id) === String(relation._id))
        ) {
          relationIds.push(relation._id);
        }

        if (
          await this.physicalMigration.mongoCollectionExists(
            SYSTEM_TABLES.fieldPermission,
          )
        ) {
          await db
            .collection(SYSTEM_TABLES.fieldPermission)
            .deleteMany({ relation: { $in: relationIds } });
        }
        await db
          .collection(coreNames.relation)
          .deleteMany({ _id: { $in: relationIds } });
        this.verbose(`  Removed relation metadata pair: ${relName}`);
        continue;
      }

      const knex = this.queryBuilderService.getKnex();
      const relation = await knex(coreNames.relation)
        .where(sourceTableField, tableId)
        .where('propertyName', relName)
        .first();
      if (!relation) continue;

      const owningId = relation.mappedById || relation.id;
      const relationRows = (
        await knex(coreNames.relation).select('id', 'mappedById')
      ).filter(
        (row: any) =>
          String(row.id) === String(owningId) ||
          String(row.mappedById) === String(owningId),
      );
      const relationIds = relationRows.map((row: any) => row.id);
      if (!relationIds.includes(relation.id)) relationIds.push(relation.id);

      if (await knex.schema.hasTable(SYSTEM_TABLES.fieldPermission)) {
        await knex(SYSTEM_TABLES.fieldPermission)
          .whereIn('relationId', relationIds)
          .delete();
      }
      await knex(coreNames.relation).whereIn('id', relationIds).delete();
      this.verbose(`  Removed relation metadata pair: ${relName}`);
    }
  }
}
