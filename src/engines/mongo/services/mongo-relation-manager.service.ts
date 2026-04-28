import { Logger } from '../../../shared/logger';
import { ObjectId, Collection, Document } from 'mongodb';
import { MetadataCacheService } from '../../cache';
import {
  normalizeRelationOnDelete,
  TRelationOnDeleteAction,
} from '../utils/mongo-relation-on-delete.util';
import { resolveMongoJunctionInfo } from '../utils/mongo-junction.util';
import { ValidationException } from '../../../domain/exceptions';
import { isMetadataTable } from '../../../shared/utils/cache-events.constants';

const M2M_PENDING = Symbol('mongoService.m2mPending');

export class MongoRelationManagerService {
  private readonly logger = new Logger(MongoRelationManagerService.name);

  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: { metadataCacheService: MetadataCacheService }) {
    this.metadataCacheService = deps.metadataCacheService;
  }

  async stripInverseRelations(tableName: string, data: any): Promise<any> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata?.relations) {
      return data;
    }

    const result = { ...data };

    for (const relation of metadata.relations) {
      if (relation.type === 'many-to-many') continue;

      const isInverse = relation.type === 'one-to-many' || relation.isInverse;

      if (isInverse && relation.propertyName in result) {
        delete result[relation.propertyName];
      }
    }

    return result;
  }

  async updateInverseRelationsOnUpdate(
    tableName: string,
    recordId: ObjectId,
    oldData: any,
    newData: any,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (relation.type === 'many-to-many') {
        continue;
      }
      if (!relation.mappedBy) {
        continue;
      }

      const fieldName = relation.propertyName;

      if (!(fieldName in newData)) {
        continue;
      }

      const oldValue = oldData?.[fieldName];
      const newValue = newData?.[fieldName];

      const targetCollection = relation.targetTableName || relation.targetTable;

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        const oldId =
          oldValue instanceof ObjectId
            ? oldValue
            : oldValue
              ? typeof oldValue === 'object' && oldValue._id
                ? new ObjectId(oldValue._id)
                : new ObjectId(oldValue)
              : null;
        const newId =
          newValue instanceof ObjectId
            ? newValue
            : newValue
              ? typeof newValue === 'object' && newValue._id
                ? new ObjectId(newValue._id)
                : new ObjectId(newValue)
              : null;

        if (oldId && (!newId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await getCollection(targetCollection).updateOne({ _id: oldId }, {
              $pull: { [relation.mappedBy]: recordId },
            } as any);
          } else {
            await getCollection(targetCollection).updateOne({ _id: oldId }, {
              $unset: { [relation.mappedBy]: '' },
            } as any);
          }
        }

        if (newId && (!oldId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await getCollection(targetCollection).updateOne(
              { _id: newId },
              { $addToSet: { [relation.mappedBy]: recordId } },
            );
          } else {
            await getCollection(targetCollection).updateOne(
              { _id: newId },
              { $set: { [relation.mappedBy]: recordId } },
            );
          }
        }
      } else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const oldIds = Array.isArray(oldValue)
          ? oldValue.map((v) => {
              if (v instanceof ObjectId) return v;
              if (typeof v === 'object' && v._id) return new ObjectId(v._id);
              return new ObjectId(v);
            })
          : [];
        const newIds = Array.isArray(newValue)
          ? newValue.map((v) => {
              if (v instanceof ObjectId) return v;
              if (typeof v === 'object' && v._id) return new ObjectId(v._id);
              return new ObjectId(v);
            })
          : [];

        const removed = oldIds.filter(
          (oldId) =>
            !newIds.some((newId) => newId.toString() === oldId.toString()),
        );
        const added = newIds.filter(
          (newId) =>
            !oldIds.some((oldId) => oldId.toString() === newId.toString()),
        );

        for (const targetId of removed) {
          if (relation.type === 'one-to-many') {
            await getCollection(targetCollection).updateOne({ _id: targetId }, {
              $unset: { [relation.mappedBy]: '' },
            } as any);
          } else {
            await getCollection(targetCollection).updateOne({ _id: targetId }, {
              $pull: { [relation.mappedBy]: recordId },
            } as any);
          }
        }

        for (const targetId of added) {
          if (relation.type === 'one-to-many') {
            await getCollection(targetCollection).updateOne(
              { _id: targetId },
              { $set: { [relation.mappedBy]: recordId } },
            );
          } else {
            await getCollection(targetCollection).updateOne(
              { _id: targetId },
              { $addToSet: { [relation.mappedBy]: recordId } },
            );
          }
        }
      }
    }
  }

  async processNestedRelations(
    tableName: string,
    data: any,
    getCollection: (name: string) => Collection<Document>,
    checkPolicy: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>,
    insertOne: (collectionName: string, data: any) => Promise<any>,
    updateOne: (collectionName: string, id: string, data: any) => Promise<any>,
  ): Promise<any> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return data;
    }

    const processed = { ...data };

    for (const relation of metadata.relations) {
      const fieldName = relation.propertyName;

      if (!(fieldName in processed)) continue;

      const isInverse =
        relation.type === 'one-to-many' ||
        (relation.type === 'one-to-one' &&
          (relation.mappedBy || relation.isInverse));

      if (isInverse) {
        continue;
      }

      const fieldValue = processed[fieldName];
      const targetCollection = relation.targetTableName || relation.targetTable;

      if (fieldValue === null || fieldValue === undefined) {
        if (relation.type === 'many-to-many') {
          this.setM2mPending(processed, fieldName, []);
          delete processed[fieldName];
        } else {
          processed[fieldName] = null;
        }
        continue;
      }

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        if (
          typeof fieldValue !== 'object' ||
          Array.isArray(fieldValue) ||
          fieldValue instanceof ObjectId ||
          fieldValue instanceof Date
        ) {
          if (typeof fieldValue === 'string' && fieldValue.length === 24) {
            try {
              processed[fieldName] = new ObjectId(fieldValue);
            } catch (_) {}
          }
          continue;
        }

        const { _id: nestedId, id, ...nestedData } = fieldValue;
        const hasDataToUpdate = Object.keys(nestedData).length > 0;

        if (!nestedId && !id) {
          if (hasDataToUpdate) {
            await checkPolicy(targetCollection, 'create', nestedData);
            const inserted = await insertOne(targetCollection, nestedData);
            processed[fieldName] = new ObjectId(inserted._id);
          } else {
            processed[fieldName] = null;
          }
        } else if (hasDataToUpdate) {
          const idToUse = nestedId || id;
          await checkPolicy(targetCollection, 'update', nestedData);
          await updateOne(targetCollection, idToUse, nestedData);
          processed[fieldName] =
            typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        } else {
          const idToUse = nestedId || id;
          processed[fieldName] =
            typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        }
      } else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        if (!Array.isArray(fieldValue)) {
          if (relation.type === 'many-to-many') {
            this.setM2mPending(processed, fieldName, []);
            delete processed[fieldName];
          } else {
            processed[fieldName] = [];
          }
          continue;
        }

        const processedArray = [];
        for (const item of fieldValue) {
          if (
            typeof item !== 'object' ||
            item instanceof ObjectId ||
            item instanceof Date
          ) {
            processedArray.push(
              item instanceof ObjectId ? item : new ObjectId(item),
            );
            continue;
          }

          const { _id: itemId, id: itemIdAlt, ...itemData } = item;
          const hasDataToUpdate = Object.keys(itemData).length > 0;

          if (!itemId && !itemIdAlt) {
            if (hasDataToUpdate) {
              await checkPolicy(targetCollection, 'create', itemData);
              const inserted = await insertOne(targetCollection, itemData);
              processedArray.push(new ObjectId(inserted._id));
            }
          } else if (hasDataToUpdate) {
            const idToUse = itemId || itemIdAlt;
            await checkPolicy(targetCollection, 'update', itemData);
            await updateOne(targetCollection, idToUse, itemData);
            processedArray.push(
              typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse,
            );
          } else {
            const idToUse = itemId || itemIdAlt;
            processedArray.push(
              typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse,
            );
          }
        }
        if (relation.type === 'many-to-many') {
          this.setM2mPending(processed, fieldName, processedArray);
          delete processed[fieldName];
        } else {
          processed[fieldName] = processedArray;
        }
      }
    }

    return processed;
  }

  async cleanupInverseRelationsOnDelete(
    tableName: string,
    recordId: ObjectId,
    recordData: any,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);

    if (metadata?.relations) {
      for (const relation of metadata.relations) {
        const onDelete = normalizeRelationOnDelete(relation);
        const fieldName = relation.propertyName;
        const fieldValue = recordData?.[fieldName];
        const targetCollection =
          relation.targetTableName || relation.targetTable;

        if (relation.type === 'many-to-many') {
          await this.applyManyToManyOnDelete(
            tableName,
            relation,
            recordId,
            onDelete,
            getCollection,
          );
          continue;
        }

        if (!relation.mappedBy) {
          continue;
        }

        if (relation.type === 'many-to-one') {
          await this.unlinkManyToOneInverse(
            relation,
            recordId,
            recordData,
            targetCollection,
            getCollection,
          );
          continue;
        }

        if (relation.type === 'one-to-many') {
          await this.applyOneToManyOnDelete(
            relation,
            recordId,
            targetCollection,
            fieldValue,
            onDelete,
            getCollection,
          );
          continue;
        }

        if (relation.type === 'one-to-one') {
          await this.applyOneToOneOnDelete(
            relation,
            recordId,
            targetCollection,
            fieldValue,
            onDelete,
            getCollection,
          );
        }
      }
    }

    await this.cleanupReverseManyToManyOnDelete(
      tableName,
      recordId,
      getCollection,
    );
  }

  private async cleanupReverseManyToManyOnDelete(
    tableName: string,
    recordId: ObjectId,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const allTables = await this.metadataCacheService.getAllTablesMetadata();
    if (!allTables) return;

    for (const table of allTables) {
      if (table.name === tableName) continue;
      if (!table.relations) continue;

      for (const relation of table.relations) {
        if (relation.type !== 'many-to-many') continue;

        const targetTable = relation.targetTableName || relation.targetTable;
        if (targetTable !== tableName) continue;

        const info = this.resolveJunctionInfo(table.name, relation);
        if (!info) continue;

        await getCollection(info.junctionName).deleteMany({
          [info.otherColumn]: recordId,
        } as any);
      }
    }
  }

  private async isSystemFilterIfApplicable(
    targetCollection: string,
  ): Promise<Record<string, unknown>> {
    const meta =
      await this.metadataCacheService.lookupTableByName(targetCollection);
    const has = !!meta?.columns?.some(
      (c: { name?: string }) => c.name === 'isSystem',
    );
    return has ? { isSystem: { $ne: true } } : {};
  }

  private async unlinkManyToOneInverse(
    relation: any,
    recordId: ObjectId,
    recordData: any,
    targetCollection: string,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const fieldName = relation.propertyName;
    const mappedBy = relation.mappedBy;
    const raw = recordData?.[fieldName];
    const coll = getCollection(targetCollection);

    if (raw != null && raw !== undefined) {
      let parentId: ObjectId;
      try {
        parentId = raw instanceof ObjectId ? raw : new ObjectId(String(raw));
      } catch {
        return;
      }
      const parent = await coll.findOne({ _id: parentId });
      if (!parent) {
        return;
      }
      if (Array.isArray(parent[mappedBy])) {
        await coll.updateOne({ _id: parentId }, {
          $pull: { [mappedBy]: recordId },
        } as any);
      } else if (
        parent[mappedBy] != null &&
        parent[mappedBy].toString() === recordId.toString()
      ) {
        await coll.updateOne({ _id: parentId }, {
          $unset: { [mappedBy]: '' },
        } as any);
      }
      return;
    }

    const alt = await coll.findOne({ [mappedBy]: recordId } as any);
    if (!alt) {
      return;
    }
    if (Array.isArray(alt[mappedBy])) {
      await coll.updateOne({ _id: alt._id }, {
        $pull: { [mappedBy]: recordId },
      } as any);
    } else if (
      alt[mappedBy] != null &&
      alt[mappedBy].toString() === recordId.toString()
    ) {
      await coll.updateOne({ _id: alt._id }, {
        $unset: { [mappedBy]: '' },
      } as any);
    }
  }

  private async applyOneToManyOnDelete(
    relation: any,
    recordId: ObjectId,
    targetCollection: string,
    fieldValue: any,
    onDelete: TRelationOnDeleteAction,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const mappedBy = relation.mappedBy;
    let targetIds: ObjectId[] = [];
    if (Array.isArray(fieldValue) && fieldValue.length > 0) {
      targetIds = fieldValue.map((v) =>
        v instanceof ObjectId ? v : new ObjectId(v),
      );
    } else {
      const targets = await getCollection(targetCollection)
        .find({ [mappedBy]: recordId } as any)
        .toArray();
      targetIds = targets.map((t) => t._id);
    }

    if (targetIds.length === 0) {
      return;
    }

    if (onDelete === 'RESTRICT') {
      throw new ValidationException(
        `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
        { relation: relation.propertyName, targetCollection },
      );
    }

    const coll = getCollection(targetCollection);
    const sys = await this.isSystemFilterIfApplicable(targetCollection);

    if (onDelete === 'CASCADE') {
      if (isMetadataTable(targetCollection)) {
        this.logger.warn(
          `[applyOneToManyOnDelete] Blocked cascade to metadata table "${targetCollection}"`,
        );
        return;
      }
      await coll.deleteMany({
        _id: { $in: targetIds },
        ...sys,
      } as any);
      return;
    }

    await coll.updateMany(
      { _id: { $in: targetIds }, ...sys } as any,
      { $set: { [mappedBy]: null } } as any,
    );
  }

  private async applyManyToManyOnDelete(
    tableName: string,
    relation: any,
    recordId: ObjectId,
    onDelete: TRelationOnDeleteAction,
    getCollection: (name: string) => Collection<Document>,
    matchColumnOverride?: string,
  ): Promise<void> {
    const info = this.resolveJunctionInfo(tableName, relation);
    if (!info) return;

    const junctionColl = getCollection(info.junctionName);
    const matchColumn = matchColumnOverride ?? info.selfColumn;

    if (onDelete === 'RESTRICT') {
      const count = await junctionColl.countDocuments({
        [matchColumn]: recordId,
      } as any);
      if (count > 0) {
        const targetCollection =
          relation.targetTableName || relation.targetTable;
        throw new ValidationException(
          `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
          { relation: relation.propertyName, targetCollection },
        );
      }
    }

    await junctionColl.deleteMany({ [matchColumn]: recordId } as any);
  }

  private setM2mPending(
    carrier: any,
    propertyName: string,
    ids: ObjectId[],
  ): void {
    if (!carrier[M2M_PENDING]) {
      carrier[M2M_PENDING] = new Map<string, ObjectId[]>();
    }
    (carrier[M2M_PENDING] as Map<string, ObjectId[]>).set(propertyName, ids);
  }

  getM2mPending(carrier: any): Map<string, ObjectId[]> | null {
    return (carrier?.[M2M_PENDING] as Map<string, ObjectId[]>) || null;
  }

  private resolveJunctionInfo(currentTable: string, relation: any) {
    return resolveMongoJunctionInfo(currentTable, relation);
  }

  async writeM2mJunctionsForInsert(
    tableName: string,
    recordId: ObjectId,
    data: any,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const pending = this.getM2mPending(data);
    if (!pending || pending.size === 0) return;

    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata?.relations) return;

    for (const [propertyName, targetIds] of pending.entries()) {
      const relation = metadata.relations.find(
        (r: any) => r.propertyName === propertyName,
      );
      if (!relation) continue;
      const info = this.resolveJunctionInfo(tableName, relation);
      if (!info) continue;
      if (!targetIds.length) continue;

      const rows = targetIds.map((otherId) => ({
        [info.selfColumn]: recordId,
        [info.otherColumn]: otherId,
      }));

      try {
        await getCollection(info.junctionName).insertMany(rows as any, {
          ordered: false,
        });
      } catch (err: any) {
        if (err?.code !== 11000) throw err;
      }
    }
  }

  async writeM2mJunctionsForUpdate(
    tableName: string,
    recordId: ObjectId,
    data: any,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const pending = this.getM2mPending(data);
    if (!pending || pending.size === 0) return;

    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata?.relations) return;

    for (const [propertyName, targetIds] of pending.entries()) {
      const relation = metadata.relations.find(
        (r: any) => r.propertyName === propertyName,
      );
      if (!relation) continue;
      const info = this.resolveJunctionInfo(tableName, relation);
      if (!info) continue;

      const junctionColl = getCollection(info.junctionName);

      await junctionColl.deleteMany({ [info.selfColumn]: recordId } as any);

      if (!targetIds.length) continue;

      const rows = targetIds.map((otherId) => ({
        [info.selfColumn]: recordId,
        [info.otherColumn]: otherId,
      }));
      try {
        await junctionColl.insertMany(rows as any, { ordered: false });
      } catch (err: any) {
        if (err?.code !== 11000) throw err;
      }
    }
  }

  private async applyOneToOneOnDelete(
    relation: any,
    recordId: ObjectId,
    targetCollection: string,
    fieldValue: any,
    onDelete: TRelationOnDeleteAction,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const mappedBy = relation.mappedBy;
    const coll = getCollection(targetCollection);

    const inverseDocs = await coll
      .find({ [mappedBy]: recordId } as any)
      .toArray();

    let ownedChildId: ObjectId | null = null;
    if (fieldValue != null && fieldValue !== undefined) {
      try {
        ownedChildId =
          fieldValue instanceof ObjectId
            ? fieldValue
            : new ObjectId(String(fieldValue));
      } catch {
        ownedChildId = null;
      }
    }

    const hasInverse = inverseDocs.length > 0;
    const hasOwned = !!ownedChildId;

    if (onDelete === 'RESTRICT' && (hasInverse || hasOwned)) {
      throw new ValidationException(
        `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
        { relation: relation.propertyName, targetCollection },
      );
    }

    const sys = await this.isSystemFilterIfApplicable(targetCollection);

    if (onDelete === 'CASCADE') {
      if (isMetadataTable(targetCollection)) {
        this.logger.warn(
          `[applyOneToOneOnDelete] Blocked cascade to metadata table "${targetCollection}"`,
        );
        return;
      }
      const byId = new Map<string, ObjectId>();
      for (const d of inverseDocs) {
        byId.set(d._id.toString(), d._id);
      }
      if (ownedChildId) {
        byId.set(ownedChildId.toString(), ownedChildId);
      }
      for (const id of byId.values()) {
        await coll.deleteOne({ _id: id, ...sys } as any);
      }
      return;
    }

    for (const d of inverseDocs) {
      await coll.updateOne({ _id: d._id }, {
        $unset: { [mappedBy]: '' },
      } as any);
    }
    if (ownedChildId) {
      await coll.updateOne({ _id: ownedChildId }, {
        $unset: { [mappedBy]: '' },
      } as any);
    }
  }

  async clearUniqueFKHolders(
    collectionName: string,
    recordId: ObjectId,
    data: any,
    getCollection: (name: string) => Collection<Document>,
  ): Promise<void> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(collectionName);
    if (!metadata?.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (!['one-to-one', 'many-to-one'].includes(relation.type)) continue;
      if (relation.isInverse || relation.mappedBy) continue;

      const fieldName = relation.propertyName;
      const hasUnique = this.hasUniqueConstraintOnField(metadata, fieldName);

      if (!hasUnique) continue;

      const newValue = data[fieldName];
      if (newValue == null) continue;

      const newId =
        newValue instanceof ObjectId ? newValue : new ObjectId(newValue);

      await getCollection(collectionName).updateMany(
        {
          [fieldName]: newId,
          _id: { $ne: recordId },
        },
        { $set: { [fieldName]: null } },
      );
    }
  }

  private hasUniqueConstraintOnField(
    metadata: any,
    fieldName: string,
  ): boolean {
    if (!metadata?.uniques) return false;

    const uniques = Array.isArray(metadata.uniques)
      ? metadata.uniques
      : Object.values(metadata.uniques || {});

    for (const unique of uniques) {
      const fields = Array.isArray(unique) ? unique : [unique];
      if (fields.length === 1 && fields[0] === fieldName) {
        return true;
      }
    }

    return false;
  }
}
