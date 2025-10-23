import { Injectable } from '@nestjs/common';
import { Knex } from 'knex';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class MenuDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  async processSql(
    records: any[],
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const miniSidebars = records.filter(r => r.type === 'Mini Sidebar');
    const dropdownMenus = records.filter(r => r.type === 'Dropdown Menu');
    const menuItems = records.filter(r => r.type === 'Menu');

    let totalCreated = 0;
    let totalSkipped = 0;

    if (miniSidebars.length > 0) {
      this.logger.log(`Processing ${miniSidebars.length} Mini Sidebars...`);
      const result = await super.processSql(miniSidebars, knex, tableName, { ...context, knex });
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    if (dropdownMenus.length > 0) {
      this.logger.log(`Processing ${dropdownMenus.length} Dropdown Menus...`);
      const result = await super.processSql(dropdownMenus, knex, tableName, { ...context, knex });
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    if (menuItems.length > 0) {
      this.logger.log(`Processing ${menuItems.length} Menu items...`);
      const result = await super.processSql(menuItems, knex, tableName, { ...context, knex });
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    return { created: totalCreated, skipped: totalSkipped };
  }

  /**
   * Process menu items in order for MongoDB (Mini Sidebar → Dropdown → Menu items with parents)
   */
  async processMongo(
    records: any[],
    db: any,
    collectionName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    // Group menu items by type
    const miniSidebars = records.filter(r => r.type === 'Mini Sidebar');
    const dropdownMenus = records.filter(r => r.type === 'Dropdown Menu');
    const menuItemsWithSidebar = records.filter(r => r.type === 'Menu' && r.sidebar && !r.parent);
    const menuItemsWithParent = records.filter(r => r.type === 'Menu' && r.parent);
    const otherMenuItems = records.filter(r => r.type === 'Menu' && !r.sidebar && !r.parent);

    let totalCreated = 0;
    let totalSkipped = 0;

    // Process in order: Mini Sidebars first
    if (miniSidebars.length > 0) {
      this.logger.log(`Processing ${miniSidebars.length} Mini Sidebars...`);
      const result = await super.processMongo(miniSidebars, db, collectionName, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    // Then Dropdown Menus (can reference sidebars)
    if (dropdownMenus.length > 0) {
      this.logger.log(`Processing ${dropdownMenus.length} Dropdown Menus...`);
      const result = await super.processMongo(dropdownMenus, db, collectionName, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    // Then Menu items with sidebars (no parents yet)
    if (menuItemsWithSidebar.length > 0) {
      this.logger.log(`Processing ${menuItemsWithSidebar.length} Menu items with sidebars...`);
      const result = await super.processMongo(menuItemsWithSidebar, db, collectionName, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    // Then other menu items without references
    if (otherMenuItems.length > 0) {
      this.logger.log(`Processing ${otherMenuItems.length} other Menu items...`);
      const result = await super.processMongo(otherMenuItems, db, collectionName, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    // Finally Menu items with parent references (must be last)
    if (menuItemsWithParent.length > 0) {
      this.logger.log(`Processing ${menuItemsWithParent.length} Menu items with parents...`);
      const result = await super.processMongo(menuItemsWithParent, db, collectionName, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
    }

    return { created: totalCreated, skipped: totalSkipped };
  }
  
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const knex = context?.knex;
    
    if (!isMongoDB && !knex) {
      this.logger.warn('Knex not provided in context for SQL, returning records as-is');
      return records;
    }

    const transformedRecords = [];

    for (const record of records) {
      const transformed = { ...record };

      // Add default values
      if (transformed.icon === undefined) transformed.icon = 'lucide:menu';
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.isSystem === undefined) transformed.isSystem = false;
      if (transformed.order === undefined) transformed.order = 0;

      // Add timestamps for MongoDB
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      // Set default null for MongoDB fields
      if (isMongoDB) {
        if (!('sidebar' in transformed)) transformed.sidebar = null;
        if (!('parent' in transformed)) transformed.parent = null;
      }

      // Handle sidebar reference
      if (transformed.sidebar && typeof transformed.sidebar === 'string') {
        const sidebarLabel = transformed.sidebar;
        
        if (isMongoDB) {
          // MongoDB: Convert to sidebar ObjectId
          const sidebar = await this.queryBuilder.findOneWhere('menu_definition', {
            type: 'Mini Sidebar',
            label: sidebarLabel,
          });

          if (sidebar) {
            this.logger.debug(`Found sidebar: ${sidebarLabel} with id ${sidebar._id}`);
            transformed.sidebar = typeof sidebar._id === 'string' 
              ? new ObjectId(sidebar._id) 
              : sidebar._id;
          } else {
            this.logger.warn(`Sidebar not found: ${sidebarLabel} for ${transformed.label}`);
            transformed.sidebar = null;
          }
        } else {
          // SQL: Convert to sidebarId
          const sidebar = await knex('menu_definition')
            .where({ type: 'Mini Sidebar', label: sidebarLabel })
            .first();

          if (sidebar) {
            this.logger.debug(`Found sidebar: ${sidebarLabel} with id ${sidebar.id}`);
            transformed.sidebarId = sidebar.id;
            delete transformed.sidebar;
          } else {
            this.logger.warn(`Sidebar not found: ${sidebarLabel} for ${transformed.label}`);
            delete transformed.sidebar;
          }
        }
      }

      // Handle parent reference
      if (transformed.parent && typeof transformed.parent === 'string') {
        const parentLabel = transformed.parent;
        
        if (isMongoDB) {
          // MongoDB: Convert to parent ObjectId
          const parent = await this.queryBuilder.findOneWhere('menu_definition', {
            type: 'Dropdown Menu',
            label: parentLabel,
          });

          if (parent) {
            this.logger.debug(`Found parent: ${parentLabel} with id ${parent._id}`);
            transformed.parent = typeof parent._id === 'string' 
              ? new ObjectId(parent._id) 
              : parent._id;
          } else {
            this.logger.warn(`Parent not found: ${parentLabel} for ${transformed.label}`);
            transformed.parent = null;
          }
        } else {
          // SQL: Convert to parentId
          const parent = await knex('menu_definition')
            .where({ type: 'Dropdown Menu', label: parentLabel })
            .first();

          if (parent) {
            this.logger.debug(`Found parent: ${parentLabel} with id ${parent.id}`);
            transformed.parentId = parent.id;
            delete transformed.parent;
          } else {
            this.logger.warn(`Parent not found: ${parentLabel} for ${transformed.label}`);
            delete transformed.parent;
          }
        }
      }

      transformedRecords.push(transformed);
    }

    return transformedRecords;
  }

  getUniqueIdentifier(record: any): object[] {
    if (record.type === 'Mini Sidebar' || record.type === 'mini') {
      // For mini sidebars, check by type + label
      return [{ type: record.type, label: record.label }];
    } else if (record.type === 'Menu' || record.type === 'menu' || record.type === 'Dropdown Menu') {
      // For menu items and dropdown menus, try multiple strategies
      const conditions = [];
      
      // If has sidebar, try with sidebar first
      if (record.sidebar) {
        conditions.push({ type: record.type, label: record.label, sidebar: record.sidebar });
      }
      
      // Always add fallback without sidebar
      conditions.push({ type: record.type, label: record.label });
      
      return conditions;
    }
    
    // Fallback for other types
    return [{ type: record.type, label: record.label }];
  }

  protected getCompareFields(): string[] {
    return ['label', 'icon', 'path', 'isEnabled', 'description', 'order', 'permission', 'sidebar', 'parent'];
  }

  protected hasValueChanged(newValue: any, existingValue: any): boolean {
    // Special handling for sidebar and parent relations
    if (typeof newValue === 'object' && newValue?.id && typeof existingValue === 'object' && existingValue?.id) {
      // Compare only IDs for relation objects
      return newValue.id !== existingValue.id;
    }

    // Handle case where one is null/undefined and other is object
    if ((newValue === null || newValue === undefined) && (existingValue && typeof existingValue === 'object' && existingValue.id)) {
      return true; // Changed from object to null
    }
    if ((existingValue === null || existingValue === undefined) && (newValue && typeof newValue === 'object' && newValue.id)) {
      return true; // Changed from null to object
    }

    // Use parent class logic for other cases
    return super.hasValueChanged(newValue, existingValue);
  }

  protected getRecordIdentifier(record: any): string {
    const type = record.type;
    const label = record.label;
    const sidebar = record.sidebar;
    
    if (type === 'Mini Sidebar' || type === 'mini') {
      return `[Mini Sidebar] ${label}`;
    } else if (type === 'Dropdown Menu') {
      return `[Dropdown Menu] ${label}${sidebar ? ` (sidebar: ${sidebar})` : ''}`;
    } else if (type === 'Menu' || type === 'menu') {
      return `[Menu] ${label}${sidebar ? ` (sidebar: ${sidebar})` : ''} -> ${record.path || 'no-path'}`;
    }
    
    return `[${type}] ${label}`;
  }
}