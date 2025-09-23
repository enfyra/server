import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';

@Injectable()
export class MenuDefinitionProcessor extends BaseTableProcessor {
  async process(records: any[], repo: Repository<any>, context?: any): Promise<UpsertResult> {
    // Tách records theo type và xử lý theo thứ tự
    const miniSidebars = records.filter(r => r.type === 'Mini Sidebar');
    const dropdownMenus = records.filter(r => r.type === 'Dropdown Menu');
    const menuItems = records.filter(r => r.type === 'Menu');

    let totalCreated = 0;
    let totalSkipped = 0;

    // 1. Process Mini Sidebars first (không có dependencies)
    if (miniSidebars.length > 0) {
      this.logger.log(`Processing ${miniSidebars.length} Mini Sidebars...`);
      const result = await super.process(miniSidebars, repo, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
      this.logger.log(`Mini Sidebars done: ${result.created} created, ${result.skipped} skipped`);
    }

    // 2. Process Dropdown Menus (có thể có sidebar dependency)
    if (dropdownMenus.length > 0) {
      this.logger.log(`Processing ${dropdownMenus.length} Dropdown Menus...`);
      const result = await super.process(dropdownMenus, repo, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
      this.logger.log(`Dropdown Menus done: ${result.created} created, ${result.skipped} skipped`);
    }

    // 3. Process Menu items (có thể có sidebar và parent dependencies)
    if (menuItems.length > 0) {
      this.logger.log(`Processing ${menuItems.length} Menu items...`);
      const result = await super.process(menuItems, repo, context);
      totalCreated += result.created;
      totalSkipped += result.skipped;
      this.logger.log(`Menu items done: ${result.created} created, ${result.skipped} skipped`);
    }

    return { created: totalCreated, skipped: totalSkipped };
  }
  async transformRecords(records: any[], context: { repo: Repository<any> }): Promise<any[]> {
    const { repo } = context;
    const transformedRecords = [];

    for (const record of records) {
      const transformed = { ...record };

      // Handle sidebar reference - tìm Mini Sidebar theo label
      if (transformed.sidebar && typeof transformed.sidebar === 'string') {
        const sidebarLabel = transformed.sidebar;
        const sidebar = await repo.findOne({
          where: {
            type: 'Mini Sidebar',
            label: sidebarLabel
          },
          select: ['id', 'label']
        });

        if (sidebar) {
          this.logger.debug(`Found sidebar: ${sidebarLabel} with id ${sidebar.id}`);
          transformed.sidebar = { id: sidebar.id };
        } else {
          this.logger.warn(`Sidebar not found: ${sidebarLabel} for ${transformed.label}`);
          transformed.sidebar = null;
        }
      }

      // Handle parent reference - tìm Dropdown Menu theo label
      if (transformed.parent && typeof transformed.parent === 'string') {
        const parentLabel = transformed.parent;
        const parent = await repo.findOne({
          where: {
            type: 'Dropdown Menu',
            label: parentLabel
          },
          select: ['id', 'label']
        });

        if (parent) {
          this.logger.debug(`Found parent: ${parentLabel} with id ${parent.id}`);
          transformed.parent = { id: parent.id };
        } else {
          this.logger.warn(`Parent not found: ${parentLabel} for ${transformed.label}`);
          transformed.parent = null;
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