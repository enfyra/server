import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';

@Injectable()
export class MenuDefinitionProcessor extends BaseTableProcessor {
  async process(records: any[], repo: Repository<any>, context?: any): Promise<UpsertResult> {
    // Sort records để process theo thứ tự: Mini Sidebar -> Dropdown Menu -> Menu
    const sortedRecords = [...records].sort((a, b) => {
      const order = { 'Mini Sidebar': 1, 'Dropdown Menu': 2, 'Menu': 3 };
      return (order[a.type] || 4) - (order[b.type] || 4);
    });

    return super.process(sortedRecords, repo, context);
  }
  async transformRecords(records: any[], context: { repo: Repository<any> }): Promise<any[]> {
    const { repo } = context;

    // Create sidebar cache để tối ưu lookup
    const sidebarCache = new Map<string, any>();
    const parentCache = new Map<string, any>();

    // Tìm tất cả mini sidebars để làm sidebar cache
    const miniSidebars = await repo.find({
      where: { type: 'Mini Sidebar' },
      select: ['id', 'label']
    });

    for (const sidebar of miniSidebars) {
      sidebarCache.set(sidebar.label, { id: sidebar.id });
    }

    // Tìm tất cả dropdown menus từ database để làm parent cache (rebuild mỗi lần)
    const dropdownMenus = await repo.find({
      where: { type: 'Dropdown Menu' },
      select: ['id', 'label']
    });

    for (const parent of dropdownMenus) {
      parentCache.set(parent.label, { id: parent.id });
    }

    const transformedRecords = [];

    // Process all records: Mini Sidebar, Dropdown Menu, và Menu
    for (const record of records) {
      const transformed = { ...record };

      // Handle sidebar reference - applies to ALL types (Dropdown Menu có thể có sidebar)
      if (transformed.sidebar && typeof transformed.sidebar === 'string') {
        const sidebarRef = sidebarCache.get(transformed.sidebar);
        if (sidebarRef) {
          transformed.sidebar = sidebarRef;
        } else {
          delete transformed.sidebar;
        }
      }

      // Handle parent reference - chỉ apply cho Menu items
      if (transformed.parent && typeof transformed.parent === 'string') {
        const parentRef = parentCache.get(transformed.parent);
        if (parentRef) {
          transformed.parent = parentRef;
        } else {
          delete transformed.parent;
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
    return ['label', 'icon', 'path', 'isEnabled', 'description', 'order', 'permission'];
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