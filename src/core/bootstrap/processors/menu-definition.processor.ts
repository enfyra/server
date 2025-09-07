import { Injectable } from '@nestjs/common';
import { Repository } from 'typeorm';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class MenuDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[], context: { repo: Repository<any> }): Promise<any[]> {
    const { repo } = context;
    
    // Process records in order: sidebars first, then menu items
    const transformedRecords = [];
    
    // First, process all sidebars
    for (const record of records) {
      if (record.type === 'Mini Sidebar') {
        transformedRecords.push(record);
      }
    }
    
    // Then process menu items and dropdowns with sidebar references
    for (const record of records) {
      if (record.type === 'Menu' || record.type === 'Dropdown Menu') {
        const transformed = { ...record };
        
        // Convert sidebar name to ID if needed
        if (transformed.sidebar && typeof transformed.sidebar === 'string') {
          // Look for sidebar in both existing DB and records being processed
          let sidebar = await repo.findOne({
            where: { type: 'Mini Sidebar', label: transformed.sidebar }
          });
          
          // If not found in DB, look in current batch of records
          if (!sidebar) {
            const sidebarRecord = records.find(r => 
              (r.type === 'Mini Sidebar') && r.label === transformed.sidebar
            );
            if (sidebarRecord) {
              // Create the sidebar first if it doesn't exist
              const created = repo.create(sidebarRecord);
              sidebar = await repo.save(created);
              this.logger.debug(`Created sidebar "${transformed.sidebar}" with id ${sidebar.id}`);
            }
          }
          
          if (sidebar) {
            transformed.sidebar = sidebar.id;
          } else {
            // Remove invalid sidebar reference
            delete transformed.sidebar;
            this.logger.warn(`Sidebar "${record.sidebar}" not found for menu item "${record.label}"`);
          }
        }
        
        transformedRecords.push(transformed);
      }
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