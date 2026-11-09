import type { TCreateTableBody } from '../../../src/modules/table-management/types/table-handler.types';

export interface RuntimeMigrationMatrixDriver {
  loadTableBody(tableId: string | number): Promise<TCreateTableBody>;
  applyConfirmedTableUpdate(
    tableId: string | number,
    body: TCreateTableBody,
  ): Promise<unknown>;
  deleteTableConfirmed(tableId: string | number): Promise<unknown>;
}
