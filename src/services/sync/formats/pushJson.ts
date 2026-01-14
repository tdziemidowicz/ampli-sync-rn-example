import type { QueryLike } from '../../db/database';
import type { PushJsonRequest } from './pushTypes';

type DbAll = (query: QueryLike) => Promise<any[]>;

const toValue = (value: unknown): unknown => {
  if (value === null || typeof value === 'undefined') return 'null';
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return value;
  }
  return String(value);
};

const opColumnNames = (sqlText: string): string[] => {
  const columnParts = sqlText.replace(/^[^(]+\(([^)]+)\)/, '$1').split(',');
  return columnParts
    .map(col =>
      col
        .trim()
        .split(' ')[0]
        .replace('[', '')
        .replace(']', '')
        .replace(/"/g, ''),
    )
    .filter(Boolean);
};

const toRow = (source: any, columns: string[]): Record<string, unknown> => {
  const row: Record<string, unknown> = {};
  for (const columnName of columns) {
    if (columnName === 'mergeupdate') continue;
    row[columnName] = toValue(source?.[columnName]);
  }
  return row;
};

export const buildPushJsonRequestFromDb = async (
  dbAll: DbAll,
): Promise<{
  request: PushJsonRequest;
  recordsUpdated: QueryLike[];
  recordsDeleted: QueryLike[];
}> => {
  const recordsUpdated: QueryLike[] = [];
  const recordsDeleted: QueryLike[] = [];

  const tablesArray = await dbAll(
    "select tbl_name, sql from sqlite_master where type='table' and sql like '%rowid%'",
  );
  const tables = tablesArray
    .filter(({ tbl_name }) => tbl_name && tbl_name !== 'mergedelete')
    .map(({ tbl_name, sql }) => ({ tableName: tbl_name, sql }));

  const changes: PushJsonRequest['changes'] = [];

  for (const { tableName, sql } of tables) {
    const columns = sql ? opColumnNames(sql) : [];
    if (columns.length === 0) continue;

    const insertsSource = await dbAll(
      `select * from ${tableName} where rowid is null`,
    );
    const updatesSource = await dbAll(
      `select * from ${tableName} where mergeupdate > 0 and rowid is not null`,
    );

    const inserts = insertsSource.map((row: any) => toRow(row, columns));
    const updates = updatesSource.map((row: any) => {
      const rowIdValue = row.rowid;
      const mergeUpdateValue = row.mergeupdate;
      recordsUpdated.push({
        sql: `UPDATE ${tableName} SET mergeupdate=0 WHERE rowid=? AND mergeupdate=?`,
        args: [String(rowIdValue), mergeUpdateValue],
      });
      return toRow(row, columns);
    });

    if (inserts.length || updates.length) {
      changes.push({ table: tableName, inserts, updates });
    }
  }

  const deletesSource = await dbAll('select * from mergedelete');
  const deletes: PushJsonRequest['deletes'] = deletesSource.map((row: any) => {
    const tableIdValue = row.tableid;
    const rowIdValue = row.rowid;
    recordsDeleted.push({
      sql: 'DELETE FROM mergedelete WHERE tableid=? AND rowid=?',
      args: [tableIdValue, rowIdValue],
    });
    return { table: String(tableIdValue), rowid: String(rowIdValue) };
  });

  return { request: { changes, deletes }, recordsUpdated, recordsDeleted };
};
