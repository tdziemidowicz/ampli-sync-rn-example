import type { QueryLike } from '../../db/database';

export type JsonRecords = {
  inserts?: any[];
  updates?: any[];
  deletes?: any[];
};

const cleanIdentifier = (value: string): string =>
  value.trim().replace(/^[[`"]+/, '').replace(/[\]`"]+$/, '');

export const extractInsertColumns = (queryInsert: string): string[] => {
  const match = queryInsert.match(/\binto\s+[^(]+\(([^)]+)\)\s*values/i);
  if (!match) return [];
  return match[1]
    .split(',')
    .map(part => cleanIdentifier(part))
    .filter(Boolean);
};

export const extractUpdateColumns = (
  queryUpdate: string,
): { setColumns: string[]; whereColumn?: string } => {
  const matchSet = queryUpdate.match(/\bset\s+(.+?)\s+where\b/i);
  const setPart = matchSet?.[1] ?? '';
  const setColumns = [...setPart.matchAll(/([^\s=,]+)\s*=\s*\?/gi)].map(m =>
    cleanIdentifier(m[1]),
  );

  const matchWhere = queryUpdate.match(/\bwhere\s+([^\s=]+)\s*=\s*\?/i);
  const whereColumn = matchWhere?.[1] ? cleanIdentifier(matchWhere[1]) : undefined;

  return { setColumns, whereColumn };
};

const getRowValue = (row: any, column: string): unknown => {
  if (!row || typeof row !== 'object') return null;
  return (row as any)[column] ?? null;
};

export const countJsonRecords = (records: JsonRecords | null | undefined): number => {
  if (!records) return 0;
  const inserts = Array.isArray(records.inserts) ? records.inserts.length : 0;
  const updates = Array.isArray(records.updates) ? records.updates.length : 0;
  const deletes = Array.isArray(records.deletes) ? records.deletes.length : 0;
  return inserts + updates + deletes;
};

export const buildBatchQueriesFromJsonRecords = (params: {
  tableName: string;
  records: JsonRecords;
  queryInsert: string;
  queryUpdate: string;
  queryDelete: string;
  triggerInsertDrop: string;
  triggerUpdateDrop: string;
  triggerDeleteDrop: string;
  triggerInsert: string;
  triggerUpdate: string;
  triggerDelete: string;
}): QueryLike[] => {
  const isMergeIdentity = params.tableName === 'mergeidentity';

  const batchQueries: QueryLike[] = [];

  if (!isMergeIdentity) {
    batchQueries.push(
      params.triggerInsertDrop,
      params.triggerUpdateDrop,
      params.triggerDeleteDrop,
    );
  }

  const insertColumns = extractInsertColumns(params.queryInsert);
  const updateInfo = extractUpdateColumns(params.queryUpdate);

  for (const row of params.records.inserts ?? []) {
    const attrs = insertColumns.length
      ? insertColumns.map(col => getRowValue(row, col))
      : Object.values(row);
    batchQueries.push({
      sql: params.queryInsert,
      args: attrs,
    });
  }

  for (const row of params.records.updates ?? []) {
    const attrs = updateInfo.setColumns.length
      ? updateInfo.setColumns.map(col => getRowValue(row, col))
      : Object.values(row);
    if (updateInfo.whereColumn) {
      attrs.push(getRowValue(row, updateInfo.whereColumn));
    }
    batchQueries.push({
      sql: params.queryUpdate,
      args: attrs,
    });
  }

  for (const row of params.records.deletes ?? []) {
    if (!row || typeof row !== 'object') {
      throw new Error(
        `Invalid delete record for table '${params.tableName}': expected object with rowid.`,
      );
    }
    const rowId = (row as any).rowid;
    if (typeof rowId !== 'string' && typeof rowId !== 'number') {
      throw new Error(
        `Invalid delete record for table '${params.tableName}': expected { rowid: string | number }.`,
      );
    }
    const sql = params.queryDelete.includes('?')
      ? params.queryDelete
      : `${params.queryDelete.trimEnd()}?`;
    batchQueries.push({ sql, args: [String(rowId)] });
  }

  if (!isMergeIdentity) {
    batchQueries.push(params.triggerInsert, params.triggerUpdate, params.triggerDelete);
  }

  return batchQueries;
};
