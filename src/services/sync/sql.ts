import {
  commitSyncAPI,
  pullChangesForTableAPI,
  migrationResultAPI,
  pushChangesAPI,
} from './api';
import type { MigrationRow } from './api';
import {
  buildBatchQueriesFromJsonRecords,
  countJsonRecords,
} from './formats/json';
import { buildPushJsonRequestFromDb } from './formats/pushJson';
import { dbAll, dbBatch, dbExec, type QueryLike } from '../db/database';

type SyncParams = {
  syncUrl: string;
  accessToken: string;
  deviceUniqueId: string;
};

type TableSyncParams = SyncParams & { tableName: string };

type UpdateDatabaseParams = {
  syncUrl: string;
  accessToken: string;
  data: MigrationRow[];
};

type ProgressFn = (progress: number) => void;

const convertDateTimeToUTCDateString = (value: unknown): string =>
  new Date(value as any).toISOString().slice(0, 10);

const convertDateTimeToUTCTimeString = (value: unknown): string =>
  new Date(value as any).toISOString().slice(11, 19);

export const sendChangesQuery = async (params: SyncParams): Promise<void> => {
  const { request, recordsUpdated, recordsDeleted } =
    await buildPushJsonRequestFromDb(dbAll);

  await pushChangesAPI({
    syncUrl: params.syncUrl,
    accessToken: params.accessToken,
    data: request,
    deviceUniqueId: params.deviceUniqueId,
  });

  await clearUpdateMarker(recordsUpdated);
  await clearDeletedRecords(recordsDeleted);
};

const clearUpdateMarker = async (recordsUpdated: QueryLike[]) => {
  if (!recordsUpdated?.length) return;
  await dbBatch(recordsUpdated);
};

const clearDeletedRecords = async (recordsDeleted: QueryLike[]) => {
  if (!recordsDeleted?.length) return;
  await dbBatch(recordsDeleted);
};

export const updateDatabaseQuery = async ({
  syncUrl,
  accessToken,
  data,
}: UpdateDatabaseParams): Promise<void> => {
  for (const { id, query } of data) {
    const time_start = new Date();
    try {
      if (/^\s*initialize\s+table\b/i.test(query)) {
        throw new Error(
          `Unsupported migration query: ${query}. Backend should return full SQL migrations (JSON-only client).`,
        );
      }

      await dbExec(query);

      const execution_time = Date.now() - time_start.getTime();
      await migrationResultAPI({
        syncUrl,
        id,
        accessToken,
        executionTime: execution_time,
        result: `ok ${convertDateTimeToUTCDateString(
          time_start,
        )} ${convertDateTimeToUTCTimeString(time_start)} ''`,
      });
    } catch (error: any) {
      const execution_time = Date.now() - time_start.getTime();
      try {
        await migrationResultAPI({
          syncUrl,
          id,
          accessToken,
          executionTime: execution_time,
          result: error?.message || error,
        });
      } catch {
        // ignore
      }
      throw error;
    }
  }
};

export const receiveChangesQuery = async (
  params: SyncParams,
  onProgress: ProgressFn,
): Promise<unknown | null> => {
  let firstError: unknown | null = null;

  const tablesArray = await dbAll<{ tbl_name: string }>(
    "SELECT tbl_name FROM sqlite_master WHERE type='table' AND sql like '%rowid%' AND tbl_name!='mergedelete'",
  );
  const tableNames = tablesArray.map(row => row.tbl_name).filter(Boolean);

  for (let tableIndex = 0; tableIndex < tableNames.length; tableIndex++) {
    const tableName = tableNames[tableIndex];
    if (firstError) break;

    while (true) {
      const { maxPackageSize, recordCount, error } =
        await pullAndApplyRemoteChangesOnce({ tableName, ...params });

      if (error) {
        if (!firstError) firstError = error;
        break;
      }

      const shouldContinue =
        maxPackageSize > 0 && recordCount > 0 && maxPackageSize >= recordCount;
      if (!shouldContinue) break;
    }

    onProgress((4 + tableIndex) / (tableNames.length + 3));
  }

  return firstError;
};

const pullAndApplyRemoteChangesOnce = async (params: TableSyncParams) => {
  let recordCount = 0;
  let maxPackageSize = 0;
  let error: unknown | null = null;

  const data = await pullChangesForTableAPI(params);
  if (!Array.isArray(data) || data.length === 0) {
    throw new Error(
      `Invalid pull response for table '${params.tableName}': expected non-empty array.`,
    );
  }

  const row = data[0];
  maxPackageSize = Number(row.MaxPackageSize) || 0;
  if (row.SyncId <= 0) {
    return { maxPackageSize, recordCount, error };
  }

  if (!row.Records || typeof row.Records !== 'object') {
    throw new Error(
      `Invalid Records for table '${params.tableName}': expected JSON object.`,
    );
  }

  recordCount = countJsonRecords(row.Records);
  const batchQueries: QueryLike[] = buildBatchQueriesFromJsonRecords({
    tableName: params.tableName,
    records: row.Records,
    queryInsert: row.QueryInsert,
    queryUpdate: row.QueryUpdate,
    queryDelete: row.QueryDelete,
    triggerInsertDrop: row.TriggerInsertDrop,
    triggerUpdateDrop: row.TriggerUpdateDrop,
    triggerDeleteDrop: row.TriggerDeleteDrop,
    triggerInsert: row.TriggerInsert,
    triggerUpdate: row.TriggerUpdate,
    triggerDelete: row.TriggerDelete,
  });

  try {
    console.log(batchQueries);
    if (batchQueries.length > 0) await dbBatch(batchQueries);
    await commitSyncAPI({
      syncUrl: params.syncUrl,
      syncId: row.SyncId,
      accessToken: params.accessToken,
    });
  } catch (err) {
    error = err;
    maxPackageSize = 0;
    recordCount = 0;
  }

  return { maxPackageSize, recordCount, error };
};
