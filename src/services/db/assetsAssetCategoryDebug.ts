import { dbAll, dbExec } from './database';
import uuid from 'react-native-uuid';

const TABLE_NAME = 'assets_assetcategory';
const TEST_SLUG_PREFIX = 'category-from-app-';

const toSqlDateTimeUTC = (date: Date): string =>
  date.toISOString().replace('T', ' ').slice(0, 19);

export const addTestAssetCategoryRecord = async (): Promise<{
  id: string;
  slug: string;
}> => {
  const now = new Date();
  const createdAt = toSqlDateTimeUTC(now);
  const id = uuid.v4();
  const slug = `${TEST_SLUG_PREFIX}${Date.now()}`;
  const name = `Test category ${slug.slice(TEST_SLUG_PREFIX.length)}`;
  const description = `Created from app at ${createdAt}`;

  const lft = 1;
  const rght = 2;
  const treeId = 1;
  const level = 0;
  const parentId = null;

  await dbExec({
    sql: `INSERT INTO "${TABLE_NAME}" (id, name, slug, description, created_at, lft, rght, tree_id, level, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    args: [
      id,
      name,
      slug,
      description,
      createdAt,
      lft,
      rght,
      treeId,
      level,
      parentId,
    ],
  });

  return { id, slug };
};

export const deleteLastTestAssetCategoryRecord = async (
  lastKnownId?: string | null,
): Promise<{ id: string } | null> => {
  const byId = async (id: string) => {
    const rows = await dbAll<{ id: string; rowId: string | null }>({
      sql: `SELECT id, [rowid] as rowId FROM "${TABLE_NAME}" WHERE id = ? LIMIT 1`,
      args: [id],
    });
    return rows?.[0] ?? null;
  };

  const findLatest = async () => {
    const rows = await dbAll<{ id: string; rowId: string | null }>({
      sql: `SELECT id, [rowid] as rowId FROM "${TABLE_NAME}" WHERE slug LIKE ? ORDER BY _rowid_ DESC LIMIT 1`,
      args: [`${TEST_SLUG_PREFIX}%`],
    });
    return rows?.[0] ?? null;
  };

  const record = lastKnownId ? await byId(lastKnownId) : await findLatest();
  if (!record) return null;

  await dbExec({
    sql: `DELETE FROM "${TABLE_NAME}" WHERE id = ?`,
    args: [record.id],
  });

  const hasServerRowId = record.rowId != null;
  if (!hasServerRowId) {
    await dbExec({
      sql: `DELETE FROM "mergedelete" WHERE tableid = ? AND rowid IS NULL`,
      args: [TABLE_NAME],
    });
  }

  return { id: record.id };
};
