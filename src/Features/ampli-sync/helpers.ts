// 1. Define "Loose" Types
// We know the high-level structure (changes/deletes), but not the table contents.
type DynamicRow = Record<string, any>;

interface TableChange {
  table: string;
  inserts: DynamicRow[];
  updates: DynamicRow[];
}

interface DeleteRecord {
  table: string;
  rowid: string;
}

interface SyncData {
  changes: TableChange[];
  deletes: DeleteRecord[];
}

// 2. Initialize the main container
const syncData: SyncData = {
  changes: [],
  deletes: [],
};

// 3. Helper function to add table data dynamically
// This function doesn't care what columns you pass in.
function addTableChange(
  tableName: string,
  newInserts: DynamicRow[],
  newUpdates: DynamicRow[],
) {
  syncData.changes.push({
    table: tableName,
    inserts: newInserts,
    updates: newUpdates,
  });
}

// 4. Usage: Adding "assets_asset" (We don't need to define an interface for it)
addTableChange(
  'assets_asset',
  [
    {
      id: 20860229,
      product_name: 'BATERIA ENERGIZER ALKALICZNA MAX D LR20 /2 new',
      // You can add any random column here without errors
      random_dynamic_column: 'works_fine',
      line_metadata: JSON.stringify([
        { description: 'default_price', amount: 24.26 },
      ]), // Nested JSON string
    },
    {
      id: 20860230,
      product_name: 'BATERIA ENERGIZER ALKALICZNA MAX PLUS',
      quantity: 1,
      // You can omit columns here that were present in the previous row
      custom_flag: true,
    },
  ],
  [], // No updates
);

// 5. Usage: Adding "assets_crew" (Totally different structure, still works)
addTableChange(
  'assets_crew',
  [
    {
      id: 20850074,
      username: 'michal_kuklinski',
      coords_details: JSON.stringify({ 'android.permission': 'blocked' }),
    },
  ],
  [
    {
      id: 20850073,
      rowid: '84c614d4-7d37-47f5-934d-f7d61312bd9c',
      date_start: '2025-11-12 09:56:17.0',
    },
  ],
);

// 6. Add Deletes
syncData.deletes.push({
  table: 'assets_asset',
  rowid: '8428ba91-6d2a-4303-aa61-2e0560059ef9',
});

// 7. Output
console.log(JSON.stringify(syncData, null, 2));
