import { useEffect, useState } from 'react';
import { ActivityIndicator, StyleSheet, View } from 'react-native';

import type { Session } from './src/auth/types';
import { LoginScreen } from './src/screens/LoginScreen';
import { MainScreen } from './src/screens/MainScreen';
import { clearSession, loadSession } from './src/services/session/sessionStorage';
import {
  initializeForSession,
  logoutCleanup,
  syncPull,
  syncSend,
} from './src/services/sync/service';
import {
  addTestAssetCategoryRecord,
  deleteLastTestAssetCategoryRecord,
} from './src/services/db/assetsAssetCategoryDebug';

const App = () => {
  const [isBooting, setIsBooting] = useState(true);
  const [session, setSession] = useState<Session | null>(null);
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncInfo, setSyncInfo] = useState<string | null>(null);
  const [lastTestAssetCategoryId, setLastTestAssetCategoryId] = useState<
    string | null
  >(null);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const stored = await loadSession();
        if (mounted) setSession(stored);
      } finally {
        if (mounted) setIsBooting(false);
      }
    })();
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    if (!session) return;
    let canceled = false;

    (async () => {
      setIsSyncing(true);
      setSyncInfo('Syncing database...');
      try {
        await initializeForSession(session);
        if (canceled) return;
        if (!canceled) setSyncInfo('Database: OK');
      } catch (e) {
        if (!canceled) {
          setSyncInfo(e instanceof Error ? e.message : String(e));
        }
      } finally {
        if (!canceled) setIsSyncing(false);
      }
    })();

    return () => {
      canceled = true;
    };
  }, [session]);

  const handleAddTestAssetCategory = async () => {
    setIsSyncing(true);
    setSyncInfo('Adding assets_assetcategory…');
    try {
      const { id } = await addTestAssetCategoryRecord();
      setLastTestAssetCategoryId(id);
      setSyncInfo(`Added assets_assetcategory: ${id}`);
    } catch (e) {
      setSyncInfo(e instanceof Error ? e.message : String(e));
    } finally {
      setIsSyncing(false);
    }
  };

  const handleDeleteLastTestAssetCategory = async () => {
    setIsSyncing(true);
    setSyncInfo('Deleting last record from assets_assetcategory...');
    try {
      const deleted = await deleteLastTestAssetCategoryRecord(
        lastTestAssetCategoryId,
      );
      setLastTestAssetCategoryId(null);
      setSyncInfo(
        deleted
          ? `Deleted assets_assetcategory: ${deleted.id}`
          : 'No records...',
      );
    } catch (e) {
      setSyncInfo(e instanceof Error ? e.message : String(e));
    } finally {
      setIsSyncing(false);
    }
  };

  const handleSend = async () => {
    const currentSession = session;
    if (!currentSession) return;

    setIsSyncing(true);
    setSyncInfo('Sending...');
    try {
      await syncSend(currentSession);
      setSyncInfo('Sending: DONE. Syncing...');
      await syncPull(currentSession, progress => {
        setSyncInfo(`Syncing: ${Math.ceil(progress * 100)}%`);
      });
      setSyncInfo('Syncing: DONE');
    } catch (e) {
      setSyncInfo(e instanceof Error ? e.message : String(e));
    } finally {
      setIsSyncing(false);
    }
  };

  const handlePull = async () => {
    const currentSession = session;
    if (!currentSession) return;

    setIsSyncing(true);
    setSyncInfo('Syncing...');
    try {
      await syncPull(currentSession, progress => {
        setSyncInfo(`Syncing: ${Math.ceil(progress * 100)}%`);
      });
      setSyncInfo('Syncing: DONE');
    } catch (e) {
      setSyncInfo(e instanceof Error ? e.message : String(e));
    } finally {
      setIsSyncing(false);
    }
  };

  const handleLogout = async () => {
    setIsSyncing(false);
    setSyncInfo(null);
    setSession(null);
    setLastTestAssetCategoryId(null);
    await logoutCleanup();
    await clearSession();
  };

  if (isBooting) {
    return (
      <View style={styles.container}>
        <ActivityIndicator />
      </View>
    );
  }

  if (!session) {
    return <LoginScreen onLoggedIn={setSession} />;
  }

  return (
    <MainScreen
      session={session}
      onAddTestAssetCategory={handleAddTestAssetCategory}
      onDeleteLastTestAssetCategory={handleDeleteLastTestAssetCategory}
      onSend={handleSend}
      onPull={handlePull}
      onLogout={handleLogout}
      isSyncing={isSyncing}
      syncInfo={syncInfo}
    />
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
});

export default App;
