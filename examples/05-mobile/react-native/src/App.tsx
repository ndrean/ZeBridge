import React, { useEffect, useState, useRef } from 'react';
import {
  SafeAreaView,
  ScrollView,
  View,
  Text,
  TextInput,
  TouchableOpacity,
  StyleSheet,
  Switch,
  ActivityIndicator
} from 'react-native';
import { ZeBridge } from './ZeBridge';

const ZB = new ZeBridge({
  url: 'ws://localhost:8080',
  dbPath: 'zb.sqlite3',
  principal: 'alice',
  tables: ['counter_public', 'counter_tenant', 'app_users', 'app_orders'],
  clientId: 'react-native-client',
});

export default function App() {
  const [isConnected, setIsConnected] = useState(false);
  const [tenant, setTenant] = useState('—');

  const [counterPublic, setCounterPublic] = useState<any>({});
  const [counterTenant, setCounterTenant] = useState<any>({});
  const [users, setUsers] = useState<any[]>([]);
  const [orders, setOrders] = useState<any[]>([]);

  // SQL Console
  const [sqlText, setSqlText] = useState("SELECT count, note, updated_at, last_writer FROM app_orders ORDER BY updated_at DESC LIMIT 3");
  const [sqlRows, setSqlRows] = useState<any[]>([]);
  const [sqlError, setSqlError] = useState<string | null>(null);
  const [sqlLive, setSqlLive] = useState(true);

  // Forms
  const [formUserName, setFormUserName] = useState('');
  const [formItem, setFormItem] = useState('');
  const [formCount, setFormCount] = useState('1');
  const [formNote, setFormNote] = useState('');

  const mounted = useRef(true);

  useEffect(() => {
    try {
      const info = ZB.sync();
      setTenant(info.tenant || '—');
      setIsConnected(true);
      refreshAll();
      startPolling();
    } catch (e: any) {
      console.error(e);
    }

    return () => {
      mounted.current = false;
      ZB.close();
    };
  }, []);

  const startPolling = async () => {
    while (mounted.current) {
      try {
        const report = await ZB.poll(1000);
        let needsRefresh = false;
        
        if (report.seeded.length > 0 || report.changedTables.length > 0) {
          needsRefresh = true;
        }

        if (needsRefresh) {
          refreshAll([...report.changedTables, ...report.seeded]);
        }
        
        await ZB.flush(0);
      } catch (e) {
        console.warn("Poll error:", e);
        await new Promise(r => setTimeout(r, 1000));
      }
      await new Promise(r => setTimeout(r, 0)); // Yield to event loop
    }
  };

  const refreshAll = (changedTables?: string[]) => {
    if (!changedTables || changedTables.includes('counter_public')) {
      try {
        const res = ZB.query("SELECT value, updated_at, last_writer FROM counter_public WHERE uid = '00000000-0000-4000-8000-00000000c0de'");
        if (res.length > 0) setCounterPublic(res[0]);
      } catch (e) {}
    }

    if (!changedTables || changedTables.includes('counter_tenant')) {
      try {
        const res = ZB.query("SELECT value, updated_at, last_writer FROM counter_tenant LIMIT 1");
        if (res.length > 0) setCounterTenant(res[0]);
      } catch (e) {}
    }

    if (!changedTables || changedTables.includes('app_users')) {
      try {
        setUsers(ZB.query("SELECT uid, name FROM app_users WHERE deleted_at IS NULL ORDER BY name"));
      } catch (e) {}
    }

    if (!changedTables || changedTables.includes('app_orders')) {
      try {
        setOrders(ZB.query("SELECT * FROM app_orders WHERE deleted_at IS NULL ORDER BY updated_at DESC"));
      } catch (e) {}
    }

    if (sqlLive) {
      runSql(sqlText);
    }
  };

  const runSql = (query: string) => {
    if (!query.trim()) return;
    try {
      const res = ZB.query(query);
      setSqlRows(res);
      setSqlError(null);
    } catch (e: any) {
      setSqlError(e.message);
      setSqlRows([]);
    }
  };

  const bumpPublicCounter = (delta: number) => {
    const uid = '00000000-0000-4000-8000-00000000c0de';
    if (!counterPublic.value && counterPublic.value !== 0) {
      const v = new Date().toISOString();
      ZB.mutate('counter_public', 'INSERT', { uid }, { uid, value: delta, inserted_at: v, updated_at: v });
    } else {
      ZB.mutate('counter_public', 'UPDATE', { uid }, { value: counterPublic.value + delta });
    }
    refreshAll(['counter_public']);
  };

  const bumpTenantCounter = (delta: number) => {
    const uid = counterTenant.uid || '00000000-0000-4000-8000-8b0b2beac0df';
    if (!counterTenant.value && counterTenant.value !== 0) {
      const v = new Date().toISOString();
      ZB.mutate('counter_tenant', 'INSERT', { uid }, { uid, tenant_id: tenant, value: delta, inserted_at: v, updated_at: v });
    } else {
      ZB.mutate('counter_tenant', 'UPDATE', { uid }, { value: counterTenant.value + delta });
    }
    refreshAll(['counter_tenant']);
  };

  const createOrder = () => {
    if (!formUserName.trim() || !formItem.trim()) return;
    
    let userId = '';
    const existingUser = users.find(u => u.name === formUserName.trim());
    
    if (!existingUser) {
      userId = Date.now().toString() + "-user";
      const v = new Date().toISOString();
      ZB.mutate('app_users', 'INSERT', { uid: userId }, {
        uid: userId, name: formUserName.trim(), tenant_id: tenant, inserted_at: v, updated_at: v
      });
    } else {
      userId = existingUser.uid;
    }
    
    const v = new Date().toISOString();
    const count = parseInt(formCount, 10) || 1;
    ZB.mutate('app_orders', 'INSERT', { user_id: userId, item: formItem.trim() }, {
      user_id: userId, item: formItem.trim(), count, note: formNote.trim() || null, tenant_id: tenant, inserted_at: v, updated_at: v
    });
    
    setFormUserName('');
    setFormItem('');
    setFormCount('1');
    setFormNote('');
    refreshAll(['app_users', 'app_orders']);
  };

  return (
    <SafeAreaView style={styles.container}>
      <ScrollView contentContainerStyle={styles.scroll}>
        <Text style={styles.headerTitle}>ZeBridge React Native</Text>
        <Text style={styles.subtitle}>Principal: alice  ·  Tenant: {tenant}</Text>
        
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Two Counters</Text>
          <View style={styles.row}>
            {/* Public Counter */}
            <View style={styles.card}>
              <Text style={styles.cardTitle}>counter_public</Text>
              <View style={styles.counterRow}>
                <TouchableOpacity style={styles.btn} onPress={() => bumpPublicCounter(-1)}><Text style={styles.btnText}>-</Text></TouchableOpacity>
                <Text style={styles.counterValue}>{counterPublic.value ?? 0}</Text>
                <TouchableOpacity style={styles.btn} onPress={() => bumpPublicCounter(1)}><Text style={styles.btnText}>+</Text></TouchableOpacity>
              </View>
              <Text style={styles.vText}>v: {counterPublic.updated_at ?? '—'}</Text>
            </View>

            {/* Tenant Counter */}
            <View style={styles.card}>
              <Text style={styles.cardTitle}>counter_tenant</Text>
              <View style={styles.counterRow}>
                <TouchableOpacity style={styles.btn} onPress={() => bumpTenantCounter(-1)}><Text style={styles.btnText}>-</Text></TouchableOpacity>
                <Text style={styles.counterValue}>{counterTenant.value ?? 0}</Text>
                <TouchableOpacity style={styles.btn} onPress={() => bumpTenantCounter(1)}><Text style={styles.btnText}>+</Text></TouchableOpacity>
              </View>
              <Text style={styles.vText}>v: {counterTenant.updated_at ?? '—'}</Text>
            </View>
          </View>
        </View>

        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Users ⟶ Orders</Text>
          <View style={styles.formCard}>
            <TextInput style={styles.input} placeholder="User" value={formUserName} onChangeText={setFormUserName} />
            <TextInput style={styles.input} placeholder="Item" value={formItem} onChangeText={setFormItem} />
            <View style={styles.row}>
              <TextInput style={[styles.input, { flex: 1, marginRight: 8 }]} placeholder="Count" keyboardType="numeric" value={formCount} onChangeText={setFormCount} />
              <TextInput style={[styles.input, { flex: 1 }]} placeholder="Note" value={formNote} onChangeText={setFormNote} />
            </View>
            <TouchableOpacity style={styles.submitBtn} onPress={createOrder}>
              <Text style={styles.submitBtnText}>CREATE ORDER</Text>
            </TouchableOpacity>

            <View style={styles.divider} />
            <Text style={{ fontWeight: 'bold', marginBottom: 8 }}>Orders List</Text>
            {orders.map((o, i) => {
              const u = users.find(u => u.uid === o.user_id)?.name || o.user_id;
              return (
                <View key={i} style={styles.orderItem}>
                  <Text style={{ fontWeight: '600' }}>{o.item} x {o.count}</Text>
                  <Text style={{ color: '#666' }}>{u} {o.note ? ` · ${o.note}` : ''}</Text>
                </View>
              );
            })}
          </View>
        </View>

        <View style={styles.section}>
          <Text style={styles.sectionTitle}>SQL Console</Text>
          <TextInput
            style={[styles.input, { height: 80 }]}
            multiline
            value={sqlText}
            onChangeText={setSqlText}
          />
          <View style={styles.row}>
            <TouchableOpacity style={[styles.submitBtn, { flex: 1, marginRight: 16 }]} onPress={() => runSql(sqlText)}>
              <Text style={styles.submitBtnText}>RUN</Text>
            </TouchableOpacity>
            <View style={{ flexDirection: 'row', alignItems: 'center' }}>
              <Switch value={sqlLive} onValueChange={setSqlLive} />
              <Text style={{ marginLeft: 8 }}>Live</Text>
            </View>
          </View>

          {sqlError && <Text style={{ color: 'red', marginTop: 8 }}>{sqlError}</Text>}
          
          {sqlRows.length > 0 && (
            <ScrollView horizontal style={styles.table}>
              <View>
                <View style={styles.tableRow}>
                  {Object.keys(sqlRows[0]).map((k, i) => (
                    <Text key={i} style={styles.tableHeaderCell}>{k}</Text>
                  ))}
                </View>
                {sqlRows.map((row, i) => (
                  <View key={i} style={styles.tableRow}>
                    {Object.values(row).map((v: any, j) => (
                      <Text key={j} style={styles.tableCell}>{String(v)}</Text>
                    ))}
                  </View>
                ))}
              </View>
            </ScrollView>
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#f5f5f5' },
  scroll: { padding: 16 },
  headerTitle: { fontSize: 24, fontWeight: 'bold', textAlign: 'center' },
  subtitle: { fontSize: 14, color: '#666', textAlign: 'center', marginBottom: 24 },
  section: { marginBottom: 32 },
  sectionTitle: { fontSize: 20, fontWeight: 'bold', marginBottom: 12 },
  row: { flexDirection: 'row', justifyContent: 'space-between' },
  card: { flex: 1, backgroundColor: 'white', padding: 16, borderRadius: 8, marginHorizontal: 4, alignItems: 'center', elevation: 2 },
  cardTitle: { fontSize: 16, fontWeight: '600', marginBottom: 12 },
  counterRow: { flexDirection: 'row', alignItems: 'center' },
  btn: { backgroundColor: '#e0e0e0', width: 32, height: 32, borderRadius: 16, alignItems: 'center', justifyContent: 'center' },
  btnText: { fontSize: 20, fontWeight: 'bold' },
  counterValue: { fontSize: 28, fontWeight: 'bold', marginHorizontal: 16 },
  vText: { fontSize: 10, color: '#aaa', marginTop: 8 },
  formCard: { backgroundColor: 'white', padding: 16, borderRadius: 8, elevation: 2 },
  input: { backgroundColor: '#f9f9f9', borderWidth: 1, borderColor: '#ddd', borderRadius: 4, padding: 12, marginBottom: 12 },
  submitBtn: { backgroundColor: '#6200ee', padding: 12, borderRadius: 4, alignItems: 'center' },
  submitBtnText: { color: 'white', fontWeight: 'bold' },
  divider: { height: 1, backgroundColor: '#eee', marginVertical: 16 },
  orderItem: { paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: '#f0f0f0' },
  table: { marginTop: 16, backgroundColor: 'white', borderRadius: 4, padding: 8, elevation: 1 },
  tableRow: { flexDirection: 'row', borderBottomWidth: 1, borderBottomColor: '#eee' },
  tableHeaderCell: { fontWeight: 'bold', padding: 8, minWidth: 100 },
  tableCell: { padding: 8, minWidth: 100 }
});
