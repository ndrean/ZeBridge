/// One table, one clock — on a phone. The SEED of a big table from the generation chain
/// through libzb (src/libzb-seed.tsx, the zb-react-native module), then the live tail.
/// Nothing else — no mutation. The three facts at the end (rows, distinct keys, a sum)
/// are the ones the Node, Flutter and browser seeds are checked with against PostgreSQL,
/// so a run here is a measurement.
import { StyleSheet, Text, View } from 'react-native';
import { LibzbSeed } from './src/libzb-seed';

export default function App() {
  return (
    <View style={s.root}>
      <Text style={s.title}>ZeBridge — one large table</Text>
      <LibzbSeed />
    </View>
  );
}

const s = StyleSheet.create({
  root: { flex: 1, backgroundColor: '#121212', paddingTop: 56, paddingHorizontal: 14 },
  title: { color: '#e0e0e0', fontSize: 18, fontWeight: '700' },
});
