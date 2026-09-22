// §10ij: no router. This app is ONE screen, and expo-router brought expo-linking,
// expo-constants and an UNDECLARED dependency on `query-string` — which resolves under
// npm's flat layout only because something else in the tree happens to provide it, and
// does not resolve under pnpm at all. A single screen needs a root component, not a
// routing table.
import { registerRootComponent } from 'expo';
import App from './App';

registerRootComponent(App);
