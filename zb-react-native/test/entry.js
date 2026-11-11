// Alternate Expo app entry for the native ordering test. The normal app entry stays unchanged.
import React from 'react';
import { Text } from 'react-native';
import { registerRootComponent } from 'expo';
import { File, Paths } from 'expo-file-system';
import { checkOrderingModes } from './ordering_modes';

function OrderingProbe() {
  const [label, setLabel] = React.useState('ordering running');
  React.useEffect(() => {
    const result = new File(Paths.cache, 'ordering-result.txt');
    const db = new File(Paths.cache, 'ordering-' + Date.now() + '.sqlite3');
    const finish = (message) => {
      result.write(message);
      setLabel(message);
    };
    checkOrderingModes(process.env.EXPO_PUBLIC_ORDERING_URL, db.uri.replace(/^file:\/\//, ''))
      .then(() => finish('ordering PASS'), error => finish('ordering ERROR ' + error));
  }, []);
  return React.createElement(Text, { accessibilityLabel: label }, label);
}

registerRootComponent(OrderingProbe);
