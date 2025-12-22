import React, { JSX } from 'react';
import { Alert, Button, StyleSheet, View } from 'react-native';

function App(): JSX.Element {
  const handlePress = () => {
    Alert.alert('Hello', 'Button clicked');
  };

  return (
    <View style={styles.container}>
      <Button title="Click me" onPress={handlePress} />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center', // center vertically
    alignItems: 'center', // center horizontally
  },
});

export default App;
