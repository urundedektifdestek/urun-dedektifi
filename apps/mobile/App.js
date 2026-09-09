import React from 'react';
import {SafeAreaView, Text, TextInput, Button, StyleSheet} from 'react-native';

export default function App(){
  return <SafeAreaView style={s.root}>
    <Text style={s.title}>Ürün Dedektifii</Text>
    <Text>AI Oda ve ürün analizi için backend bağlanacak.</Text>
  </SafeAreaView>
}
const s = StyleSheet.create({root:{flex:1,padding:24,justifyContent:'center'},title:{fontSize:28,fontWeight:'900'}});
