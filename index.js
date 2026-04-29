const mqtt = require('mqtt')
const { createClient } = require('@supabase/supabase-js')

const MQTT_HOST = 'wss://0792197711f646529230ffbc39922ad3.s1.eu.hivemq.cloud:8884/mqtt'
const MQTT_USERNAME = 'tim_kenmegne'
const MQTT_PASSWORD = 'Group10.'

const SUPABASE_URL = 'https://kyytlcztsnxkvbqvzege.supabase.co'
const SUPABASE_KEY = 'sb_publishable_TK6i1h56GUhD3IfEpUBhKg_CwntSq2y'

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
const client = mqtt.connect(MQTT_HOST, {
  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,
  rejectUnauthorized: false
})

client.on('connect', () => {
  console.log('Connecté à HiveMQ ✅')
  client.subscribe('agri/capteurs/#', (err) => {
    if (!err) console.log('Souscrit aux topics capteurs ✅')
  })
})

client.on('message', (topic, message) => {
  console.log(`Topic: ${topic} | Message: ${message.toString()}`)
})

client.on('error', (err) => {
  console.log('Erreur de connexion:', err.message)
})