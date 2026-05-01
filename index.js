require('dotenv').config()
const mqtt = require('mqtt')
const { createClient } = require('@supabase/supabase-js')

const MQTT_HOST = `wss://${process.env.MQTT_HOST}:8884/mqtt`
const MQTT_USERNAME = process.env.MQTT_USERNAME
const MQTT_PASSWORD = process.env.MQTT_PASSWORD
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_KEY

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

client.on('message', async (topic, message) => {
  const valeur = parseFloat(message.toString())
  const capteur = topic.split('/').pop()
  
  const unites = {
    'temperature': '°C',
    'humidite_sol': '%',
    'humidite_air': '%',
    'luminosite': 'lux',
    'co2': 'ppm',
    'niveau_eau': 'cm'
  }
  
  const { error } = await supabase
    .from('releves')
    .insert({ capteur, valeur, unite: unites[capteur] || '' })
  
  if (error) console.log('Erreur Supabase:', error.message)
  else console.log(`✅ ${capteur}: ${valeur} ${unites[capteur]}`)
})

client.on('error', (err) => {
  console.log('Erreur de connexion:', err.message)
})
const http = require('http')
const PORT = process.env.PORT || 3000

http.createServer((req, res) => {
  res.writeHead(200)
  res.end('Agri Server Running ✅')
}).listen(PORT, () => {
  console.log(`Serveur HTTP actif sur port ${PORT}`)
})