require('dotenv').config()
const mqtt = require('mqtt')
const { createClient } = require('@supabase/supabase-js')
const { Redis } = require('@upstash/redis')
const http = require('http')

const MQTT_HOST = `wss://${process.env.MQTT_HOST}:8884/mqtt`
const MQTT_USERNAME = process.env.MQTT_USERNAME
const MQTT_PASSWORD = process.env.MQTT_PASSWORD
const SUPABASE_URL = process.env.SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_KEY

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY)
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
})

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

  await redis.set(`capteur:${capteur}`, valeur)
  console.log(`Redis mis à jour → capteur:${capteur} = ${valeur}`)
})

client.on('error', (err) => {
  console.log('Erreur de connexion:', err.message)
})

const express = require('express')
const app = express()
const PORT = process.env.PORT || 3000

// Route santé
app.get('/health', (req, res) => {
  res.json({ status: 'ok', message: 'Agri Server Running ✅' })
})

// Dernières valeurs depuis Redis
app.get('/capteurs/latest', async (req, res) => {
  const capteurs = ['temperature', 'humidite_sol', 'humidite_air', 'luminosite', 'co2', 'niveau_eau']
  const data = {}
  
  for (const capteur of capteurs) {
    const valeur = await redis.get(`capteur:${capteur}`)
    data[capteur] = valeur
  }
  
  res.json(data)
})

// Historique depuis Supabase
app.get('/capteurs/historique', async (req, res) => {
  const { data, error } = await supabase
    .from('releves')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(100)
  
  if (error) return res.status(500).json({ error: error.message })
  res.json(data)
})

app.listen(PORT, () => {
  console.log(`Serveur HTTP actif sur port ${PORT}`)
})