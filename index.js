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

// Charger les seuils dans Redis au démarrage
async function chargerSeuils() {
  const { data, error } = await supabase.from('seuils').select('*')
  if (error) return console.log('Erreur chargement seuils:', error.message)
  
  for (const seuil of data) {
    await redis.set(`seuil:${seuil.capteur}`, JSON.stringify({
      min: seuil.valeur_min,
      max: seuil.valeur_max
    }))
  }
  console.log('Seuils chargés dans Redis ✅')
}

chargerSeuils()
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
  
  // Stocker dans Supabase
  const { error } = await supabase
    .from('releves')
    .insert({ capteur, valeur, unite: unites[capteur] || '' })
  
  if (error) console.log('Erreur Supabase:', error.message)
  else console.log(`✅ ${capteur}: ${valeur} ${unites[capteur]}`)

  // Mettre à jour Redis
  await redis.set(`capteur:${capteur}`, valeur)

  // Vérifier les seuils
  const seuilData = await redis.get(`seuil:${capteur}`)
  if (seuilData) {
    const seuil = typeof seuilData === 'string' ? JSON.parse(seuilData) : seuilData
    
    if (valeur < seuil.min) {
      await supabase.from('alerts').insert({
        type: `low_${capteur}`,
        message: `${capteur} trop bas (${valeur} ${unites[capteur] || ''})`,
        severity: 'critical',
        resolved: false,
         timestamp: new Date()
      })
      console.log(`🚨 Alerte : ${capteur} trop bas`)
    } else if (valeur > seuil.max) {
      await supabase.from('alerts').insert({
        type: `high_${capteur}`,
        message: `${capteur} trop élevé (${valeur} ${unites[capteur] || ''})`,
        severity: 'medium',
        resolved: false,
        timestamp: new Date()
      })
      console.log(`⚠️ Alerte : ${capteur} trop élevé`)
    }
  }
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

// Lire les seuils
app.get('/seuils', async (req, res) => {
  const { data, error } = await supabase
    .from('seuils')
    .select('*')
  
  if (error) return res.status(500).json({ error: error.message })
  res.json(data)
})

// Modifier un seuil
app.put('/seuils/:capteur', express.json(), async (req, res) => {
  const { capteur } = req.params
  const { valeur_min, valeur_max } = req.body
  
  const { data, error } = await supabase
    .from('seuils')
    .update({ valeur_min, valeur_max, updated_at: new Date() })
    .eq('capteur', capteur)
  
  if (error) return res.status(500).json({ error: error.message })
  res.json({ message: `Seuils de ${capteur} mis à jour ✅` })
})

// Contrôle des actionneurs
app.post('/actionneurs/:nom', express.json(), (req, res) => {
  const { nom } = req.params
  const { etat } = req.body // "ON" ou "OFF"
  
  const actionneurs = ['pompe', 'ventilateur', 'eclairage']
  
  if (!actionneurs.includes(nom)) {
    return res.status(400).json({ error: `Actionneur "${nom}" inconnu` })
  }
  
  client.publish(`agri/actionneurs/${nom}`, etat, (err) => {
    if (err) return res.status(500).json({ error: 'Erreur publication MQTT' })
    console.log(`Actionneur ${nom} → ${etat}`)
    res.json({ message: `${nom} ${etat} ✅` })
  })
})

// Lire les alertes
app.get('/alertes', async (req, res) => {
  const { data, error } = await supabase
    .from('alerts')
    .select('*')
    .order('timestamp', { ascending: false })
    .limit(50)
  
  if (error) return res.status(500).json({ error: error.message })
  res.json(data)
})

// Marquer une alerte comme résolue
app.put('/alertes/:id/resolve', express.json(), async (req, res) => {
  const { id } = req.params
  
  const { error } = await supabase
    .from('alerts')
    .update({ resolved: true, resolved_at: new Date() })
    .eq('id', id)
  
  if (error) return res.status(500).json({ error: error.message })
  res.json({ message: `Alerte ${id} résolue ✅` })
})
// Middleware API Key
const authenticateApiKey = (req, res, next) => {
  const authHeader = req.headers['authorization']
  
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Accès non autorisé — API Key manquante' })
  }
  
  const apiKey = authHeader.split(' ')[1]
  
  if (apiKey !== process.env.API_KEY) {
    return res.status(403).json({ error: 'API Key invalide' })
  }
  
  next()
}

// Appliquer sur toutes les routes
app.use(authenticateApiKey)