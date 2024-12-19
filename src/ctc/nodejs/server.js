// src/ctc/server.js
const express = require('express');
const axios = require('axios');
const app = express();
const port = 5000;

let API_KEY = '70656a4c102bee1ed903fcb7e3938267';
const BASE_URL = 'http://api.openweathermap.org/data/2.5/weather';
let currentCity = 'Alicante';

// Middleware para manejar JSON
app.use(express.json());

// Endpoint para obtener el estado del tráfico
app.get('/api/traffic-status', async (req, res) => {
  try {
    const response = await axios.get(BASE_URL, {
      params: {
        q: currentCity,
        appid: API_KEY,
        units: 'metric'
      }
    });
    const temp = response.data.main.temp;
    const status = temp > 0 ? 'OK' : 'KO';
    res.json({ status, city: currentCity, temperature: temp });
  } catch (error) {
    console.error('Error al obtener la temperatura:', error);
    res.status(500).json({ error: 'Error al obtener la temperatura' });
  }
});

// Endpoint para cambiar la ciudad actual
app.post('/api/city/:cityName', (req, res) => {
  currentCity = req.params.cityName;
  console.log(`Ciudad cambiada a ${currentCity}`);
  res.json({ message: `Ciudad cambiada a ${currentCity}` });
});

app.post('/api/apikey/:newKey', (req, res) => {
  API_KEY = req.params.newKey;
  console.log(`API Key actualizada a: ${API_KEY}`);
  res.json({ message: `API Key actualizada correctamente` });
});

// Iniciar el servidor
app.listen(port, '0.0.0.0', () => {
  console.log(`Servidor CTC ejecutándose en el puerto ${port}`);
});
