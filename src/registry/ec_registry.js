const https = require('https');
const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");

// Configuración inicial
const port = 10000;
const app = express();
const taxisFilePath = "/app/data/taxis.json"; // Ruta del archivo JSON

// Verificar si el archivo de datos existe; si no, crearlo
if (!fs.existsSync(taxisFilePath)) {
    fs.mkdirSync(path.dirname(taxisFilePath), { recursive: true });
    fs.writeFileSync(taxisFilePath, JSON.stringify({}, null, 4), "utf8");
}

// Middleware para procesar JSON
app.use(bodyParser.json());

// Función para cargar los datos del archivo JSON
function loadTaxis() {
    try {
        const data = fs.readFileSync(taxisFilePath, "utf8");
        let taxis = JSON.parse(data);

        // Convertir lista a objeto si es necesario
        if (Array.isArray(taxis)) {
            const taxisDict = {};
            taxis.forEach(taxi => {
                if (taxi && taxi.id !== undefined) {
                    taxisDict[taxi.id] = taxi;
                }
            });
            taxis = taxisDict;
            saveTaxisToFile(taxis); // Guardar en formato correcto
        }

        console.log("Taxis cargados desde el archivo:", taxis);
        return taxis;
    } catch (error) {
        console.error("Error al cargar taxis.json:", error);
        return {};
    }
}

// Función para guardar los datos en el archivo JSON
function saveTaxisToFile(taxis) {
    try {
        fs.writeFileSync(taxisFilePath, JSON.stringify(taxis, null, 4), "utf8");
        console.log("Taxis guardados correctamente:", taxis);
    } catch (error) {
        console.error("Error al guardar datos en taxis.json:", error);
    }
}

// Registrar un taxi
app.post("/register", (req, res) => {
    // Cargar los taxis existentes
    const taxis = loadTaxis();
    const { taxi_id } = req.body;

    if (!taxi_id) {
        return res.status(400).json({ error: "Faltan datos obligatorios" });
    }

    if (taxis[taxi_id]) {
        return res.status(409).json({ error: "El taxi ya está registrado" });
    }

    // Agregar el bloque completo de información
    taxis[taxi_id] = {
        id: taxi_id,
        status: "OK",
        color: "RED",
        position: [1, 1],
        customer_assigned: "x",
        picked_off: 0,
        authenticated: 0,
        token: ""
    };

    saveTaxisToFile(taxis); // Guardar los cambios
    console.log(`Taxi ${taxi_id} registrado con éxito.`);
    return res.status(201).json({ message: "Taxi registrado con éxito" });
});

// Dar de baja un taxi
app.delete("/deregister/:id", (req, res) => {
    const taxis = loadTaxis();
    const { id } = req.params;

    if (!taxis[id]) {
        return res.status(404).json({ error: "El taxi no está registrado" });
    }

    delete taxis[id]; // Elimina el taxi
    saveTaxisToFile(taxis); // Guardar los cambios
    console.log(`Taxi ${id} eliminado con éxito.`);
    return res.status(200).json({ message: "Taxi eliminado con éxito" });
});

// Consultar estado de un taxi
app.get("/status/:id", (req, res) => {
    const taxis = loadTaxis();
    const { id } = req.params;

    if (!taxis[id]) {
        return res.status(404).json({ error: "El taxi no está registrado" });
    }

    return res.status(200).json(taxis[id]);
});

// Listar todos los taxis
app.get("/taxis", (req, res) => {
    const taxis = loadTaxis();
    return res.status(200).json(Object.values(taxis));
});

// Ruta raíz
app.get('/', (req, res) => {
    res.send("Conexión haciendo uso de HTTPS mediante certificado autofirmado.");
});

// Arrancar el servidor
https
    .createServer(
        {
            key: fs.readFileSync("certServ.key"),
            cert: fs.readFileSync("certServ.pem"),
        },
        app
    )
    .listen(port, () => {
        console.log("HTTPS Registry API listening on port " + port);
    });
