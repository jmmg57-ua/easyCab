const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");

// Configuración inicial
const app = express();
const port = 10000; // Puerto parametrizable
const taxisFilePath = "/data/taxis.json"; // Ruta del archivo JSON

// Verificar si el archivo de datos existe; si no, crearlo
if (!fs.existsSync(taxisFilePath)) {
    fs.mkdirSync(path.dirname(taxisFilePath), { recursive: true }); // Crear el directorio si no existe
    fs.writeFileSync(taxisFilePath, JSON.stringify({}, null, 4), "utf8"); // Crear un archivo vacío
}

// Cargar los datos del archivo JSON
let taxis = {};
try {
    const data = fs.readFileSync(taxisFilePath, "utf8");
    taxis = JSON.parse(data);
} catch (error) {
    console.error("Error al cargar taxis.json:", error);
    taxis = {}; // Usar un objeto vacío en caso de error
}

// Middleware para procesar JSON
app.use(bodyParser.json());

// Función para guardar los datos en el archivo JSON
function saveTaxisToFile() {
    try {
        fs.writeFileSync(taxisFilePath, JSON.stringify(taxis, null, 4), "utf8");
    } catch (error) {
        console.error("Error al guardar datos en taxis.json:", error);
    }
}

// Rutas del API REST

// Registrar un taxi
app.post("/register", (req, res) => {
    const { taxi_id, color, position } = req.body;

    if (!taxi_id || !color || !position) {
        return res.status(400).json({ error: "Faltan datos obligatorios" });
    }

    if (taxis[taxi_id]) {
        return res.status(409).json({ error: "El taxi ya está registrado" });
    }

    taxis[taxi_id] = { taxi_id, color, position, status: "FREE" };
    saveTaxisToFile(); // Guardar los cambios en el archivo
    console.log(`Taxi ${taxi_id} registrado.`);
    return res.status(201).json({ message: "Taxi registrado con éxito" });
});

// Dar de baja un taxi
app.delete("/deregister/:id", (req, res) => {
    const { id } = req.params;

    if (!taxis[id]) {
        return res.status(404).json({ error: "El taxi no está registrado" });
    }

    delete taxis[id];
    saveTaxisToFile(); // Guardar los cambios en el archivo
    console.log(`Taxi ${id} eliminado.`);
    return res.status(200).json({ message: "Taxi eliminado con éxito" });
});

// Consultar estado de un taxi
app.get("/status/:id", (req, res) => {
    const { id } = req.params;

    if (!taxis[id]) {
        return res.status(404).json({ error: "El taxi no está registrado" });
    }

    return res.status(200).json(taxis[id]);
});

// Listar todos los taxis
app.get("/taxis", (req, res) => {
    return res.status(200).json(Object.values(taxis));
});

// Arrancar el servidor
app.listen(port, () => {
    console.log(`Servidor Registry ejecutándose en el puerto ${port}`);
});
