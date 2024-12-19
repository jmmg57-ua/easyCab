const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const axios = require("axios");
const cors = require("cors");

// Configuración inicial
const port = 11000;
const app = express();
const taxisFilePath = "./data/taxis.json"; // Ruta del archivo JSON
const customersFilePath = "./data/customers.json";
const auditFilePath = "./data/audit.json";

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

app.use(cors());
// Función para cargar los datos del archivo JSON
function loadCustomers() {
    try {
        const data = fs.readFileSync(customersFilePath, "utf8");
        let customers = JSON.parse(data);

        // Convertir lista a objeto si es necesario
        if (Array.isArray(customers)) {
            const customersDict = {};
            customers.forEach(customer => {
                if (customer && customer.id !== undefined) {
                    customersDict[customer.id] = customer;
                }
            });
            customers = customersDict;
        }

        console.log("Taxis cargados desde el archivo:", customers);
        return customers;
    } catch (error) {
        console.error("Error al cargar taxis.json:", error);
        return {};
    }
}

// Función para cargar los registros del archivo audit.json
function loadLogs() {
    try {
        const data = fs.readFileSync(auditFilePath, "utf8");
        const logs = data
            .split("\n") // Dividir por líneas
            .filter(line => line.trim() !== "") // Eliminar líneas vacías
            .map(line => JSON.parse(line)); // Parsear cada línea como JSON

        console.log("Logs cargados desde el archivo:", logs);
        return logs;
    } catch (error) {
        console.error("Error al cargar audit.json:", error);
        return [];
    }
}

function loadMapConfig() {
    try {
        const data = fs.readFileSync('./data/map_config.json', "utf8");
        return JSON.parse(data);
    } catch (error) {
        console.error("Error al cargar map_config.json:", error);
        return {};
    }
}

// Endpoint para listar los registros de auditoría
app.get("/logs", (req, res) => {
    const logs = loadLogs();
    return res.status(200).json(logs);
});

// Listar todos los taxis
app.get("/taxis", (req, res) => {
    const taxis = loadTaxis();
    return res.status(200).json(Object.values(taxis));
});

// Listar todos los taxis
app.get("/customers", (req, res) => {
    const customers = loadCustomers();
    return res.status(200).json(Object.values(customers));
});

app.get("/locations", (req, res) => {
    const locations = loadMapConfig();
    return res.status(200).json(locations);
});

// Ruta raíz
app.get('/', (req, res) => {
    res.send("Conexión establecida con api_central.");
});

// Ejecutar la aplicacion
app.listen(port, () => {
    console.log(`Ejecutando la aplicación API REST de SD en el puerto
   ${port}`);
});