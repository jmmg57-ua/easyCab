class TaxiSystem {
    constructor() {
        this.mapSize = 20;
        this.map = [];
        this.locations = {};
        this.taxis = {};
        this.customers = {};
        this.initializeMap();
    }

    initializeMap() {
        const mapElement = document.getElementById('map');
        
        // Crear la cuadrícula del mapa
        for (let y = 0; y < this.mapSize; y++) {
            this.map[y] = [];
            for (let x = 0; x < this.mapSize; x++) {
                const cell = document.createElement('div');
                cell.className = 'cell';
                cell.id = `cell-${x}-${y}`;
                mapElement.appendChild(cell);
                this.map[y][x] = '';
            }
        }
    }

    async loadData() {
        try {
            // Cargar datos de los archivos JSON
            const [mapConfig, taxis, customers] = await Promise.all([
                fetch('data/map_config.json').then(res => res.json()),
                fetch('data/taxis.json').then(res => res.json()),
                fetch('data/customers.json').then(res => res.json())
            ]);

            this.locations = mapConfig;
            this.taxis = taxis;
            this.customers = customers;

            this.updateMap();
        } catch (error) {
            console.error('Error loading data:', error);
        }
    }

    updateMap() {
        // Limpiar el mapa
        this.clearMap();

        // Colocar localizaciones
        for (const [id, location] of Object.entries(this.locations)) {
            const [x, y] = location.position;
            this.updateCell(x-1, y-1, id, 'location');
        }

        // Colocar taxis
        for (const [id, taxi] of Object.entries(this.taxis)) {
            const [x, y] = taxi.position;
            const colorClass = taxi.color === 'GREEN' ? 'green' : '';
            
            if (taxi.customer_assigned !== 'x') {
                // Taxi con cliente
                this.updateCell(x-1, y-1, `${id}${taxi.customer_assigned}`, 'combined');
            } else {
                // Taxi solo
                this.updateCell(x-1, y-1, id, `taxi ${colorClass}`);
            }
        }

        // Colocar clientes
        for (const [id, customer] of Object.entries(this.customers)) {
            const [x, y] = customer.position;
            if (customer.taxi_assigned === 0) {
                // Cliente sin taxi
                this.updateCell(x-1, y-1, id, 'customer');
            }
            // Los clientes con taxi ya se colocaron en el bucle anterior
        }
    }

    clearMap() {
        for (let y = 0; y < this.mapSize; y++) {
            for (let x = 0; x < this.mapSize; x++) {
                const cell = document.getElementById(`cell-${x}-${y}`);
                cell.className = 'cell';
                cell.textContent = '';
            }
        }
    }

    updateCell(x, y, content, className) {
        const cell = document.getElementById(`cell-${x}-${y}`);
        if (cell) {
            cell.className = `cell ${className}`;
            cell.textContent = content;
        }
    }

    startUpdates() {
        // Actualizar el mapa cada segundo
        setInterval(() => this.loadData(), 1000);
    }
}

// Inicializar el sistema cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    const system = new TaxiSystem();
    system.loadData();
    system.startUpdates();
});