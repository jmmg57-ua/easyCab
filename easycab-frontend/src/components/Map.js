import React from 'react';
import styled from 'styled-components';

const MapGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(20, 30px);
  grid-template-rows: repeat(20, 30px);
  gap: 1px;
  background-color: #eee;
  padding: 10px;
  border: 2px solid #333;
`;

const Cell = styled.div`
  width: 30px;
  height: 30px;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: ${props => props.color || 'white'};
  border: 1px solid #ccc;
  font-size: 12px;
  font-weight: bold;
`;

const Map = ({ taxis, customers, locations }) => {
  const grid = Array(20).fill().map(() => Array(20).fill(null));

  // Colocar localizaciones
  Object.entries(locations).forEach(([id, loc]) => {
    const [x, y] = loc.position;
    grid[y-1][x-1] = { type: 'location', id, color: 'lightblue' };
  });

  // Colocar taxis
  taxis.forEach(taxi => {
    const [x, y] = taxi.position;
    // Solo mostrar el cliente junto al taxi si picked_off es 1
    const customer = (taxi.customer_assigned !== 'x' && taxi.picked_off === 1) ? taxi.customer_assigned : '';
    grid[y-1][x-1] = {
      type: 'taxi',
      id: `${taxi.id}${customer}`,
      color: taxi.color.toLowerCase()
    };
  });

  // Colocar clientes que no han sido recogidos
  customers.forEach(customer => {
    // Solo mostrar clientes que no están asignados o que están asignados pero no recogidos
    const taxi = taxis.find(t => t.customer_assigned === customer.id);
    if (!taxi || (taxi && taxi.picked_off === 0)) {
      const [x, y] = customer.position;
      grid[y-1][x-1] = { type: 'customer', id: customer.id, color: 'yellow' };
    }
  });

  return (
    <MapGrid>
      {grid.map((row, i) => 
        row.map((cell, j) => (
          <Cell 
            key={`${i}-${j}`}
            color={cell?.color}
          >
            {cell?.id || ''}
          </Cell>
        ))
      )}
    </MapGrid>
  );
};

export default Map;