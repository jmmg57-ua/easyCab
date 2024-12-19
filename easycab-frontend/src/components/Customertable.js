import React from 'react';
import styled from 'styled-components';

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin: 10px 0;
`;

const Th = styled.th`
  background-color: #f4f4f4;
  padding: 8px;
  border: 1px solid #ddd;
`;

const Td = styled.td`
  padding: 8px;
  border: 1px solid #ddd;
  text-align: center;
  background-color: ${props => {
    switch (props.status) {
      case 'UNATTENDED': return '#ffebee';
      case 'WAITING': return '#fff3e0';
      case 'TRANSIT': return '#e8f5e9';
      case 'SERVICED': return '#e3f2fd';
      default: return 'white';
    }
  }};
`;

const CustomerTable = ({ customers }) => (
  <Table>
    <thead>
      <tr>
        <Th>ID</Th>
        <Th>Estado</Th>
        <Th>Posición</Th>
        <Th>Destino</Th>
        <Th>Taxi Asignado</Th>
        <Th>Recogido</Th>
      </tr>
    </thead>
    <tbody>
      {customers.map(customer => (
        <tr key={customer.id}>
          <Td status={customer.status}>{customer.id}</Td>
          <Td status={customer.status}>{customer.status}</Td>
          <Td status={customer.status}>[{customer.position.join(', ')}]</Td>
          <Td status={customer.status}>{customer.destination}</Td>
          <Td status={customer.status}>
            {customer.taxi_assigned === 0 ? '-' : customer.taxi_assigned}
          </Td>
          <Td status={customer.status}>
            {customer.picked_off ? 'Sí' : 'No'}
          </Td>
        </tr>
      ))}
    </tbody>
  </Table>
);

export default CustomerTable;