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
`;

const TaxiTable = ({ taxis }) => (
  <Table>
    <thead>
      <tr>
        <Th>ID</Th>
        <Th>Estado</Th>
        <Th>Color</Th>
        <Th>Cliente</Th>
      </tr>
    </thead>
    <tbody>
      {taxis.map(taxi => (
        <tr key={taxi.id}>
          <Td>{taxi.id}</Td>
          <Td>{taxi.status}</Td>
          <Td>{taxi.color}</Td>
          <Td>{taxi.customer_assigned}</Td>
        </tr>
      ))}
    </tbody>
  </Table>
);

export default TaxiTable;