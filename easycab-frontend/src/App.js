import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { api } from './services/api';
import Map from './components/Map';
import TaxiTable from './components/TaxiTable';
import CustomerTable from './components/Customertable';
import CTCStatus from './components/CTCStatus';
import ErrorLogs from './components/ErrorLogs';

const Container = styled.div`
  padding: 20px;
  max-width: 1400px;
  margin: 0 auto;
`;

const Header = styled.h1`
  text-align: center;
  color: #333;
  margin-bottom: 30px;
`;

const Grid = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  margin-bottom: 20px;
`;

const Section = styled.div`
  background-color: white;
  padding: 20px;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
`;

const SectionTitle = styled.h2`
  color: #444;
  margin-bottom: 15px;
  padding-bottom: 10px;
  border-bottom: 2px solid #eee;
`;

function App() {
  const [data, setData] = useState({
    taxis: [],
    customers: [],
    logs: [],
    locations: {}
  });

  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const newData = await api.getMapData();
        setData(newData);
        setError(null);
      } catch (error) {
        console.error('Error fetching data:', error);
        setError('Error al cargar los datos del sistema');
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1000);
    return () => clearInterval(interval);
  }, []);

  if (error) {
    return <div>Error: {error}</div>;
  }

  return (
    <Container>
      <Header>EasyCab Dashboard</Header>
      <Grid>
        <Section>
          <SectionTitle>Mapa del Sistema</SectionTitle>
          <Map 
            taxis={data.taxis}
            customers={data.customers}
            locations={data.locations}
          />
          <CTCStatus />
        </Section>
        <Section>
          <SectionTitle>Estado de Taxis</SectionTitle>
          <TaxiTable taxis={data.taxis} />
          <SectionTitle>Estado de Clientes</SectionTitle>
          <CustomerTable customers={data.customers} />
        </Section>
      </Grid>
      <Section>
        <SectionTitle>Registro de Eventos</SectionTitle>
        <ErrorLogs logs={data.logs} />
      </Section>
    </Container>
  );
}

export default App;