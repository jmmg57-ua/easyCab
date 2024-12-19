import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import axios from 'axios';

const StatusContainer = styled.div`
  margin: 20px 0;
  padding: 15px;
  border-radius: 8px;
  background-color: ${props => props.status === 'OK' ? '#e8f5e9' : '#ffebee'};
  border: 1px solid ${props => props.status === 'OK' ? '#66bb6a' : '#ef5350'};
`;

const StatusHeader = styled.h3`
  margin: 0 0 10px 0;
  color: ${props => props.status === 'OK' ? '#2e7d32' : '#c62828'};
`;

const StatusInfo = styled.div`
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 10px;
`;

const InfoItem = styled.div`
  background-color: rgba(255, 255, 255, 0.7);
  padding: 8px;
  border-radius: 4px;
`;

const Label = styled.span`
  font-weight: bold;
  margin-right: 5px;
`;

const CTCStatus = () => {
  const [status, setStatus] = useState({
    status: 'Loading...',
    city: 'Loading...',
    temperature: 'Loading...'
  });

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const response = await axios.get('http://localhost:5000/api/traffic-status');
        setStatus(response.data);
      } catch (error) {
        console.error('Error fetching CTC status:', error);
        setStatus({
          status: 'ERROR',
          city: 'Error de conexión',
          temperature: 'N/A'
        });
      }
    };

    fetchStatus();
    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <StatusContainer status={status.status}>
      <StatusHeader status={status.status}>
        Estado del Tráfico: {status.status}
      </StatusHeader>
      <StatusInfo>
        <InfoItem>
          <Label>Ciudad:</Label>
          {status.city}
        </InfoItem>
        <InfoItem>
          <Label>Temperatura:</Label>
          {typeof status.temperature === 'number' 
            ? `${status.temperature.toFixed(1)}°C`
            : status.temperature}
        </InfoItem>
      </StatusInfo>
    </StatusContainer>
  );
};

export default CTCStatus;