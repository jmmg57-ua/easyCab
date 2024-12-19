import React, { useState, useEffect } from 'react';
import styled from 'styled-components';

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

const CTCStatus = ({ weather }) => {
  if (!weather || !weather.city || !weather.status) {
    return (
      <StatusContainer status="ERROR">
        <StatusHeader status="ERROR">
          Estado del Tráfico: Error
        </StatusHeader>
        <StatusInfo>
          <InfoItem>
            <Label>Ciudad:</Label>
            N/A
          </InfoItem>
          <InfoItem>
            <Label>Temperatura:</Label>
            N/A
          </InfoItem>
        </StatusInfo>
      </StatusContainer>
    );
  }

  return (
    <StatusContainer status={weather.status}>
      <StatusHeader status={weather.status}>
        Estado del Tráfico: {weather.status}
      </StatusHeader>
      <StatusInfo>
        <InfoItem>
          <Label>Ciudad:</Label>
          {weather.city}
        </InfoItem>
        <InfoItem>
          <Label>Temperatura:</Label>
          {typeof weather.temperature === 'number'
            ? `${weather.temperature.toFixed(1)}°C`
            : weather.temperature}
        </InfoItem>
      </StatusInfo>
    </StatusContainer>
  );
};
export default CTCStatus;