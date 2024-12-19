import React from 'react';
import styled from 'styled-components';

const LogContainer = styled.div`
  margin: 20px 0;
  border: 1px solid #ddd;
  border-radius: 8px;
  overflow: hidden;
`;

const LogHeader = styled.div`
  background-color: #f4f4f4;
  padding: 10px;
  border-bottom: 1px solid #ddd;
  font-weight: bold;
`;

const LogContent = styled.div`
  height: 300px;
  overflow-y: auto;
  background-color: #fafafa;
`;

const LogEntry = styled.div`
  padding: 8px;
  border-bottom: 1px solid #eee;
  font-family: monospace;
  font-size: 0.9em;
  
  &:nth-child(odd) {
    background-color: #f8f8f8;
  }
`;

const Timestamp = styled.span`
  color: #666;
  margin-right: 10px;
`;

const EventType = styled.span`
  font-weight: bold;
  color: ${props => {
    switch (props.type.toLowerCase()) {
      case 'error': return '#d32f2f';
      case 'warning': return '#f57c00';
      case 'info': return '#1976d2';
      default: return '#333';
    }
  }};
  margin-right: 10px;
`;

const ErrorLogs = ({ logs }) => {
  return (
    <LogContainer>
      <LogHeader>Registro de Eventos del Sistema</LogHeader>
      <LogContent>
        {logs.map((log, index) => (
          <LogEntry key={index}>
            <Timestamp>{log.timestamp}</Timestamp>
            <EventType type={log.event_type}>{log.event_type}</EventType>
            <span>{log.description}</span>
          </LogEntry>
        ))}
      </LogContent>
    </LogContainer>
  );
};

export default ErrorLogs;