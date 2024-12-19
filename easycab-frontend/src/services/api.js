import axios from 'axios';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:11000';

export const api = {
  getMapData: async () => {
    try {
      const [taxis, customers, logs, locations] = await Promise.all([
        axios.get(`${API_URL}/taxis`),
        axios.get(`${API_URL}/customers`),
        axios.get(`${API_URL}/logs`),
        axios.get(`${API_URL}/locations`)
      ]);
      
      return {
        taxis: taxis.data,
        customers: customers.data,
        logs: logs.data,
        locations: locations.data
      };
    } catch (error) {
      console.error('Error fetching data:', error);
      throw error;
    }
  }
};