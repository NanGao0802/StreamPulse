import axios from "axios";

const http = axios.create({
  baseURL: import.meta.env.VITE_API_BASE,
  timeout: 15000,
});

export default http;
