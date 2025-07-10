import axios from "axios";

const API_BASE = process.env.REACT_APP_XORQ_API_URL || "http://localhost:9980";

const api = axios.create({
  baseURL: API_BASE,
  timeout: 5000,
});

// Tenants
export const getTenants = async () => (await api.get("/tenants/list")).data;
export const getTenantInfo = async (id: string) => (await api.get(`/tenants/info/${id}`)).data;
export const getTenantMetrics = async (id: string) => (await api.get(`/tenants/${id}/metrics`)).data;
export const getTenantBilling = async (id: string) => (await api.get(`/tenants/${id}/billing`)).data;
export const getAllMetadata = async () => (await api.get("/tenants/all_metadata")).data;

// Catalogs
export const catalogToTenant = async (catalog: string) => (await api.get(`/catalogs/tenant/${catalog}`)).data;
