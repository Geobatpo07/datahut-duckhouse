import { BrowserRouter, Routes, Route } from "react-router-dom";
import Tenants from "./pages/Tenants";
import TenantDetail from "./pages/TenantDetail";
// @ts-ignore
import CatalogLookup from "./pages/CatalogLookup";
// @ts-ignore
import Header from "./components/Header";

export default function App() {
  return (
    <BrowserRouter>
      <Header />
      <Routes>
        <Route path="/" element={<Tenants />} />
        <Route path="/tenant/:tenantId" element={<TenantDetail />} />
        <Route path="/catalog-lookup" element={<CatalogLookup />} />
      </Routes>
    </BrowserRouter>
  );
}