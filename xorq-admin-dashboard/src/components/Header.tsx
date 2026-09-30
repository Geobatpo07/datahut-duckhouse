import { Link } from "react-router-dom";

export default function Header() {
  return (
    <header
      style={{
        display: "flex",
        gap: 16,
        alignItems: "center",
        padding: "12px 20px",
        borderBottom: "1px solid #ddd",
        background: "#fafafa",
      }}
    >
      <strong>DataHut-DuckHouse — Xorq Admin</strong>
      <nav style={{ display: "flex", gap: 12 }}>
        <Link to="/">Tenants</Link>
        <Link to="/catalog-lookup">Catalog lookup</Link>
      </nav>
    </header>
  );
}
