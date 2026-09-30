// @ts-ignore
import React, { useState } from "react";
import { catalogToTenant } from "../api";

export default function CatalogLookup() {
  const [catalog, setCatalog] = useState("");
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const lookup = () => {
    setError(null);
    setResult(null);
    catalogToTenant(catalog)
      .then(setResult)
      .catch((err: any) => setError(String(err)));
  };

  return (
    <div style={{ padding: 20 }}>
      <h2>Catalog Lookup</h2>
      <input
        value={catalog}
        onChange={(e) => setCatalog(e.target.value)}
        placeholder="catalog name"
      />
      <button onClick={lookup} disabled={!catalog}>
        Lookup
      </button>
      {error && <p style={{ color: "red" }}>{error}</p>}
      {result && <pre>{JSON.stringify(result, null, 2)}</pre>}
    </div>
  );
}
