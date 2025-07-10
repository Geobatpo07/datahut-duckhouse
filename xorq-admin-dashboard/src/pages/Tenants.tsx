// @ts-ignore
import React, { useEffect, useState } from "react";
// @ts-ignore
import { getTenants, getTenantInfo } from "../api";
import { Link } from "react-router-dom";

export default function Tenants() {
  const [tenants, setTenants] = useState<string[]>([]);

  useEffect(() => {
    getTenants().then(setTenants);
  }, []);

  return (
    <div style={{ padding: 20 }}>
      <h2>Registered Tenants</h2>
      <ul>
        {tenants.map(tid => (
          <li key={tid}>
            <Link to={`/tenant/${tid}`}>{tid}</Link>
          </li>
        ))}
      </ul>
    </div>
  );
}