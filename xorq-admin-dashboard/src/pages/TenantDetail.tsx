// @ts-ignore
import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { getTenantInfo, getTenantMetrics, getTenantBilling } from "../api";

export default function TenantDetail() {
  const { tenantId } = useParams();
  const [info, setInfo] = useState<any>(null);
  const [metrics, setMetrics] = useState<any[]>([]);
  const [billing, setBilling] = useState<any>(null);

  useEffect(() => {
    if (tenantId) {
      getTenantInfo(tenantId).then(setInfo);
      getTenantMetrics(tenantId).then(setMetrics);
      getTenantBilling(tenantId).then(setBilling);
    }
  }, [tenantId]);

  if (!tenantId) return null;

  return (
    <div style={{ padding: 20 }}>
      <h2>Tenant: {tenantId}</h2>
      <h4>Metadata</h4>
      <pre>{JSON.stringify(info, null, 2)}</pre>
      <h4>Metrics/Query Log (last {metrics.length})</h4>
      <pre>{JSON.stringify(metrics, null, 2)}</pre>
      <h4>Billing</h4>
      <pre>{JSON.stringify(billing, null, 2)}</pre>
    </div>
  );
}