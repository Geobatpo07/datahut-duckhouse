"""
XORQ API for orchestration control, tenant registration, and metadata/query endpoint exposure.
"""
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from xorq.core import xorq_registry

logger = logging.getLogger("xorq.api")

app = FastAPI(title="DataHut XORQ Orchestration API")

class TenantCreateRequest(BaseModel):
    tenant_id: str
    config: dict

class DatasetRegisterRequest(BaseModel):
    tenant_id: str
    dataset: dict

class QueryMetricRequest(BaseModel):
    tenant_id: str
    query: str
    meta: dict

@app.post("/tenants/register")
def register_tenant(data: TenantCreateRequest):
    xorq_registry.register_tenant(data.tenant_id, data.config)
    return {"status": "ok"}

@app.post("/datasets/register")
def register_dataset(data: DatasetRegisterRequest):
    xorq_registry.add_dataset(data.tenant_id, data.dataset)
    return {"status": "ok"}

@app.post("/catalogs/add")
def register_catalog(tenant_id: str, catalog: str):
    xorq_registry.add_catalog(tenant_id, catalog)
    return {"status": "ok"}

@app.get("/tenants/list")
def list_tenants():
    return xorq_registry.list_tenants()

@app.get("/tenants/info/{tenant_id}")
def tenant_info(tenant_id: str):
    meta = xorq_registry.get_tenant_info(tenant_id)
    if not meta:
        raise HTTPException(status_code=404, detail="No such tenant")
    return meta.config

@app.get("/tenants/all_metadata")
def all_metadata():
    summary = {k: v.config for k, v in xorq_registry.all_metadata().items()}
    return summary

@app.post("/track_query_metric")
def track_query_metric(data: QueryMetricRequest):
    xorq_registry.track_query_metric(data.tenant_id, data.query, data.meta)
    return {"status": "ok"}

@app.get("/tenants/{tenant_id}/metrics")
def tenant_metrics(tenant_id: str):
    tenant = xorq_registry.get_tenant_info(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="No such tenant")
    return tenant.metrics

@app.get("/tenants/{tenant_id}/billing")
def tenant_billing(tenant_id: str):
    tenant = xorq_registry.get_tenant_info(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="No such tenant")
    summary = {
        "total_bill_events": len(tenant.billing),
        "total_amount": sum(event["amount"] for event in tenant.billing),
        "events": tenant.billing,
    }
    return summary

@app.get("/catalogs/tenant/{catalog_name}")
def catalog_to_tenant(catalog_name: str):
    tid = xorq_registry.find_tenant_by_catalog(catalog_name)
    if not tid:
        raise HTTPException(status_code=404, detail="No tenant for this catalog")
    return {"tenant_id": tid}

