"""
XORQ Core Orchestration API and Registry (foundation for multi-tenant SaaS)
"""
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
from threading import Lock

logger = logging.getLogger("xorq.core")

class TenantMetadata:
    """Core metadata and metric log for a SaaS tenant."""
    def __init__(self, tenant_id: str, config: Dict[str, Any]):
        self.tenant_id = tenant_id
        self.config = config
        self.datasets = []   # List of dataset dicts
        self.catalogs = []   # List of Trino/Hive catalog names
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.metrics = []    # List of {query, ts, engine, row_count, ...}
        self.billing = []    # List of billing events ({amount, type, ts, ...})

class XORQOrchestrator:
    """Central multi-tenant registry, orchestrator and service discovery for the data platform."""
    def __init__(self, storage_path: str = "xorq/metadata.db"):
        self.tenants: Dict[str, TenantMetadata] = {}
        self.catalog_index: Dict[str, str] = {}  # mapping catalog -> tenant
        self.lock = Lock()
        self.storage_path = Path(storage_path)
        self._load_metadata()

    def _load_metadata(self):
        # (Optional) Load from persistent storage/file if platform requires it
        # For now, keep in-memory for first implementation
        pass

    def register_tenant(self, tenant_id: str, config: Dict[str, Any]):
        with self.lock:
            if tenant_id not in self.tenants:
                self.tenants[tenant_id] = TenantMetadata(tenant_id, config)
                logger.info(f"Registered new tenant: {tenant_id}")
            else:
                logger.warning(f"Tenant {tenant_id} already exists—updating config.")
                self.tenants[tenant_id].config.update(config)
                self.tenants[tenant_id].updated_at = datetime.utcnow()

    def add_dataset(self, tenant_id: str, dataset: Dict[str, Any]):
        with self.lock:
            if tenant_id in self.tenants:
                self.tenants[tenant_id].datasets.append(dataset)
                logger.info(f"Registered dataset for tenant {tenant_id}: {dataset.get('name')}")

    def add_catalog(self, tenant_id: str, catalog_name: str):
        with self.lock:
            self.catalog_index[catalog_name] = tenant_id
            if tenant_id in self.tenants and catalog_name not in self.tenants[tenant_id].catalogs:
                self.tenants[tenant_id].catalogs.append(catalog_name)
            logger.info(f"Catalog {catalog_name} linked to tenant {tenant_id}")

    def list_tenants(self) -> List[str]:
        return list(self.tenants.keys())

    def find_tenant_by_catalog(self, catalog_name: str) -> Optional[str]:
        return self.catalog_index.get(catalog_name)

    def get_tenant_info(self, tenant_id: str) -> Optional[TenantMetadata]:
        return self.tenants.get(tenant_id)

    def all_metadata(self) -> Dict[str, TenantMetadata]:
        return self.tenants

    def track_query_metric(self, tenant_id: str, query: str, meta: Dict[str, Any]):
        logger.info(f"Tenant {tenant_id} ran query {query[:50]}... metrics: {meta}")
        with self.lock:
            tenant = self.tenants.get(tenant_id)
            if tenant:
                metric = {
                    "query": query[:200],
                    "meta": meta,
                    "engine": meta.get("engine"),
                    "table": meta.get("table"),
                    "ts": datetime.utcnow(),
                    "rows": meta.get("rows"),
                    "latency": meta.get("latency", 0),
                }
                tenant.metrics.append(metric)
                # If this is a "billable" query, log a billing event (basic example)
                if meta.get("bill", False):
                    usage = max(1, int(meta.get("rows", 0)) // 1000)  # Bill per 1k rows, as ex.
                    bill_evt = {"ts": metric["ts"], "amount": usage, "type": meta.get("engine", "query")}
                    tenant.billing.append(bill_evt)
                    logger.info(f"Bill event for tenant {tenant_id}: {bill_evt}")

# Singleton pattern
xorq_registry = XORQOrchestrator()

