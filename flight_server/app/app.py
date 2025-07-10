import os
import logging
from typing import Optional, Iterator, Any

import pyarrow as pa
import pyarrow.flight as flight
from dotenv import load_dotenv
import requests
import json

# Import des utilitaires
from .utils import get_duckdb_connection, validate_environment
from .query_orchestrator import get_query_orchestrator
from flight_server.app.backends.iceberg_backend import write_arrow_to_iceberg

# XORQ Orchestration Service (HTTP Admin/Metadata API)
XORQ_API_URL = os.getenv("XORQ_API_URL", "http://xorq-service:9980")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class DuckHouseFlightServer(flight.FlightServerBase):
    """Arrow Flight server for DataHut-DuckHouse hybrid data stack."""
    
    def __init__(self, location: str = "grpc://0.0.0.0:8815") -> None:
        """Initialize the Flight server.
        
        Args:
            location: The server location (default: grpc://0.0.0.0:8815)
        """
        super().__init__(location)
        try:
            validate_environment()
            self.db = get_duckdb_connection()
            self.orchestrator = get_query_orchestrator()
            logger.info(f"Flight server initialized at {location}")
            logger.info("Query orchestrator initialized - routing between DuckDB and Trino")
            # Check XORQ is up (fail-fast)
            try:
                health = requests.get(f"{XORQ_API_URL}/tenants/list", timeout=2)
                logger.info(f"XORQ orchestration layer detected: {health.json()}")
            except Exception as e:
                logger.warning(f"XORQ orchestration layer unavailable at start: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize Flight server: {e}")
            raise

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.FlightStreamReader,
        writer: flight.FlightStreamWriter,
    ) -> None:
        """Handle incoming data upload requests."""
        try:
            table_name = descriptor.path[0].decode("utf-8")
            logger.info(f"Receiving data for table: {table_name}")

            arrow_table = reader.read_all()
            logger.info(f"Read {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")

            # Get metadata to determine target backend
            metadata = dict(context.metadata()) if context.metadata() else {}
            target = metadata.get("target", "auto").lower()
            
            # Determine appropriate backend based on data characteristics
            if target == "auto":
                # Use orchestrator to determine best backend
                if arrow_table.num_rows > 100_000:  # Large datasets go to Iceberg
                    target = "iceberg"
                    logger.info(f"Auto-routing large dataset ({arrow_table.num_rows} rows) to Iceberg")
                else:
                    target = "duckdb"
                    logger.info(f"Auto-routing small dataset ({arrow_table.num_rows} rows) to DuckDB")

            if target == "iceberg":
                logger.info("Writing to Iceberg (for large datasets and distributed queries)")
                write_arrow_to_iceberg(table_name, arrow_table)
            elif target == "duckdb":
                logger.info("Writing to DuckDB (for local, lightweight queries)")
                df = arrow_table.to_pandas()
                self.db.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            else:
                raise ValueError(f"Unsupported target: {target}. Use 'duckdb', 'iceberg', or 'auto'")

            logger.info(f"Table '{table_name}' written successfully to {target.upper()}")

            # XORQ: register/update tenant and dataset metadata for SaaS admin
            try:
                payload = {
                    "tenant_id": metadata.get("tenant_id", "default-tenant"),
                    "dataset": {"name": table_name, "target": target, "rows": arrow_table.num_rows, "columns": arrow_table.num_columns, "upload_time": str(datetime.utcnow())}
                }
                resp = requests.post(f"{XORQ_API_URL}/datasets/register", json=payload, timeout=5)
                if resp.status_code == 200:
                    logger.info(f"XORQ: Dataset {table_name} registered in metadata layer.")
                else:
                    logger.warning(f"XORQ: Failed to register dataset {table_name} (status {resp.status_code})")
            except Exception as e:
                logger.warning(f"XORQ: Error tracking dataset to orchestrator: {e}")

        except Exception as e:
            logger.error(f"Error processing data upload: {e}")
            raise flight.FlightServerError(f"Data upload failed: {e}")

    def list_flights(
        self,
        context: flight.ServerCallContext,
        criteria: Optional[bytes],
    ) -> Iterator[flight.FlightInfo]:
        """List available flights (datasets)."""
        # For now, return empty list
        # TODO: Implement proper flight listing from DuckDB and Iceberg
        return iter([])
    
    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        """Get information about a specific flight."""
        # TODO: Implement flight info retrieval
        raise flight.FlightUnavailableError("Flight info not implemented yet")
    
    def do_get(
        self,
        context: flight.ServerCallContext,
        ticket: flight.Ticket,
    ) -> flight.FlightDataStream:
        """Handle data retrieval requests."""
        # TODO: Implement data retrieval from DuckDB/Iceberg
        raise flight.FlightUnavailableError("Data retrieval not implemented yet")

def main() -> None:
    """Main entry point for the Flight server."""
    try:
        port = os.getenv('FLIGHT_SERVER_PORT', '8815')
        location = f"grpc://0.0.0.0:{port}"
        
        logger.info(f"Starting Arrow Flight server on {location}")
        server = DuckHouseFlightServer(location=location)
        
        logger.info("Server is ready to accept connections")
        server.serve()
        
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Server failed to start: {e}")
        raise
    finally:
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    main()
