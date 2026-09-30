import logging
import os

from .xorq_config import get_flight_server

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    port = os.getenv("FLIGHT_SERVER_PORT", "8815")
    logger.info("Demarrage du serveur Xorq (backend hybride) sur le port %s", port)

    server = get_flight_server()
    server.serve(block=True)
