import os
from xorq_config import get_flight_server

if __name__ == "__main__":
    port = os.getenv("FLIGHT_SERVER_PORT", "8815")
    print(f"Démarrage du serveur Xorq avec backend hybride sur le port {port}...")

    server = get_flight_server()
    server.serve()
