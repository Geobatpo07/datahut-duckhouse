"""
Connexion cliente au serveur Flight de DataHut-DuckHouse.

Utilise directement `xorq.flight.backend.Backend` plutôt que
`xorq.flight.connect()` : cette dernière construit un `FlightUrl`, qui lie un
socket local à la construction (pensé pour le côté serveur, où on choisit un
port libre) — un comportement qu'on ne veut pas côté client, qui se contente
de se connecter à un host/port déjà choisi.

Limitation connue : `Backend.do_connect` ne semble pas exposer de timeout de
connexion dans la version installée de xorq (0.2.4) — si le serveur Flight
n'est pas joignable, l'appel peut bloquer indéfiniment plutôt que d'échouer
rapidement. À surveiller si ça devient gênant en usage réel ; pas résolu ici.
"""
import os

from xorq.flight.backend import Backend as FlightBackend


def get_connection() -> FlightBackend:
    """Se connecte au serveur Flight configuré via l'environnement.

    Variables d'environnement : `FLIGHT_SERVER_HOST` (défaut `localhost`),
    `FLIGHT_SERVER_PORT` (défaut `8815`) — mêmes conventions que
    `flight_server/app/utils.py`.
    """
    host = os.getenv("FLIGHT_SERVER_HOST", "localhost")
    port = int(os.getenv("FLIGHT_SERVER_PORT", "8815"))

    backend = FlightBackend()
    backend.do_connect(host=host, port=port)
    return backend
