FROM python:3.11-slim

WORKDIR /app

# Copier les fichiers de dépendances (depuis la racine si contexte = ".")
COPY pyproject.toml poetry.lock ./

# Installer Poetry et les dépendances du projet
RUN pip install --upgrade pip \
    && pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root

# Copier le code source (du dossier flight_server uniquement)
COPY . .

# Lancer l'application (exécutée comme module pour permettre les imports relatifs)
CMD ["poetry", "run", "python", "-m", "app"]
