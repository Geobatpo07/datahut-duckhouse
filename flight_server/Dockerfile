FROM python:3.11-slim

WORKDIR /app

# Copie des dépendances et installation
COPY ../pyproject.toml ../poetry.lock ./
RUN pip install --upgrade pip \
    && pip install poetry \
    && poetry install --no-root

# Copie du code source
COPY . .

CMD ["poetry", "run", "python", "app.py"]
