# XORQ Orchestration Service Dockerfile
FROM python:3.11-slim

WORKDIR /xorq
COPY xorq/ ./xorq/

RUN pip install --no-cache-dir fastapi uvicorn pydantic

# Add other project dependencies as needed...

EXPOSE 9980
CMD ["python", "-m", "xorq.main"]

