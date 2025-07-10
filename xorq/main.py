# XORQ FastAPI Docker Entrypoint
# Use this script as the entrypoint for the xorq orchestration API service

import uvicorn
from xorq.api import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9980, log_level="info")

