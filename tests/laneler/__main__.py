"""Allow running laneler as: uv run python -m tests.laneler"""

from .app import _free_port
import asyncio
import logging
import os
import uvicorn

from .app import app


def main():
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    host = os.environ.get("LANELER_HOST", "127.0.0.1")
    port = int(os.environ.get("LANELER_PORT", str(_free_port())))
    print(f"laneler starting on http://{host}:{port}")
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())


if __name__ == "__main__":
    main()
