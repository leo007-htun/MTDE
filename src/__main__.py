"""Allows  python -m src  to launch the engine."""
from src.main import main
import asyncio

asyncio.run(main())
