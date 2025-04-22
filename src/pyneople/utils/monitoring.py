import asyncio
from pyneople.config.config import Settings

async def count_requests_per_second():
    while True:
        await asyncio.sleep(1)
        print(f"초당 요청 수: {Settings.request_count}", flush=True)
        Settings.request_count = 0