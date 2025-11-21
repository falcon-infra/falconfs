import asyncio
import threading
import time
import _pyfalconfs_internal

async def fetch(offset):
    try:
        buffer = bytearray(b"hello")
        result = await _pyfalconfs_internal.AsyncGet("test", buffer, 0, offset)
        print(f"[{offset}] -> {result}")
    except Exception as e:
        print(f"[{offset}] ERROR: {e}")

def run_fetches_forever(loop):
    offset = 0
    while True:
        coro = fetch(offset)
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        # 可以异步等待结果（或略过）
        try:
            fut.result()  # 可注释掉：不阻塞
        except Exception as e:
            print("Error in fetch:", e)
        offset += 1
        time.sleep(1)  # 控制节奏，避免过快

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    t_loop = threading.Thread(target=loop.run_forever, daemon=True)
    t_loop.start()

    try:
        run_fetches_forever(loop)
    except KeyboardInterrupt:
        print("Stopping...")

    loop.call_soon_threadsafe(loop.stop)
    t_loop.join()

