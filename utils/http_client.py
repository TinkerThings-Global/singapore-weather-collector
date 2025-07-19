import asyncio
import httpx
from .logger import Logger
from models import Config
import random
from typing import Optional

logger = Logger(name="http_client", filename="weather_data.log")


class RetryableHTTPClient:
    """HTTP client with exponential backoff retry logic"""

    def __init__(self, config: Config):
        self.config = config

    async def get_with_retry(
        self, client: httpx.AsyncClient, url: str, params: dict
    ) -> Optional[dict]:
        """Make HTTP request with exponential backoff retry"""
        last_exception = None

        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.info(
                    f"Attempt {attempt + 1}/{self.config.MAX_RETRIES}: {url} with params {params}"
                )

                response = await client.get(url, params=params, timeout=30)
                    
                logger.info(f"Response {response.status_code} from {response.url}")

                if response.status_code == 429:
                    # Rate limit - calculate retry delay
                    retry_after = int(response.headers.get("Retry-After", 2**attempt))
                    jitter = random.uniform(0, retry_after * 0.1)
                    delay = min(retry_after + jitter, self.config.MAX_RETRY_DELAY)

                    logger.warning(f"Rate limited. Retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue

                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                logger.warning(f"params: {params}")
                last_exception = e
                if e.response.status_code == 429:
                    continue  # Already handled above
                elif e.response.status_code >= 500:
                    # Server error - retry with backoff
                    delay = min(
                        self.config.BASE_RETRY_DELAY * (2**attempt)
                        + random.uniform(0, 1),
                        self.config.MAX_RETRY_DELAY,
                    )
                    logger.warning(
                        f"Server error {e.response.status_code}. Retrying in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    # Client error - don't retry
                    logger.error(f"Client error {e.response.status_code}: {e}")
                    return None

            except Exception as e:
                last_exception = e
                delay = min(
                    self.config.BASE_RETRY_DELAY * (2**attempt) + random.uniform(0, 1),
                    self.config.MAX_RETRY_DELAY,
                )
                logger.warning(f"Request failed: {e}. Retrying in {delay:.1f}s")
                await asyncio.sleep(delay)

        logger.error(f"Max retries exceeded for {url}. Last error: {last_exception}")
        return None
