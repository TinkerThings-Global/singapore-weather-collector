"""
Main entry point for the weather data collection system.
Integrates all components: data collector, gap analyzer, progress tracker.
"""

import asyncio
from models import Config
from utils.data_collector import WeatherDataCollector
from utils.logger import Logger

# Initialize logger
logger = Logger(name="main", filename="weather_data.log")


async def main():
    """Main function"""
    config = Config()
    collector = WeatherDataCollector(config)

    try:
        await collector.run_multi_pass_collection()
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
