# Singapore Weather Data Collector

A robust, high-performance weather data collection system that fetches real-time air temperature and relative humidity data from Singapore's government APIs. The system features intelligent gap detection, concurrent processing, automatic retry mechanisms, and comprehensive progress tracking.

## Features

- **Multi-source Data Collection**: Fetches air temperature and relative humidity data from Singapore's official APIs
- **Intelligent Gap Detection**: Automatically identifies and fills missing data gaps with minute-level precision
- **High-Performance Concurrent Processing**: Processes multiple days and data types in parallel with configurable concurrency limits
- **Robust Error Handling**: Exponential backoff retry logic with rate limiting protection
- **Smart Pagination**: Handles API pagination efficiently with semaphore-controlled concurrent requests
- **Progress Tracking**: JSON-based progress tracking with detailed completeness analysis
- **Flexible Query Strategy**: Automatically chooses between date-only and time-specific queries based on gap size
- **Data Validation**: Built-in data quality checks and duplicate removal
- **Null Handling**: Intelligent handling of genuinely missing API data with null record creation

## Architecture

### Core Components

- **`main.py`**: Entry point for the data collection process
- **`models/`**: Data models and configuration
  - `config.py`: System configuration and API endpoints
  - `time_range.py`: Time range data structures
  - `progress_state.py`: Progress tracking models
- **`utils/`**: Core utilities
  - `data_collector.py`: Main collection orchestrator with multiprocessing
  - `gap_analyzer.py`: Gap detection and completeness analysis
  - `http_client.py`: HTTP client with retry logic
  - `progress_tracker.py`: Progress state management
  - `logger.py`: Centralized logging

### Data Flow

1. **Gap Analysis**: System identifies incomplete dates and data gaps
2. **Concurrent Collection**: Multiple dates processed in parallel (configurable limit)
3. **Pagination Handling**: Each date/type combination handles paginated API responses
4. **Data Processing**: Raw API data converted to structured CSV format using Polars
5. **Progress Updates**: Real-time tracking of collection status and completeness

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

```bash
# Clone the repository
git clone https://github.com/TinkerThings-Global/singapore-weather-collector.git
cd singapore-weather-collector

# Install dependencies using uv
uv sync
```

### Basic Usage

```bash
# Run data collection
uv run main.py
```

### Configuration

Edit `models/config.py` to customize:

```python
@dataclass
class Config:
    DATA_DIR: Path = Path("weather_data_improved")  # Output directory
    STATION_ID: str = "S50"                         # Weather station ID
    FIRST_DATE: date = date(2025, 6, 30)           # Start date for collection
    MAX_CONCURRENT_DAYS: int = 4                    # Parallel processing limit
    MAX_CONCURRENT_REQUESTS: int = 5                # API request concurrency
    MAX_RETRIES: int = 5                           # Retry attempts per request
```

## API Sources

The system collects data from Singapore's official government APIs:

- **Air Temperature**: `https://api-open.data.gov.sg/v2/real-time/api/air-temperature`
- **Relative Humidity**: `https://api-open.data.gov.sg/v2/real-time/api/relative-humidity`

Station S50 provides minute-level readings (1440 records per day) in Singapore local time.

## Output Format

Data is saved as CSV files with the naming convention:

```bash
{data_type}_{station_id}_{date}.csv
```

**Example files:**

- `temp_S50_2025-07-18.csv`
- `rh_S50_2025-07-18.csv`

**CSV Structure:**

```csv
timestamp,temperature
2025-07-18T00:00:00.000000,28.7
2025-07-18T00:01:00.000000,28.7
...
```

## Advanced Features

### Gap Detection Algorithm

The system implements sophisticated gap detection:

1. **Timestamp Validation**: Ensures all timestamps are within the target date
2. **Minute-level Precision**: Detects gaps larger than 1 minute
3. **Boundary Checking**: Verifies data starts at 00:00 and ends at 23:59
4. **Duplicate Handling**: Removes duplicate timestamps, keeping the latest value
5. **Time-aware Processing**: For today's data, only checks up to current time minus 5-minute buffer

### Null Data Handling

The system intelligently handles genuinely missing API data:

1. **Gap vs Missing Distinction**: Differentiates between data collection gaps and API data unavailability
2. **Null Record Creation**: Creates null records for timestamps where API returns no data during gap-filling
3. **Prevents Infinite Retries**: Avoids repeatedly requesting data that doesn't exist at the API level
4. **Data Completeness**: Marks files as complete even with null values for missing periods

### Multi-Pass Collection Strategy

1. **Pass 1: Initial Collection** - Processes all incomplete dates
2. **Pass 2: Gap Filling** - Targets remaining gaps with specific time queries
3. **Pass 3: Final Validation** - Reports any remaining incomplete data

### Query Optimization

The system intelligently chooses query strategies:

- **Date-only queries** (`{"date": "2025-07-18"}`): For full day collection or large gaps (e6 hours)
- **Time-specific queries** (`{"date": "2025-07-18T14:30:00"}`): For small gap filling with client-side filtering

### Concurrency Control

- **Semaphore-based rate limiting**: Prevents API overload
- **Deadlock prevention**: Proper semaphore scoping for pagination operations
- **Graceful error handling**: Individual failures don't stop the entire process

## Progress Tracking

The system maintains detailed progress in `fetch_progress.json`:

```json
{
  "2025-07-18": {
    "temp": {
      "status": "complete",
      "total_records": 1440,
      "expected_records": 1440,
      "timestamp_gaps": null,
      "last_updated": "2025-07-19T21:30:00"
    }
  }
}
```

### Status Checking

```bash
# Check completeness for specific date
uv run -c "
from models import Config
from utils.gap_analyzer import DataGapAnalyzer
from datetime import date

analyzer = DataGapAnalyzer(Config())
summary = analyzer.get_completeness_summary(date(2025, 7, 18))
print(summary)
"
```

### Rebuilding Progress

If progress tracking becomes inconsistent:

```bash
uv run rebuild_progress.py
```

## Monitoring and Logs

- **Real-time logging**: `weather_data.log` contains detailed operation logs
- **Progress visualization**: JSON progress file shows completion status
- **Error tracking**: Failed operations logged with context for debugging

### Log Monitoring

```bash
# Watch logs in real-time
tail -f weather_data.log

# Filter for specific events
tail -f weather_data.log | grep -E "(Fetched|ERROR|gaps)"
```

## Performance Characteristics

- **Typical throughput**: ~50-100 requests/minute (respecting rate limits)
- **Data volume**: ~2880 records/day (1440 temp + 1440 humidity)
- **Memory usage**: Efficient streaming with Polars DataFrames
- **Fault tolerance**: Automatic retry with exponential backoff

## Troubleshooting

### Common Issues

1. **Rate Limiting**: Reduce `MAX_CONCURRENT_REQUESTS` if getting 429 errors
2. **Memory Issues**: Reduce `MAX_CONCURRENT_DAYS` for lower memory usage
3. **Network Timeouts**: Increase retry delays in config
4. **Broken Pipe Errors**: Ensure multiprocessing tasks complete before executor shutdown

### Debug Mode

Enable detailed logging by checking log files:

```bash
# Recent activity
tail -50 weather_data.log

# Error analysis
grep -E "ERROR|Exception" weather_data.log
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with appropriate tests
4. Submit a pull request

## License

This project is designed for collecting publicly available Singapore government weather data. Ensure compliance with API terms of service and rate limiting requirements.

## Data Source Attribution

Weather data sourced from Singapore's government open data APIs:

- Data.gov.sg - Real-time Weather Data
