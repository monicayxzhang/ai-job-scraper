# API Documentation - Job Agent MVP

> 📚 Comprehensive API reference for the Job Agent MVP system

This document provides detailed API documentation for all core modules and interfaces in the Job Agent MVP system.

## 📋 Table of Contents

- [Core Pipeline API](#-core-pipeline-api)
- [Crawler Registry API](#-crawler-registry-api)
- [Deduplication API](#-deduplication-api)
- [Information Extraction API](#-information-extraction-api)
- [Filtering System API](#-filtering-system-api)
- [Notion Writer API](#-notion-writer-api)
- [Configuration API](#-configuration-api)
- [Logger API](#-logger-api)
- [Data Types](#-data-types)

---

## 🚀 Core Pipeline API

### FilteredJobPipeline

The main pipeline class that orchestrates the entire job processing workflow.

#### Constructor

```python
FilteredJobPipeline(
    config: Optional[Dict[str, Any]] = None,
    skip_crawl: bool = False,
    data_file: Optional[str] = None,
    skip_notion_load: bool = False,
    notion_cache_file: Optional[str] = None,
    enable_filters: bool = True
)
```

**Parameters:**
- `config` - Configuration dictionary override
- `skip_crawl` - Skip crawling stage (use existing data)
- `data_file` - Path to existing data file
- `skip_notion_load` - Skip loading existing Notion data
- `notion_cache_file` - Path to Notion cache file
- `enable_filters` - Enable/disable filtering system

#### Methods

##### `async run_pipeline() -> Dict[str, Any]`

Executes the complete job processing pipeline.

**Returns:** Pipeline execution results with statistics

**Example:**
```python
pipeline = FilteredJobPipeline(enable_filters=True)
results = await pipeline.run_pipeline()
print(f"Processed {results['final_job_count']} jobs")
```

##### `step1_crawl_jobs() -> List[Dict[str, Any]]`

Executes the job crawling stage.

**Returns:** List of raw job data

##### `step2_deduplicate_and_filter_jobs(jobs: List[Dict]) -> List[Dict]`

Performs deduplication and basic filtering.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Deduplicated and filtered job list

##### `step3_extract_and_advanced_filter(jobs: List[Dict]) -> List[Dict]`

Performs information extraction and advanced filtering.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Enhanced job data with extraction results

##### `step4_write_to_notion_optimized(jobs: List[Dict]) -> Dict[str, Any]`

Writes processed jobs to Notion database.

**Parameters:**
- `jobs` - List of processed job dictionaries

**Returns:** Write operation results

---

## 🕷️ Crawler Registry API

### crawler_registry

Global registry for managing job crawlers.

#### Methods

##### `load_enabled_crawlers(config: Dict[str, Any]) -> List[Crawler]`

Loads and initializes enabled crawlers from configuration.

**Parameters:**
- `config` - Configuration dictionary

**Returns:** List of initialized crawler instances

**Example:**
```python
from src.crawler_registry import crawler_registry

config = {"crawlers": {"boss_playwright": {"enabled": True}}}
crawlers = crawler_registry.load_enabled_crawlers(config)
```

##### `register_crawler(name: str, crawler_class: type)`

Registers a new crawler type.

**Parameters:**
- `name` - Crawler identifier
- `crawler_class` - Crawler class implementation

##### `get_crawler(name: str) -> Optional[type]`

Retrieves a registered crawler class.

**Parameters:**
- `name` - Crawler identifier

**Returns:** Crawler class or None if not found

---

## 🧠 Deduplication API

### EnhancedJobDeduplicator

Main deduplication engine with multiple strategies.

#### Constructor

```python
EnhancedJobDeduplicator(
    llm_client: Optional[Any] = None,
    use_llm: bool = True
)
```

**Parameters:**
- `llm_client` - LLM client instance for semantic deduplication
- `use_llm` - Enable LLM-based semantic deduplication

#### Methods

##### `async deduplicate_jobs(jobs: List[Dict]) -> List[Dict]`

Performs intelligent job deduplication.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Deduplicated job list

**Example:**
```python
deduplicator = EnhancedJobDeduplicator(llm_client, use_llm=True)
unique_jobs = await deduplicator.deduplicate_jobs(raw_jobs)
```

##### `get_stats() -> Dict[str, Any]`

Returns deduplication statistics.

**Returns:** Statistics dictionary with processing metrics

### NotionJobDeduplicator

Specialized deduplicator for Notion database integration.

#### Constructor

```python
NotionJobDeduplicator(
    notion_token: str,
    database_id: str,
    llm_client: Optional[Any] = None
)
```

**Parameters:**
- `notion_token` - Notion integration token
- `database_id` - Target Notion database ID
- `llm_client` - LLM client for semantic comparison

#### Methods

##### `async deduplicate_with_notion(jobs: List[Dict]) -> List[Dict]`

Performs deduplication against existing Notion database entries.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Jobs not present in Notion database

---

## 🔍 Information Extraction API

### EnhancedNotionExtractor

LLM-powered information extraction system.

#### Constructor

```python
EnhancedNotionExtractor(
    llm_client: Any,
    config: Optional[Dict[str, Any]] = None
)
```

**Parameters:**
- `llm_client` - LLM client instance
- `config` - Extraction configuration

#### Methods

##### `async extract_for_notion_enhanced(job_data: Dict) -> Dict[str, Any]`

Extracts structured information from job data.

**Parameters:**
- `job_data` - Raw job data dictionary

**Returns:** Enhanced job data with extracted fields

**Example:**
```python
extractor = EnhancedNotionExtractor(llm_client)
enhanced_job = await extractor.extract_for_notion_enhanced(raw_job)
```

##### `batch_extract(jobs: List[Dict]) -> List[Dict]`

Performs batch extraction on multiple jobs.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** List of enhanced job dictionaries

---

## 🎯 Filtering System API

### UnifiedJobFilterManager

Comprehensive job filtering and scoring system.

#### Constructor

```python
UnifiedJobFilterManager(config: Optional[Dict] = None)
```

**Parameters:**
- `config` - Filter configuration dictionary

#### Methods

##### `apply_basic_filters(jobs: List[Dict]) -> Tuple[List[Dict], List[Dict]]`

Applies basic filtering rules (hard constraints).

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Tuple of (passed_jobs, rejected_jobs)

##### `apply_advanced_filters(jobs: List[Dict]) -> List[Dict]`

Applies advanced filtering with scoring.

**Parameters:**
- `jobs` - List of job dictionaries

**Returns:** Scored and ranked job list

##### `calculate_job_score(job: Dict) -> float`

Calculates comprehensive job score.

**Parameters:**
- `job` - Job dictionary

**Returns:** Job score (0-100)

**Example:**
```python
filter_manager = UnifiedJobFilterManager(config)
passed, rejected = filter_manager.apply_basic_filters(jobs)
scored_jobs = filter_manager.apply_advanced_filters(passed)
```

---

## 📝 Notion Writer API

### OptimizedNotionJobWriter

Efficient Notion database writer with batch operations.

#### Constructor

```python
OptimizedNotionJobWriter(
    notion_token: str,
    database_id: str,
    batch_size: int = 10
)
```

**Parameters:**
- `notion_token` - Notion integration token
- `database_id` - Target database ID
- `batch_size` - Batch size for write operations

#### Methods

##### `async batch_write_jobs(jobs: List[Dict]) -> Dict[str, Any]`

Writes jobs to Notion database in batches.

**Parameters:**
- `jobs` - List of processed job dictionaries

**Returns:** Write operation results

##### `check_database_schema() -> Dict[str, Any]`

Validates database schema compatibility.

**Returns:** Schema validation results

**Example:**
```python
writer = OptimizedNotionJobWriter(token, database_id)
schema_check = writer.check_database_schema()
if schema_check['valid']:
    results = await writer.batch_write_jobs(jobs)
```

---

## ⚙️ Configuration API

### Configuration Management

#### `load_config(config_path: Optional[str] = None) -> Dict[str, Any]`

Loads configuration from YAML file with validation.

**Parameters:**
- `config_path` - Path to configuration file (optional)

**Returns:** Validated configuration dictionary

#### `get_default_config() -> Dict[str, Any]`

Returns default configuration values.

**Returns:** Default configuration dictionary

#### `validate_config(config: Dict[str, Any]) -> Dict[str, Any]`

Validates and completes configuration.

**Parameters:**
- `config` - Configuration dictionary to validate

**Returns:** Validated and completed configuration

**Example:**
```python
from src.config import load_config, validate_config

config = load_config("custom_config.yaml")
validated_config = validate_config(config)
```

---

## 📊 Logger API

### Logger System

#### `get_logger() -> Logger`

Gets the application logger instance.

**Returns:** Configured logger instance

#### `init_logger(level: LogLevel = LogLevel.INFO) -> Logger`

Initializes the logging system.

**Parameters:**
- `level` - Logging level (DEBUG, INFO, WARNING, ERROR)

**Returns:** Logger instance

#### `log_function_call(description: str)`

Decorator for automatic function call logging.

**Parameters:**
- `description` - Function description for logs

**Example:**
```python
from src.logger_config import get_logger, log_function_call

logger = get_logger()

@log_function_call("Process job data")
async def process_jobs(jobs):
    logger.info(f"Processing {len(jobs)} jobs")
    return processed_jobs
```

---

## 📋 Data Types

### Job Data Structure

Standard job data dictionary format:

```python
JobData = {
    "url": str,                    # Job posting URL
    "title": str,                  # Job title
    "company": str,                # Company name
    "location": str,               # Job location
    "salary": str,                 # Salary information
    "description": str,            # Job description
    "requirements": str,           # Job requirements
    "posted_date": str,           # Posting date
    "source": str,                # Data source platform
    "extracted_info": {           # LLM extracted information
        "graduation_match": bool,
        "deadline_info": str,
        "key_requirements": List[str],
        "experience_level": str
    },
    "filter_results": {           # Filter results
        "basic_passed": bool,
        "score": float,
        "recommendation": str,
        "reasons": List[str]
    }
}
```

### Configuration Structure

```python
Config = {
    "filter": {
        "location_keywords": List[str],
        "min_salary": int,
        "max_experience": int,
        "required_keywords": List[str]
    },
    "crawler": {
        "max_pages": int,
        "concurrent_limit": int,
        "request_delay": float,
        "max_retries": int
    },
    "llm": {
        "model": str,
        "temperature": float,
        "max_tokens": int
    },
    "notion": {
        "token": str,
        "database_id": str,
        "batch_size": int
    }
}
```

### Statistics Structure

```python
PipelineStats = {
    "total_processed": int,
    "url_duplicates": int,
    "content_duplicates": int,
    "semantic_duplicates": int,
    "unique_jobs": int,
    "basic_filtered": int,
    "advanced_filtered": int,
    "final_written": int,
    "processing_time": float,
    "success_rate": float
}
```

---

## 🔧 Usage Examples

### Complete Pipeline Usage

```python
import asyncio
from integrated_pipeline_with_filters import FilteredJobPipeline

async def main():
    # Initialize pipeline with custom config
    config = {
        "filter": {
            "min_salary": 30000,
            "location_keywords": ["Shanghai", "Remote"]
        }
    }
    
    pipeline = FilteredJobPipeline(
        config=config,
        enable_filters=True
    )
    
    # Run complete pipeline
    results = await pipeline.run_pipeline()
    
    print(f"Pipeline completed:")
    print(f"- Jobs processed: {results.get('total_processed', 0)}")
    print(f"- Jobs written: {results.get('final_job_count', 0)}")
    print(f"- Success rate: {results.get('success_rate', 0):.1%}")

asyncio.run(main())
```

### Custom Deduplication

```python
from src.enhanced_job_deduplicator import EnhancedJobDeduplicator

# Initialize with LLM support
deduplicator = EnhancedJobDeduplicator(
    llm_client=your_llm_client,
    use_llm=True
)

# Process jobs
unique_jobs = await deduplicator.deduplicate_jobs(raw_jobs)
stats = deduplicator.get_stats()

print(f"Deduplication completed:")
print(f"- Semantic duplicates removed: {stats['semantic_duplicates']}")
print(f"- Dedup rate: {stats['dedup_rate']:.1%}")
```

### Custom Filtering

```python
from src.unified_filter_system import UnifiedJobFilterManager

# Initialize filter with custom config
filter_config = {
    "min_salary": 25000,
    "required_keywords": ["Python", "AI", "Machine Learning"],
    "location_keywords": ["Beijing", "Shanghai"]
}

filter_manager = UnifiedJobFilterManager(filter_config)

# Apply filters
passed_jobs, rejected_jobs = filter_manager.apply_basic_filters(jobs)
scored_jobs = filter_manager.apply_advanced_filters(passed_jobs)

# Get top recommendations
top_jobs = [job for job in scored_jobs if job['filter_results']['score'] >= 80]
```

---

## ❓ Error Handling

All API methods include comprehensive error handling and return appropriate error information:

```python
try:
    results = await pipeline.run_pipeline()
except Exception as e:
    logger.error(f"Pipeline execution failed: {e}")
    # Handle error appropriately
```

Common error scenarios:
- **Configuration errors**: Invalid or missing configuration values
- **API errors**: LLM or Notion API failures
- **Data errors**: Invalid input data format
- **Network errors**: Connection timeouts or failures

---

## 📈 Performance Considerations

- **Batch Operations**: Use batch methods for better performance
- **Caching**: LLM responses and Notion queries are cached
- **Async Processing**: All major operations are asynchronous
- **Memory Management**: Large datasets are processed in chunks

---

**📝 Note**: This API documentation reflects the current version. For the latest updates, check the source code and inline documentation.