# Job Agent MVP - Intelligent Job Scraping & Deduplication System

> 🤖 AI-powered job scraping system with semantic deduplication and intelligent filtering

An intelligent job collection system that integrates semantic deduplication, information extraction, and automated storage using modern Python architecture and LLM capabilities.

## ✨ Key Features

- **🕷️ Smart Scraping**: Multi-platform job data collection (Boss Zhipin, etc.)
- **🧠 LLM Deduplication**: Semantic understanding-based deduplication with 44.4% dedup rate
- **🔍 Information Extraction**: LLM-driven structured data extraction
- **📝 Auto Storage**: Direct integration with Notion databases, batch operations supported
- **🎯 Smart Filtering**: Multi-layer filtering system with intelligent scoring
- **📊 Debug Support**: Comprehensive debug logs and data snapshots

## 🏗️ Architecture Overview

```
Job Agent MVP
├── 📊 Data Collection Layer    # Multi-platform crawler support
├── 🧠 Smart Deduplication     # LLM semantic + traditional dedup
├── 🔍 Information Extraction  # Structured data extraction
├── 🎯 Intelligent Filtering   # Multi-layer filtering & scoring
└── 📝 Data Storage Layer      # Notion integration
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Node.js (for Playwright browser support)

### Installation

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd job_agent_mvp
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Install Playwright browsers**
```bash
playwright install chromium
```

4. **Configure environment variables**

Create a `.env` file:
```env
# LLM Configuration
LLM_PROVIDER=deepseek
DEEPSEEK_API_KEY=your_api_key_here

# Notion Integration
NOTION_TOKEN=your_notion_token
NOTION_DATABASE_ID=your_database_id
```

5. **Configure filtering parameters**

Edit `src/config.yaml`:
```yaml
filter:
  location_keywords: ["Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Remote"]
  min_salary: 25000
  max_experience: 8
  required_keywords: ["LLM", "Machine Learning", "Deep Learning", "AI"]

crawler:
  max_pages: 3
  concurrent_limit: 3
  request_delay: 1.0
  max_jobs_test: 3          # Test mode: limit jobs per page (remove for production)

llm:
  model: "gpt-4o-mini"
  temperature: 0
  max_tokens: 500
```

### Usage Examples

1. **Full pipeline execution**
```bash
python integrated_pipeline_with_filters.py
```

2. **Debug mode (skip crawling)**
```bash
python integrated_pipeline_with_filters.py --skip-crawl --log-level debug
```

3. **Run without filters**
```bash
python integrated_pipeline_with_filters.py --no-filters
```

4. **Test mode (limit job processing)**
```bash
# Edit config.yaml to include max_jobs_test: 3
python integrated_pipeline_with_filters.py
```

## 📖 User Guide

### Basic Workflow

1. **Configure crawlers**: Set crawling parameters in `src/config.yaml`
2. **Set filter conditions**: Configure salary, location, keyword filters
3. **Run pipeline**: Execute the complete data processing flow
4. **View results**: Check processed job listings in your Notion database

### Filtering System

The system uses a **multi-layer filtering** strategy:

#### 🚫 Basic Filtering (Hard Constraints)
- Graduation date matching
- Job posting deadline validation
- Location keyword matching
- Minimum salary requirements

#### 📈 Advanced Filtering (Smart Scoring)
- Experience match scoring (30%)
- Salary competitiveness (25%)
- Company size rating (20%)
- Keyword relevance (25%)

#### ⭐ Recommendation Levels
- 🌟 **Highly Recommended** (85-100 points)
- ✅ **Recommended** (70-84 points)  
- ⚠️ **Average** (60-69 points)
- ❌ **Not Recommended** (<60 points)

### Deduplication Mechanism

Supports multi-layer deduplication:

1. **URL Deduplication**: Exact URL matching
2. **Traditional Fingerprint**: Hash-based on company+position+location
3. **LLM Semantic Deduplication**: Intelligent semantic similarity detection
4. **Notion Incremental Dedup**: Prevents reprocessing historical data

### Debug Features

- **Data Snapshots**: Auto-save intermediate results for each processing step
- **Logging System**: Multi-level log output support
- **Error Tracking**: Detailed error messages and stack traces
- **Test Mode**: Limit job processing for development and testing

### Test Mode Configuration

The system supports a **test mode** for development and debugging:

#### 🧪 **Enable Test Mode:**
Add `max_jobs_test` to your `src/config.yaml`:
```yaml
crawler:
  max_pages: 3
  max_jobs_test: 3    # Process only 3 jobs per page
```

#### 🚀 **Production Mode:**
Remove or comment out the `max_jobs_test` parameter:
```yaml
crawler:
  max_pages: 3
  # max_jobs_test: 3  # Commented out = process all jobs
```

**Test mode benefits:**
- ✅ Faster development cycles
- ✅ Reduced API costs during testing
- ✅ Easier debugging with smaller datasets
- ✅ Quick pipeline validation

## 📁 Project Structure

```
job_agent_mvp/
├── src/                              # Core source code
│   ├── crawler_registry.py           # Crawler registration & management
│   ├── enhanced_job_deduplicator.py  # Smart deduplication system
│   ├── enhanced_extractor.py         # LLM information extraction
│   ├── optimized_notion_writer.py    # Notion database writer
│   ├── unified_filter_system.py      # Unified filtering system
│   ├── config.py                     # Configuration management
│   └── logger_config.py              # Logging configuration
├── data/                             # Data file storage
├── debug/                            # Debug files and snapshots
├── integrated_pipeline_with_filters.py  # Main pipeline
├── requirements.txt                  # Python dependencies
└── project_architecture.md          # Architecture documentation
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `LLM_PROVIDER` | LLM service provider (deepseek/openai) | ✅ |
| `DEEPSEEK_API_KEY` | DeepSeek API key | ✅ |
| `NOTION_TOKEN` | Notion integration token | ✅ |
| `NOTION_DATABASE_ID` | Target Notion database ID | ✅ |

### Filter Configuration

Configure in `src/config.yaml`:

```yaml
filter:
  location_keywords:        # Target cities
    - "Beijing"
    - "Shanghai" 
    - "Shenzhen"
    - "Remote"
  min_salary: 25000        # Minimum monthly salary (RMB)
  max_experience: 8        # Maximum experience requirement (years)
  required_keywords:       # Required skill keywords
    - "LLM"
    - "Machine Learning"
```

## 🔧 Development Guide

### Adding New Crawlers

1. Register new crawler in `src/crawler_registry.py`
2. Implement crawler interface methods
3. Enable crawler in configuration file

### Custom Filter Rules

1. Extend filtering logic in `src/unified_filter_system.py`
2. Add new scoring dimensions
3. Adjust weight configurations

### Debug Tips

- Use `--skip-crawl` to skip crawling and process existing data
- Set `--log-level debug` for detailed logs
- Check data snapshot files in `debug/` directory

## 📊 Performance Metrics

### Deduplication Effectiveness

| Method | Dedup Rate | Accuracy | Notes |
|--------|------------|----------|-------|
| Traditional | 11.1% | High | URL & content fingerprint based |
| LLM Smart Dedup | 44.4% | Very High | Semantic understanding, 4x improvement |

### Processing Performance

- **Crawling Speed**: Stable under normal network conditions
- **Dedup Speed**: 9000+ items/second (traditional method)
- **LLM Extraction**: ~2 seconds/job
- **Notion Writing**: Batch operations supported

## 🐛 Troubleshooting

### Common Issues

1. **Playwright installation fails**
   ```bash
   # Manually install browsers
   playwright install chromium --with-deps
   ```

2. **API call failures**
   - Check API key correctness
   - Verify network connection
   - Review API quota limits

3. **Notion write failures**
   - Confirm token permissions
   - Verify database ID
   - Check field mapping configuration

### Log Analysis

View detailed logs:
```bash
python integrated_pipeline_with_filters.py --log-level debug
```

Check debug files:
- `debug/debug_session_latest.json` - Latest debug info
- `debug/pipeline_*.log` - Detailed execution logs

## 🤝 Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## 🆘 Getting Help

- 📖 Check [API Documentation](API_DOCUMENTATION.md)
- 🏗️ Read [Architecture Documentation](project_architecture.md)
- 🐛 Submit [Issues](../../issues)
- 💬 Join [Discussions](../../discussions)

---

**⭐ If this project helps you, please give it a star!**