# ETL Quality Gate

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/Tests-72%20passing-brightgreen.svg)](https://github.com/username/etl-quality-gate/actions/workflows/api-tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A professional Python testing framework for ELT (Extract, Load, Transform) data quality validation, demonstrating production-ready API client design and comprehensive testing patterns for cryptocurrency market data.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
  - [Framework in Action (GIF Demo)](#framework-in-action)
  - [Run API Tests Only](#run-api-tests-only)
  - [Run ELT Pipeline Tests](#run-elt-pipeline-tests)
- [Installation](#installation)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Project Goals](#project-goals)
- [API Testing Suite](#api-testing-suite)
- [ELT Pipeline Testing Suite](#elt-pipeline-testing-suite)
- [Test Results](#test-results)
- [Framework Design](#framework-design)
- [Testing Strategy](#testing-strategy)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [CI/CD](#cicd)
- [Reporting](#reporting)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project serves as both a **learning resource** and a **production-ready testing framework** for cryptocurrency data pipelines. It demonstrates how to build scalable API clients with proper error handling, rate limiting, and data validation while ensuring end-to-end data quality from extraction to database storage.

## Quick Start

### Framework in Action

![API Tests Running](API.gif)

*Live demonstration of the API testing framework running in Docker - 35 tests passing with comprehensive validation including endpoints, authentication, search functionality, and error handling. WARNING: This GIF shows the API tests only - the full ELT pipeline tests require additional setup. IT TAKES A LOTS OF TIME TO RUN BUT THAT IS DUE TO RATE LIMITING*

### Run API Tests Only

```bash
docker compose -f docker-compose.api.yml up --build --exit-code-from api_tests
```

### Run ELT Pipeline Tests

```bash
docker compose -f docker-compose.elt.yml up --build --exit-code-from elt_tests
```

## Installation

### Prerequisites

- **Python 3.11+** - Required for all project components
- **Docker & Docker Compose** - For containerized testing
- **PostgreSQL** - For ELT pipeline tests (can be containerized)
- **Git** - For version control and CI/CD integration

### Local Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/etl-quality-gate.git
   cd etl-quality-gate
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   # OR using pyproject.toml:
   pip install -e .
   ```

4. **Verify installation:**
   ```bash
   python -c "import src.api.coingecko; print('Installation successful')"
   ```

### Docker Setup (Alternative)

For users preferring containerized development:

```bash
# Build and run API tests
docker compose -f docker-compose.api.yml up --build

# Build and run ELT pipeline tests  
docker compose -f docker-compose.elt.yml up --build
```

## Configuration

### Environment Variables

Create a `.env` file by copying the example:

```bash
cp .env.example .env
# Then edit .env with your specific values
```

**Available Variables:**
```bash
# Database Configuration (for ELT pipeline tests)
DB_URL=postgresql://postgres:admin@localhost:5432/coincap_db

# Test Configuration
EXTRACTION_LIMIT=5

# Optional: CoinGecko API Key (for authenticated endpoints)
COINGECKO_API_KEY=your_api_key_here

# Optional: Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### Database Setup

For ELT pipeline testing, ensure PostgreSQL is running:

```bash
# Using Docker (recommended)
docker compose -f docker-compose.elt.yml up -d postgres

# Or local installation
sudo -u postgres createdb coincap_db
sudo -u postgres -c "ALTER USER postgres PASSWORD 'admin';"
```

### API Rate Limits

CoinGecko Free API limitations:
- **Rate Limit**: ~30 requests per minute
- **Retry Strategy**: Exponential backoff with 1s base delay
- **Rate Limit Response**: HTTP 429 with `Retry-After` header
- **Concurrent Requests**: Avoid parallel requests to prevent rate limiting

### Logging Configuration

Configure logging levels for debugging:

```python
import logging
logging.basicConfig(level=logging.INFO)
# For verbose debugging:
logging.basicConfig(level=logging.DEBUG)
```

## Project Structure

Unified architecture with clear separation of concerns:

```
├── src/                       # All source code
│   ├── api/                   # API clients (BaseAPIClient, CoinGeckoClient)
│   │   ├── base_client.py    # Generic REST client with retry logic
│   │   └── coingecko_client.py # CoinGecko-specific implementation
│   ├── models/                # Pydantic models for data validation
│   │   └── __init__.py        # Asset, CoinListItem, PingResponse models
│   ├── db/                    # Database layer
│   │   └── db_manager.py     # SQLAlchemy ORM and PostgreSQL operations
│   └── utils/                 # Utility functions
│       └── data_handler.py    # CSV export/import operations
├── tests/                     # All tests
│   ├── conftest.py            # Shared pytest fixtures
│   ├── assertions.py          # Custom validation helpers
│   ├── utils.py               # Common test utilities
│   ├── test_api/              # API endpoint testing
│   │   ├── test_endpoints.py   # Core endpoint validation
│   │   ├── test_search.py       # Search functionality
│   │   ├── test_coins_list.py  # Coins list endpoint
│   │   ├── test_authentication.py # Auth scenarios
│   │   ├── test_error_responses.py # Error handling
│   │   └── test_extended_models.py # Extended model validation
│   └── test_elt/              # ELT pipeline testing
│       ├── test_pipeline.py   # End-to-end data flow validation
│       ├── test_database_crud.py # Database CRUD operations
│       └── test_database_errors.py # Database error handling
├── Dockerfile.api             # API tests container image
├── docker-compose.elt.yml     # ELT tests orchestration with PostgreSQL
├── .github/workflows/         # CI/CD pipelines
│   ├── api-tests.yml         # API test automation
│   └── elt-pipeline.yml      # ELT test automation
└── pyproject.toml             # Project configuration (Ruff, pytest, dependencies)
```

## Project Goals

### ETL Testing Goal
**Validate data integrity and pipeline reliability** for cryptocurrency data flows. This framework ensures that data remains accurate, complete, and consistent throughout the Extract → Transform → Load process, providing confidence in financial data quality.

### API Testing Focus
**Comprehensive validation of CoinGecko Public API v3.0.1** - We test the reliability, accuracy, and resilience of cryptocurrency market data endpoints. This ensures our data pipelines can depend on a stable, well-documented API service.

**Value Provided:**
- **Reliability Assurance**: API endpoints respond consistently and handle errors gracefully
- **Data Accuracy**: Cryptocurrency prices and market data are validated for correctness
- **Production Readiness**: Rate limiting and retry patterns suitable for enterprise deployment
- **Educational Foundation**: Demonstrates professional API client architecture patterns

**Key API Capabilities Tested:**
- **Market Data**: Real-time cryptocurrency prices and market statistics
- **Coin Information**: Comprehensive cryptocurrency metadata and details
- **Search Functionality**: Discovery of coins, categories, and exchanges
- **Rate Limiting**: Proper handling of API usage limits (30 requests/minute)
- **Error Resilience**: Graceful handling of network issues and API errors

## API Testing Suite

Comprehensive test suite for CoinGecko API behavior with professional testing patterns.

**Test Coverage:**
- **Happy Paths**: 200 OK, valid schema validation
- **Negative Paths**: 404 Not Found, 400 Bad Request, 401 Unauthorized
- **Authentication**: Token validation, header injection
- **Error Handling**: Structured error response validation
- **Search Functionality**: Query validation and response structure

**Tested Endpoints:**
- `/ping` - Health check ([docs](https://docs.coingecko.com/v3.0.1/reference/ping-server))
- `/coins/markets` - Market data ([docs](https://docs.coingecko.com/v3.0.1/reference/coins/markets))
- `/coins/list` - Coin enumeration ([docs](https://docs.coingecko.com/v3.0.1/reference/coins-list))
- `/search` - Search coins, categories, markets ([docs](https://docs.coingecko.com/v3.0.1/reference/search-data))

**Test Organization:**
- `test_endpoints.py` - Core endpoint testing (ping, markets, coins list)
- `test_search.py` - Search functionality and response validation
- `test_coins_list.py` - Coins list endpoint validation
- `test_authentication.py` - Auth scenarios and security
- `test_error_responses.py` - Error schema validation
- `test_extended_models.py` - Extended Asset model field validation

**Testing Framework Features:**
- **Custom Assertions**: Reusable validation helpers (`tests/assertions.py`)
- **Test Utilities**: Common setup/teardown patterns (`tests/utils.py`)
- **Schema Validation**: Pydantic model validation for all responses
- **Extended Model Testing**: Validation of new Asset model fields (market_cap_rank, price_change_percentage_24h)

## ELT Pipeline Testing Suite

**Core Data Engineering pipeline with database integration that extends the API testing framework.**

### Relationship to API Testing
The ELT pipeline tests **build upon and extend** the API testing foundation:

1. **API Testing Layer** - Validates external API behavior and data reliability
2. **Database Integration Layer** - Extends testing to include data persistence and CRUD operations
3. **ELT Pipeline Layer** - Combines both layers for end-to-end data flow validation

### Why ELT Testing as Extension
- **Progressive Complexity**: Demonstrates ability to extend from simple API tests to complex data pipelines
- **Real-World Scenarios**: Tests complete Extract → Transform → Load workflows used in production
- **Data Quality Focus**: Ensures API data remains accurate throughout the entire pipeline
- **Integration Validation**: Proves API and database components work together seamlessly

**Why we implemented ELT testing:**
- **Data Quality Validation**: Ensures API data matches database records
- **Pipeline Integrity**: Validates Extract → Transform → Load flow
- **Production Readiness**: Tests real-world data persistence scenarios
- **Reconciliation**: Critical for financial data accuracy

**Test Coverage:**
- **Data Reconciliation**: API Data == DB Data (price consistency within 1%)
- **Database Persistence**: Verify data is correctly stored and retrieved
- **Schema Integrity**: Ensure database schema matches Pydantic models
- **Database CRUD Operations**: Create, Read, Update, Delete functionality
- **Database Error Handling**: Constraint violations, connection issues
- **Extended Model Validation**: New Asset model fields in database context

**Test Organization:**
- `test_pipeline.py` - End-to-end data flow validation
- `test_database_crud.py` - Database CRUD operations testing
- `test_database_errors.py` - Database error scenarios and edge cases

## Test Results

| Test Category | Tests | Pass Rate | Coverage |
|---------------|--------|-----------|----------|
| API Endpoints | 35 | 100% | Full |
| Authentication | 4 | 100% | Full |
| Search & Discovery | 11 | 100% | Full |
| Extended Models | 6 | 100% | Full |
| Database CRUD | 13 | 100% | Full |
| Database Errors | 17 | 100% | Full |
| ELT Pipeline | 6 | 100% | Full |
| **TOTAL** | **72** | **100%** | **Full** |




### Key Achievements
- **Zero Code Quality Issues**: Perfect ruff compliance
- **100% Test Success**: All functionality validated
- **Production Ready**: Docker containers passing
- **Framework Excellence**: Extensible and maintainable

## Framework Design

### Core Philosophy
This framework demonstrates **extensible API testing architecture** with clear separation of concerns and progressive complexity from API testing to full ELT pipeline validation.

### Architecture Benefits
- **Easy Test Addition**: New tests require minimal setup due to shared fixtures and utilities
- **Modular Components**: Reusable assertions, helpers, and test patterns
- **Configuration-Driven**: Environment-specific settings and flexible API client design
- **Scalable Patterns**: Designed for team collaboration and future growth

### Testing Progression
**API Tests → ELT Pipeline**: The framework is designed as a **progressive testing environment**:

1. **API Testing Layer**: Validates external API behavior and reliability
2. **Database Integration Layer**: Extends testing to data persistence and CRUD operations  
3. **ELT Pipeline Layer**: Combines API and database testing for end-to-end validation

### Why This Approach
- **Production Readiness**: Mirrors real-world testing requirements from API to database
- **Maintainability**: Clear patterns enable easy addition of new tests and APIs
- **Team Collaboration**: Modular structure allows multiple developers to work efficiently
- **Educational Value**: Demonstrates comprehensive testing methodology

## Architecture

### Custom API Client vs Official SDK

**Why we built a custom client instead of using `coingecko-sdk`:**

This project uses a custom `BaseAPIClient` and `CoinGeckoClient` instead of the official `coingecko-sdk` for educational and architectural reasons:

**Educational Benefits:**
- Demonstrates professional API client design patterns
- Shows implementation of retry logic, rate limiting, and error handling
- Teaches scalable architecture principles

**Architectural Control:**
- Full understanding of every line of code
- Customizable for specific testing requirements
- No dependency on external library maintenance

**Production Recommendation:**
For production use, we recommend the official SDK:
```bash
pip install coingecko-sdk
```

The official SDK provides:
- Built-in error handling and rate limiting
- Official support and maintenance
- Less boilerplate code

**Our Approach:**
- **Learning**: Demonstrates how to build production-ready API clients
- **Testing**: Shows professional testing patterns and scalability
- **Customization**: Tailored specifically for our testing framework needs

### Rate Limiting & Retry Strategy

**Why Exponential Backoff is Critical:**
CoinGecko's free API has strict rate limits (typically 30 requests/minute). Our custom retry implementation:

```python
@exponential_backoff_retry(
    max_retries=5,
    base_delay=1.0,
    max_delay=60.0,
    exceptions=(Exception,),
)
def _fetch_markets_with_retry(self, limit: int) -> list[dict]:
    # Handles 429 Rate Limit: Exponential backoff respects limits
    # Handles 5xx Server Errors: Automatic retry with increasing delays
    # Handles Network timeouts: Graceful recovery from transient failures
```

**Benefits:**
- **Respects API limits**: Prevents temporary blocking
- **Graceful degradation**: Fails elegantly after max retries
- **Production ready**: Standard pattern for enterprise applications

**Future Enhancements:**
- **Parallel Test Execution**: `pytest-xdist` configuration planned (skipped due to rate limits)
- **Test Categorization**: Smoke, regression, integration test markers
- **Dynamic Test Generation**: Based on API specifications

### BaseAPIClient Design (Scalability Foundation)

The `src/api/base_client.py` provides a **generic REST API client** supporting:

- **HTTP Methods**: GET, POST, PUT, DELETE
- **Configurable Base URLs**: For any REST API endpoint
- **Dynamic Header Injection**: Bearer tokens, API keys, custom headers
- **Exponential Backoff/Retry**: Built-in resilience for rate limits
- **Session Management**: Connection pooling for performance

This design enables **future scalability** for POST/PUT operations:
- Authentication endpoints (POST /auth/login)
- Data submission (POST /api/data)
- Resource updates (PUT /api/resources/{id})
- Deletion operations (DELETE /api/resources/{id})

### CoinGeckoClient

Extends `BaseAPIClient` for CoinGecko-specific operations:
```python
class CoinGeckoClient(BaseAPIClient):
    def get_assets(self, limit: int = 10) -> list[Asset]:
        return self.get("coins/markets", params={...})
```

## Why GET is Idempotent (Scalability)

We use **GET** for data extraction because it is **idempotent and stateless**:

- **Horizontal Partitioning**: Workers fetch different coin subsets independently
- **Cacheability**: Responses cached at edge locations
- **Fault Tolerance**: Failed requests retried safely without side effects
- **Load Distribution**: Requests distributed across API key pools

## Future Scalability

### BaseAPIClient for POST/PUT/DELETE & Authentication

The `BaseAPIClient` is designed for production scalability beyond just GET requests:

```python
# Authentication with Bearer token
client = BaseAPIClient(base_url="https://api.example.com")
client.set_auth_header("eyJhbGciOiJIUzI1NiIs...")  # JWT token

# POST request for data submission
response = client.post("/api/v1/data", json_data={
    "asset_id": "bitcoin",
    "price": 45000.00
})

# PUT request for updates
response = client.put("/api/v1/assets/bitcoin", json_data={
    "price": 46000.00
})

# DELETE request for resource removal
response = client.delete("/api/v1/assets/bitcoin")
```

**Production Use Cases:**
- **Webhook ingestion**: POST endpoints for real-time data ingestion
- **CRUD operations**: Full REST API lifecycle management
- **Authentication flow**: Login (POST) → Token → Authenticated requests
- **Multi-tenant APIs**: Dynamic header injection for tenant identification

## Testing Strategy

### Validation Methodology
This framework employs **multi-layered validation** to ensure comprehensive quality assurance:

#### API Layer Validation
- **Schema Validation**: Pydantic models ensure API response integrity
- **Error Boundary Testing**: Comprehensive edge case and failure scenario coverage
- **Authentication Testing**: Token validation and security scenario testing
- **Performance Testing**: Response time and reliability verification

#### Database Layer Validation  
- **CRUD Operations**: Create, Read, Update, Delete functionality testing
- **Constraint Validation**: Database rules, data integrity, and error handling
- **Connection Testing**: Database availability, reconnection, and failure scenarios
- **Schema Consistency**: Database schema matches Pydantic model validation

#### ELT Pipeline Validation
- **End-to-End Testing**: Complete Extract → Transform → Load workflow validation
- **Data Reconciliation**: API data accuracy vs. database storage verification
- **Integration Testing**: API and database layers working together seamlessly
- **Data Quality**: Price consistency, completeness, and accuracy validation

### Why This Comprehensive Approach
- **Production Readiness**: Mirrors real-world testing requirements for data pipelines
- **Risk Mitigation**: Multiple validation layers catch issues early in development
- **Maintainability**: Clear validation patterns enable easy addition of new test scenarios
- **Team Scalability**: Modular approach supports multiple developers working efficiently

### Error Prevention Strategy
- **Proactive Validation**: Test for expected failures before they occur in production
- **Rate Limiting**: Proper handling of API constraints and retry logic
- **Data Integrity**: Multiple validation checkpoints ensure data quality throughout pipeline
- **Documentation**: Clear test descriptions and expected behaviors for team alignment

## Technology Stack

- **API Client**: Custom `BaseAPIClient` with exponential backoff
- **Data Validation**: Pydantic models
- **Database**: SQLAlchemy ORM with PostgreSQL
- **Testing**: Pytest with HTML reporting (`pytest-html`)
- **Linting**: Ruff
- **CI/CD**: GitHub Actions with multi-stage Docker builds and artifact upload

### CI/CD Pipeline Strategy

The project uses two separate GitHub Actions workflows, both supporting manual execution:

#### API Tests Workflow (`api-tests.yml`)
- **Automatic Execution**: Runs on every push to `main` and `develop` branches
- **Manual Execution**: Can be triggered manually via GitHub Actions UI
- **Purpose**: Fast feedback for API functionality and code quality
- **Scope**: Tests API endpoints, authentication, and basic functionality
- **Artifacts**: Uploads HTML test reports for review

#### ELT Pipeline Workflow (`elt-pipeline.yml`)
- **Manual Execution Only**: Runs only when manually triggered via GitHub Actions UI
- **Purpose**: Comprehensive end-to-end testing with database integration
- **Scope**: Full ELT pipeline validation with PostgreSQL
- **Rationale**: Manual execution prevents excessive CI/CD resource usage due to:
  - Longer execution time (rate limiting from external API)
  - Database setup/teardown overhead
  - Resource-intensive operations

#### How to Run Workflows Manually

1. **Via GitHub Actions UI**:
   - Go to: https://github.com/plewkowycz/etl-quality-gate/actions
   - Select workflow: "API Tests" or "ELT Pipeline"
   - Click "Run workflow" with optional reason
   - Monitor progress and download HTML report

2. **Via Local Docker**:
   ```bash
   # API Tests
   docker compose -f docker-compose.api.yml up --build --exit-code-from api_tests
   
   # ELT Pipeline Tests
   docker compose -f docker-compose.elt.yml up --build --exit-code-from elt_tests
   ```

### Caching Strategy

Both workflows implement intelligent caching to optimize CI/CD performance:

#### Caching Benefits
- **Faster Builds**: Docker layers cached between runs
- **Reduced Downloads**: Python dependencies cached
- **Cost Efficiency**: Less GitHub Actions minutes usage
- **Consistent Environment**: Same cache keys across workflows

#### Cache Implementation
- **Python Dependencies**: Cached by `pyproject.toml` hash
- **Docker Layers**: Cached by Dockerfile and compose file changes
- **Cache Keys**: OS-specific with file content hashing
- **Fallback**: Partial cache restoration when exact match not found

#### Performance Impact
- **Before**: ~2-3 minutes per workflow (no cache)
- **After**: ~30-60 seconds per workflow (with cache)
- **Savings**: ~70-80% reduction in execution time

## Troubleshooting

### Common Issues

#### Docker Issues

**Problem:** `docker compose up` fails with "port already in use"
```bash
# Solution: Find and kill existing process
lsof -ti:5432 | xargs kill -9

# Or use different port
docker compose -f docker-compose.elt.yml up -d --scale postgres=5433
```

**Problem:** Container exits with "ModuleNotFoundError: No module named 'src'"
```bash
# Solution: Ensure PYTHONPATH is set correctly
docker compose -f docker-compose.api.yml up --build -e PYTHONPATH=/app
```

#### API Issues

**Problem:** HTTP 429 Rate Limit errors during testing
```bash
# Solution: Tests include automatic retry, but for manual testing:
# Wait between requests or reduce test frequency
sleep 2  # Wait 2 seconds between requests
```

**Problem:** "Invalid API response" or schema validation errors
```bash
# Solution: Check API documentation for endpoint changes
# Verify request parameters match API expectations
curl "https://api.coingecko.com/api/v3/ping" -H "accept: application/json"
```

#### Database Issues

**Problem:** "Connection refused" to PostgreSQL
```bash
# Solution: Check PostgreSQL service status
docker compose -f docker-compose.elt.yml ps postgres

# Restart services if needed
docker compose -f docker-compose.elt.yml restart postgres
```

**Problem:** "relation 'assets' does not exist" during ELT tests
```bash
# Solution: Ensure database tables are created
# The db_manager fixture handles this automatically
# Manual verification:
docker compose -f docker-compose.elt.yml exec elt_tests python -c "
from src.db.db_manager import DBManager
db = DBManager()
db.create_tables()
print('Tables created successfully')
"
```

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
# Run tests with debug output
docker compose -f docker-compose.api.yml up --build -e LOG_LEVEL=DEBUG

# Run pytest with verbose output
pytest tests/ -v -s --tb=long
```

## Contributing

We welcome contributions! Please follow these steps:

### Development Setup
```bash
# Clone repository
git clone <repository-url>
cd etl-quality-gate

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Start database for ELT tests
docker compose -f docker-compose.elt.yml up -d postgres

# Run tests
pytest tests/ -v
```

### Code Quality Standards
- **Ruff**: All code must pass `ruff check src/ tests/`
- **Tests**: New features must include comprehensive tests
- **Documentation**: Update README for new functionality
- **Git**: Use conventional commits (`feat:`, `fix:`, `docs:`)

### Testing Requirements
- **API Tests**: Must pass in Docker container
- **Database Tests**: Must handle edge cases and errors
- **Integration Tests**: Validate end-to-end workflows
- **Coverage**: Maintain high test coverage for new code

### Pull Request Process
1. Fork repository and create feature branch
2. Implement changes with tests
3. Ensure all tests pass locally and in Docker
4. Submit PR with clear description
5. Address review feedback promptly

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

**Permissions**:
- ✅ Commercial use
- ✅ Modification  
- ✅ Distribution
- ✅ Private use

**Conditions**:
- ⚠️ Include license and copyright notice
- ⚠️ Provide license copy with major changes

**Disclaimer**:
This project is for educational and testing purposes. Cryptocurrency data is provided "as-is" without warranties. Use at your own risk.

