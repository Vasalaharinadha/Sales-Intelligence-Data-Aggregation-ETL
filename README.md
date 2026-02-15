# Sales-Intelligence-Data-Aggregation-ETL

# Sales Intelligence Data Aggregation & ETL Pipeline

A production-ready ETL (Extract, Transform, Load) pipeline for aggregating B2B sales intelligence data from multiple sources with comprehensive data quality controls.

## 🎯 Overview

This ETL pipeline consolidates contact data from various sources (CSV, JSON, Excel), standardizes formats, applies business rules, and generates clean, verified datasets ready for CRM integration or sales prospecting.

## ✨ Key Features

### Extract Phase
- **Multi-Source Support**: CSV, JSON, Excel file formats
- **Source Tracking**: Maintains data lineage from origin to final output
- **Error Handling**: Graceful failure recovery for individual sources
- **Metadata Capture**: Extraction timestamps and source identifiers

### Transform Phase
- **Column Standardization**: Unified naming across diverse data sources
- **Field Validation**: Required field checks and completeness scoring
- **Deduplication**: Smart duplicate removal with recency prioritization
- **Data Enrichment**: 
  - Record IDs generation
  - Data freshness calculation
  - Completeness scoring
  - High-value contact flagging (C-Level, VP, Director)
- **Business Rules**: Configurable filters for data age and quality thresholds
- **Company Aggregations**: Roll-up statistics at organization level

### Load Phase
- **Multiple Output Formats**: CSV, Excel (multi-sheet), JSON
- **Data Quality Reports**: Automated quality metrics generation
- **Company Intelligence**: Aggregated company-level insights
- **Excel Dashboard**: Multi-sheet workbook with contacts, companies, and quality metrics

## 🚀 Installation

```bash
git clone https://github.com/yourusername/sales-intelligence-etl.git
cd sales-intelligence-etl

pip install -r requirements.txt
```

## 📦 Requirements

```
pandas>=1.5.0
numpy>=1.23.0
openpyxl>=3.0.0
```

## 💻 Usage

### Quick Start

```python
from etl_pipeline import SalesIntelligenceETL

# Configure data sources
sources = [
    {'type': 'csv', 'filepath': 'contacts.csv', 'name': 'CRM Export'},
    {'type': 'json', 'filepath': 'api_data.json', 'name': 'API Data'},
    {'type': 'excel', 'filepath': 'leads.xlsx', 'name': 'Marketing Leads', 'sheet_name': 'Sheet1'}
]

# Initialize ETL pipeline
etl = SalesIntelligenceETL(output_dir='output')

# Run complete pipeline
results = etl.run_etl_pipeline(
    source_configs=sources,
    required_fields=['email', 'company_name'],
    key_fields=['email']
)
```

### Individual Phase Execution

```python
# Extract only
raw_data = etl.extract_from_multiple_sources(sources)

# Transform operations
standardized = etl.standardize_column_names(raw_data)
validated = etl.validate_required_fields(standardized, ['email', 'company_name'])
deduplicated = etl.deduplicate_records(validated, ['email'])
enriched = etl.enrich_with_metadata(deduplicated)

# Load to different formats
etl.load_to_csv(enriched, 'output.csv')
etl.load_to_json(enriched, 'output.json')
etl.load_to_excel({'Contacts': enriched}, 'output.xlsx')
```

## 📊 Output Files

The pipeline generates the following outputs in the `output/` directory:

### 1. transformed_contacts.csv
Main cleaned and enriched contact dataset with:
- Standardized fields (email, phone, company_name, job_title)
- Record IDs
- Data quality scores
- High-value contact flags
- Processing timestamps
- Source tracking

### 2. company_aggregations.csv
Company-level statistics:
- Total contacts per company
- High-value contact count
- Average data completeness
- Data source summary

### 3. data_quality_report.csv
Column-level quality metrics:
- Null/non-null counts
- Null percentages
- Unique value counts
- Data types

### 4. sales_intelligence_data.xlsx
Multi-sheet Excel workbook containing all above datasets for easy analysis.

## 🔍 Data Quality Features

### Completeness Scoring
Each record receives a completeness score (0-100%) based on:
- Number of non-null fields
- Weighted by field importance

### Business Rules
Configurable filters:
- **Data Freshness**: Filter records older than 180 days (default)
- **Quality Threshold**: Minimum completeness score of 40% (default)
- **Email Validation**: Remove invalid email addresses
- **Required Fields**: Enforce presence of critical fields

### High-Value Contact Detection
Automatically flags decision-makers:
- C-Level executives (CEO, CTO, CFO, etc.)
- Vice Presidents
- Directors
- Other configurable titles

## 📈 ETL Metrics

The pipeline tracks and reports:
```
============================================================
ETL PIPELINE SUMMARY
============================================================
Sources Processed: 3
Total Records Extracted: 1,523
Records Transformed: 1,401
Records Loaded: 1,401
Average Data Quality: 87.34%
Execution Time: 2.45 seconds
============================================================
```

## 🎯 Use Cases

Perfect for:
- **Data Migration**: Consolidate contacts before CRM migration
- **Multi-Source Integration**: Merge data from CRM, marketing tools, APIs
- **Data Quality Improvement**: Clean and standardize existing databases
- **Sales Intelligence**: Prepare verified contact lists for outreach
- **Company Research**: Generate company-level intelligence reports
- **GTM Operations**: Build unified contact databases for go-to-market teams

## 🔧 Configuration

### Column Mapping
Automatically maps common variations:
```python
'email address' → 'email'
'company' → 'company_name'
'title' → 'job_title'
'first name' → 'first_name'
# ... and more
```

### Deduplication Strategy
- Sorts by extraction timestamp (most recent first)
- Removes duplicates based on configurable key fields
- Keeps the most recent record

### Business Rules Customization
Modify thresholds in the code:
```python
# Data age filter (days)
df = df[df['data_age_days'] <= 180]

# Completeness threshold (%)
df = df[df['completeness_score'] >= 40]
```

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   EXTRACT   │────▶│  TRANSFORM   │────▶│     LOAD     │
└─────────────┘     └──────────────┘     └──────────────┘
      │                    │                     │
   CSV/JSON/Excel    Standardize            CSV/Excel/JSON
   Multiple Sources   Validate              Quality Reports
   Source Tracking    Deduplicate           Company Aggregations
   Error Handling     Enrich                Multi-sheet Excel
                      Business Rules
```

## 📊 Sample Output

### Transformed Contacts
| email | company_name | job_title | completeness_score | is_high_value_contact |
|-------|--------------|-----------|-------------------|-----------------------|
| john@techcorp.com | TechCorp | CEO | 95.5 | True |
| jane@datasys.io | DataSystems | VP Sales | 88.2 | True |

### Company Aggregations
| company_name | total_contacts | high_value_contacts | avg_completeness_score |
|--------------|----------------|---------------------|------------------------|
| TechCorp | 15 | 5 | 87.3 |
| DataSystems | 23 | 8 | 91.5 |



---

⭐ Star this repo if you find it useful!
