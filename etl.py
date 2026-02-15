"""
Sales Intelligence Data Aggregation & ETL Pipeline
---------------------------------------------------
Extract, Transform, Load pipeline for aggregating B2B sales intelligence data
from multiple sources, with focus on data quality and standardization.

Author: Your Name
Purpose: Portfolio demonstrating ETL expertise for sales intelligence platforms
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SalesIntelligenceETL:
    """
    ETL Pipeline for aggregating B2B sales intelligence data from multiple sources.
    Handles extraction, transformation, validation, and loading of contact data.
    """
    
    def __init__(self, output_dir: str = 'output'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.metrics = {
            'sources_processed': 0,
            'total_records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'data_quality_avg': 0.0
        }
        
    # ==================== EXTRACT PHASE ====================
    
    def extract_from_csv(self, filepath: str, source_name: str) -> pd.DataFrame:
        """
        Extract data from CSV source.
        
        Args:
            filepath: Path to CSV file
            source_name: Identifier for the data source
            
        Returns:
            DataFrame with source metadata
        """
        logger.info(f"Extracting from CSV: {filepath}")
        
        df = pd.read_csv(filepath)
        df['source'] = source_name
        df['extracted_at'] = datetime.now()
        
        logger.info(f"Extracted {len(df)} records from {source_name}")
        return df
    
    def extract_from_json(self, filepath: str, source_name: str) -> pd.DataFrame:
        """
        Extract data from JSON source.
        
        Args:
            filepath: Path to JSON file
            source_name: Identifier for the data source
            
        Returns:
            DataFrame with source metadata
        """
        logger.info(f"Extracting from JSON: {filepath}")
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        df['source'] = source_name
        df['extracted_at'] = datetime.now()
        
        logger.info(f"Extracted {len(df)} records from {source_name}")
        return df
    
    def extract_from_excel(self, filepath: str, source_name: str, sheet_name: str = 0) -> pd.DataFrame:
        """
        Extract data from Excel source.
        
        Args:
            filepath: Path to Excel file
            source_name: Identifier for the data source
            sheet_name: Excel sheet name or index
            
        Returns:
            DataFrame with source metadata
        """
        logger.info(f"Extracting from Excel: {filepath}, Sheet: {sheet_name}")
        
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        df['source'] = source_name
        df['extracted_at'] = datetime.now()
        
        logger.info(f"Extracted {len(df)} records from {source_name}")
        return df
    
    def extract_from_multiple_sources(self, source_configs: List[Dict]) -> pd.DataFrame:
        """
        Extract data from multiple sources and combine.
        
        Args:
            source_configs: List of dictionaries with extraction configurations
                Example: [
                    {'type': 'csv', 'filepath': 'data1.csv', 'name': 'source1'},
                    {'type': 'json', 'filepath': 'data2.json', 'name': 'source2'}
                ]
            
        Returns:
            Combined DataFrame from all sources
        """
        logger.info(f"Extracting from {len(source_configs)} sources...")
        
        dfs = []
        
        for config in source_configs:
            try:
                source_type = config['type']
                filepath = config['filepath']
                name = config['name']
                
                if source_type == 'csv':
                    df = self.extract_from_csv(filepath, name)
                elif source_type == 'json':
                    df = self.extract_from_json(filepath, name)
                elif source_type == 'excel':
                    sheet = config.get('sheet_name', 0)
                    df = self.extract_from_excel(filepath, name, sheet)
                else:
                    logger.warning(f"Unknown source type: {source_type}")
                    continue
                
                dfs.append(df)
                self.metrics['sources_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error extracting from {config.get('name', 'unknown')}: {str(e)}")
                continue
        
        if not dfs:
            logger.error("No data extracted from any source")
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        self.metrics['total_records_extracted'] = len(combined_df)
        
        logger.info(f"Total records extracted: {len(combined_df)}")
        return combined_df
    
    # ==================== TRANSFORM PHASE ====================
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names across different sources.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized column names
        """
        logger.info("Standardizing column names...")
        
        # Column mapping for common variations
        column_mapping = {
            'email address': 'email',
            'e-mail': 'email',
            'mail': 'email',
            'phone number': 'phone',
            'telephone': 'phone',
            'mobile': 'phone',
            'company': 'company_name',
            'organization': 'company_name',
            'firm': 'company_name',
            'title': 'job_title',
            'position': 'job_title',
            'role': 'job_title',
            'first name': 'first_name',
            'fname': 'first_name',
            'last name': 'last_name',
            'lname': 'last_name',
            'surname': 'last_name',
            'city': 'location',
            'country': 'country'
        }
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.strip()
        df = df.rename(columns=column_mapping)
        
        logger.info(f"Standardized {len(df.columns)} column names")
        return df
    
    def validate_required_fields(self, df: pd.DataFrame, required_fields: List[str]) -> pd.DataFrame:
        """
        Validate that required fields exist and have values.
        
        Args:
            df: Input DataFrame
            required_fields: List of required field names
            
        Returns:
            DataFrame with validation flag
        """
        logger.info(f"Validating required fields: {required_fields}")
        
        df = df.copy()
        
        # Check for missing required fields
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            logger.warning(f"Missing required fields: {missing_fields}")
        
        # Create validation flag
        df['has_required_fields'] = True
        for field in required_fields:
            if field in df.columns:
                df['has_required_fields'] &= df[field].notna()
        
        valid_count = df['has_required_fields'].sum()
        logger.info(f"Records with all required fields: {valid_count}/{len(df)}")
        
        return df
    
    def deduplicate_records(self, df: pd.DataFrame, key_fields: List[str]) -> pd.DataFrame:
        """
        Remove duplicate records based on key fields.
        
        Args:
            df: Input DataFrame
            key_fields: Fields to use for deduplication
            
        Returns:
            Deduplicated DataFrame
        """
        logger.info(f"Deduplicating based on: {key_fields}")
        
        initial_count = len(df)
        
        # Sort by extracted_at to keep most recent
        df = df.sort_values('extracted_at', ascending=False)
        
        # Remove duplicates, keeping first (most recent)
        df = df.drop_duplicates(subset=key_fields, keep='first')
        
        duplicates_removed = initial_count - len(df)
        logger.info(f"Removed {duplicates_removed} duplicates")
        
        return df
    
    def enrich_with_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata and computed fields to enhance the dataset.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Enriched DataFrame
        """
        logger.info("Enriching data with metadata...")
        
        df = df.copy()
        
        # Add record ID
        df['record_id'] = ['REC_' + str(i).zfill(8) for i in range(len(df))]
        
        # Add processing timestamp
        df['processed_at'] = datetime.now()
        
        # Calculate data freshness (days since extracted)
        if 'extracted_at' in df.columns:
            df['data_age_days'] = (datetime.now() - df['extracted_at']).dt.days
        
        # Add data completeness score
        total_cols = len(df.columns)
        df['completeness_score'] = (df.notna().sum(axis=1) / total_cols * 100).round(2)
        
        # Flag high-value contacts (C-level, VP, Director)
        if 'job_title' in df.columns:
            high_value_titles = ['ceo', 'cto', 'cfo', 'coo', 'cmo', 'vp', 'vice president', 'director']
            df['is_high_value_contact'] = df['job_title'].str.lower().apply(
                lambda x: any(title in str(x).lower() for title in high_value_titles) if pd.notna(x) else False
            )
        
        logger.info("Metadata enrichment completed")
        return df
    
    def apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business rules and filters to the dataset.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        logger.info("Applying business rules...")
        
        initial_count = len(df)
        
        # Filter out records older than 180 days
        if 'data_age_days' in df.columns:
            df = df[df['data_age_days'] <= 180]
            logger.info(f"Filtered {initial_count - len(df)} records older than 180 days")
        
        # Filter out records with low completeness
        if 'completeness_score' in df.columns:
            df = df[df['completeness_score'] >= 40]
            logger.info(f"Filtered records with completeness < 40%")
        
        # Filter out invalid emails (if email validation was done)
        if 'email_valid' in df.columns:
            df = df[df['email_valid'] == True]
            logger.info("Filtered records with invalid emails")
        
        logger.info(f"Records after business rules: {len(df)}")
        return df
    
    def aggregate_by_company(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create company-level aggregations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Company-level aggregated DataFrame
        """
        logger.info("Creating company aggregations...")
        
        if 'company_name' not in df.columns:
            logger.warning("company_name column not found, skipping aggregation")
            return pd.DataFrame()
        
        company_agg = df.groupby('company_name').agg({
            'record_id': 'count',  # Total contacts
            'is_high_value_contact': lambda x: x.sum() if 'is_high_value_contact' in df.columns else 0,
            'completeness_score': 'mean',
            'source': lambda x: ', '.join(x.unique())
        }).reset_index()
        
        company_agg.columns = [
            'company_name', 
            'total_contacts', 
            'high_value_contacts',
            'avg_completeness_score',
            'data_sources'
        ]
        
        logger.info(f"Created aggregations for {len(company_agg)} companies")
        return company_agg
    
    # ==================== LOAD PHASE ====================
    
    def load_to_csv(self, df: pd.DataFrame, filename: str):
        """
        Load transformed data to CSV file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
        """
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False)
        logger.info(f"Loaded {len(df)} records to {output_path}")
        self.metrics['records_loaded'] = len(df)
    
    def load_to_excel(self, dataframes: Dict[str, pd.DataFrame], filename: str):
        """
        Load multiple DataFrames to Excel with separate sheets.
        
        Args:
            dataframes: Dictionary of sheet_name: DataFrame pairs
            filename: Output filename
        """
        output_path = self.output_dir / filename
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet_name, df in dataframes.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"Loaded {len(df)} records to sheet '{sheet_name}'")
        
        logger.info(f"Created Excel file: {output_path}")
    
    def load_to_json(self, df: pd.DataFrame, filename: str):
        """
        Load transformed data to JSON file.
        
        Args:
            df: DataFrame to save
            filename: Output filename
        """
        output_path = self.output_dir / filename
        
        # Convert datetime columns to string
        df_copy = df.copy()
        for col in df_copy.columns:
            if df_copy[col].dtype == 'datetime64[ns]':
                df_copy[col] = df_copy[col].astype(str)
        
        df_copy.to_json(output_path, orient='records', indent=2)
        logger.info(f"Loaded {len(df)} records to {output_path}")
    
    def create_data_quality_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate data quality report.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame containing quality metrics
        """
        logger.info("Generating data quality report...")
        
        report = []
        
        for col in df.columns:
            col_stats = {
                'column_name': col,
                'total_records': len(df),
                'non_null_count': df[col].notna().sum(),
                'null_count': df[col].isna().sum(),
                'null_percentage': (df[col].isna().sum() / len(df) * 100).round(2),
                'unique_values': df[col].nunique(),
                'data_type': str(df[col].dtype)
            }
            report.append(col_stats)
        
        report_df = pd.DataFrame(report)
        logger.info("Data quality report generated")
        
        return report_df
    
    # ==================== MAIN ETL PROCESS ====================
    
    def run_etl_pipeline(self, source_configs: List[Dict], 
                         required_fields: List[str] = None,
                         key_fields: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Run the complete ETL pipeline.
        
        Args:
            source_configs: List of source configurations
            required_fields: List of required field names
            key_fields: Fields for deduplication
            
        Returns:
            Dictionary containing transformed data and reports
        """
        logger.info("="*60)
        logger.info("STARTING SALES INTELLIGENCE ETL PIPELINE")
        logger.info("="*60)
        
        start_time = datetime.now()
        
        # Set defaults
        if required_fields is None:
            required_fields = ['email', 'company_name']
        if key_fields is None:
            key_fields = ['email']
        
        # EXTRACT
        logger.info("\n--- EXTRACT PHASE ---")
        raw_data = self.extract_from_multiple_sources(source_configs)
        
        if raw_data.empty:
            logger.error("No data extracted. Pipeline terminated.")
            return {}
        
        # TRANSFORM
        logger.info("\n--- TRANSFORM PHASE ---")
        
        # Standardize columns
        transformed_data = self.standardize_column_names(raw_data)
        
        # Validate required fields
        transformed_data = self.validate_required_fields(transformed_data, required_fields)
        
        # Deduplicate
        transformed_data = self.deduplicate_records(transformed_data, key_fields)
        
        # Enrich with metadata
        transformed_data = self.enrich_with_metadata(transformed_data)
        
        # Apply business rules
        transformed_data = self.apply_business_rules(transformed_data)
        
        self.metrics['records_transformed'] = len(transformed_data)
        
        # Create company aggregations
        company_data = self.aggregate_by_company(transformed_data)
        
        # Generate quality report
        quality_report = self.create_data_quality_report(transformed_data)
        
        # LOAD
        logger.info("\n--- LOAD PHASE ---")
        
        # Save main dataset
        self.load_to_csv(transformed_data, 'transformed_contacts.csv')
        
        # Save company aggregations
        if not company_data.empty:
            self.load_to_csv(company_data, 'company_aggregations.csv')
        
        # Save quality report
        self.load_to_csv(quality_report, 'data_quality_report.csv')
        
        # Create Excel with multiple sheets
        excel_data = {
            'Contacts': transformed_data,
            'Companies': company_data,
            'Quality Report': quality_report
        }
        self.load_to_excel(excel_data, 'sales_intelligence_data.xlsx')
        
        # Calculate final metrics
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        if len(transformed_data) > 0:
            self.metrics['data_quality_avg'] = transformed_data['completeness_score'].mean()
        
        # Print summary
        self.print_etl_summary(execution_time)
        
        logger.info("="*60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
        return {
            'contacts': transformed_data,
            'companies': company_data,
            'quality_report': quality_report
        }
    
    def print_etl_summary(self, execution_time: float):
        """Print ETL pipeline execution summary"""
        print("\n" + "="*60)
        print("ETL PIPELINE SUMMARY")
        print("="*60)
        print(f"Sources Processed: {self.metrics['sources_processed']}")
        print(f"Total Records Extracted: {self.metrics['total_records_extracted']}")
        print(f"Records Transformed: {self.metrics['records_transformed']}")
        print(f"Records Loaded: {self.metrics['records_loaded']}")
        print(f"Average Data Quality: {self.metrics['data_quality_avg']:.2f}%")
        print(f"Execution Time: {execution_time:.2f} seconds")
        print("="*60 + "\n")


def generate_sample_sources():
    """Generate sample data sources for testing"""
    
    # Source 1: CSV
    source1_data = {
        'email': ['john@techcorp.com', 'jane@datasys.io', 'mike@cloud.net'],
        'phone': ['+14155551234', '+14155555678', '+14155559012'],
        'company': ['TechCorp', 'DataSystems', 'CloudSolutions'],
        'title': ['CEO', 'VP Sales', 'Director Marketing']
    }
    pd.DataFrame(source1_data).to_csv('source1.csv', index=False)
    
    # Source 2: JSON
    source2_data = [
        {'email address': 'sarah@innovate.com', 'phone number': '+14155553456', 
         'organization': 'InnovateLabs', 'position': 'CTO'},
        {'email address': 'david@digital.io', 'phone number': '+14155557890',
         'organization': 'DigitalVentures', 'position': 'CFO'}
    ]
    with open('source2.json', 'w') as f:
        json.dump(source2_data, f, indent=2)
    
    logger.info("Sample source files created: source1.csv, source2.json")


if __name__ == "__main__":
    # Generate sample sources
    generate_sample_sources()
    
    # Configure sources
    sources = [
        {'type': 'csv', 'filepath': 'source1.csv', 'name': 'CRM Export'},
        {'type': 'json', 'filepath': 'source2.json', 'name': 'API Data'}
    ]
    
    # Run ETL pipeline
    etl = SalesIntelligenceETL(output_dir='output')
    results = etl.run_etl_pipeline(
        source_configs=sources,
        required_fields=['email', 'company_name'],
        key_fields=['email']
    )
    
    print("\nSample of Transformed Data:")
    print(results['contacts'][['email', 'company_name', 'job_title', 
                               'completeness_score', 'is_high_value_contact']].head())
