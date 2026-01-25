# owner: data_team
# schedule: @daily
# tags: [etl, analytics, daily]
# description: Daily ETL pipeline for analytics
# retries: 3
# retry_delay: 5
# catchup: false

def extract_data_from_api():
    """
    Extract data from external API.
    Returns raw data for processing.
    """
    # params: {'api_endpoint': 'https://api.example.com/data', 'timeout': 30}
    print("Extracting data from API...")

def transform_data():
    """
    Clean and transform extracted data.
    Applies business rules and validations.
    """
    print("Transforming data...")

def load_to_database():
    """
    Load transformed data to target database.
    """
    print("Loading data to database...")

# Зависимости задач
# extract_data_from_api >> transform_data >> load_to_database