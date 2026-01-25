# DAG: data_pipeline
# schedule: @daily
# owner: data_team
# tags: [etl, data]
# description: Daily data processing pipeline

def extract_data():
    """Извлечение данных из источника"""
    print("Extracting data from source")

def transform_data():
    """Преобразование данных"""
    print("Transforming data")

def load_data():
    """Загрузка данных в хранилище"""
    print("Loading data to warehouse")