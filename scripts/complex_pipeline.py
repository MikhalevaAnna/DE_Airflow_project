# DAG: complex_pipeline
# schedule: @once

def main_pipeline(**kwargs):
    """
    Главный пайплайн обработки данных
    """
    # Функция вызывает другие функции
    data = extract()
    processed = transform(data)
    load(processed)

def extract():
    return "raw_data"

def transform(data):
    return f"processed_{data}"

def load(data):
    print(f"Loaded: {data}")