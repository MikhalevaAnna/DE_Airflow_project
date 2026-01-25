# scripts/simple_etl.py
# DAG_OWNER: data_engineer
# DAG_SCHEDULE_INTERVAL: 0 2 * * *
# DAG_TAGS: [etl, daily]

import pandas as pd

def extract():
    """Извлечение данных"""
    return pd.DataFrame({'col': [1, 2, 3]})

def transform(df):
    """Трансформация (принимает параметр!)"""
    df['new_col'] = df['col'] * 2
    return df

def load(df):
    """Загрузка (принимает параметр!)"""
    df.to_csv('/tmp/output.csv', index=False)

def etl_pipeline():
    """Главная функция пайплайна"""
    data = extract()
    transformed = transform(data)
    load(transformed)