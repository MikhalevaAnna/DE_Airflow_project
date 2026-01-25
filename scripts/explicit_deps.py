# Файл: explicit_deps.py
# schedule: @once

def extract():
    """Извлечь данные"""
    return "data"

def transform():
    """Преобразовать данные"""
    return "transformed"

def load():
    """Загрузить данные"""
    print("loaded")

# extract >> transform >> load