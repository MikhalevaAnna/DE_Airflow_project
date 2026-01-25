# DAG: pipeline_with_deps
# schedule: @hourly
# owner: admin

def download_file():
    """Скачать файл"""
    print("Downloading")

def process_file():
    """Обработать файл"""
    return "Processing"

def upload_results():
    """Загрузить результаты"""
    prev = process_file()
    print(prev)
    print("Uploading")

