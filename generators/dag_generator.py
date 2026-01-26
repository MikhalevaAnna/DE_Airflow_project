#!/usr/bin/env python3
"""
Генератор DAG из файлов в папке scripts
Каждый SQL запрос - отдельная задача
Для Python функции - если функции независимые, то отдельные задачи
Для зависимых Python функций - одна задача
"""

import re
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from sql_parser import SQLParser
from python_parser import PythonParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MultiTaskDAGGenerator:
    def __init__(self):
        self.scripts_dir = Path("/opt/airflow/scripts")
        self.dags_dir = Path("/opt/airflow/dags")

        # Инициализируем парсеры
        self.sql_parser = SQLParser()
        self.python_parser = PythonParser()

        # Создаем директории, если их нет
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        self.dags_dir.mkdir(parents=True, exist_ok=True)

    def parse_file(self, file_path: Path) -> Dict:
        """Парсинг файла для извлечения задач"""
        result = {
            'filename': file_path.name,
            'tasks': [],
            'config': {},
            'original_content': '',
            'imports': [],
            'functions_info': [],
            'global_imports': set()
        }

        try:
            content = file_path.read_text(encoding='utf-8')
            result['original_content'] = content

            logger.info(f"\n{'=' * 60}")
            logger.info(f"Парсим файл: {file_path.name}")
            logger.info(f"{'=' * 60}")

            # Базовое извлечение конфигурации DAG
            config_pattern = r'(?:#|--)\s*(?:DAG_)?(\w+):\s*(.+)'
            matches = re.findall(config_pattern, content, re.IGNORECASE)

            # Логгирование найденной конфигурации
            if matches:
                logger.info(f"Найдена конфигурация DAG: {len(matches)} параметров")
                for key, value in matches:
                    logger.info(f"  {key}: {value}")
            else:
                logger.info("Конфигурация DAG не найдена, будут использованы значения по умолчанию")

            for key, value in matches:
                key = key.lower()
                value = value.strip()

                if key == 'schedule':
                    if 'schedule_interval' not in result['config']:
                        result['config']['schedule_interval'] = self._format_value(value)
                elif key == 'schedule_interval':
                    result['config'][key] = self._format_value(value)
                elif key == 'tags':
                    result['config'][key] = self._parse_tags(value)
                elif key == 'description':
                    result['config'][key] = self._format_value(value)
                elif key == 'catchup':
                    result['config'][key] = 'True' if value.lower() == 'true' else 'False'
                elif key == 'owner':
                    result['config'][key] = f'"{value}"'
                elif key == 'start_date':
                    result['config'][key] = self._parse_start_date(value)
                elif key == 'retry_delay':
                    if value.isdigit():
                        result['config'][key] = f"timedelta(seconds={value})"
                    else:
                        result['config'][key] = f"timedelta({value})"
                elif key == 'retries':
                    result['config'][key] = value
                else:
                    result['config'][key] = self._format_value(value)

            # Устанавливаем значение по умолчанию для start_date если оно не задано
            if 'start_date' not in result['config']:
                result['config']['start_date'] = self._parse_start_date('')
                logger.info(f"start_date не задан, используется по умолчанию: {result['config']['start_date']}")

            # Для Python файлов
            if file_path.suffix.lower() == '.py':
                logger.info("Обрабатываем Python файл")
                parsed_data = self.python_parser.parse_python_file(content, file_path.stem)
                result['tasks'] = parsed_data['tasks']
                result['imports'] = parsed_data['imports']
                result['functions_info'] = parsed_data['functions_info']
                result['global_imports'] = parsed_data['global_imports']
                logger.info(f"Найдено {len(result['tasks'])} функций")

            # Для SQL файлов
            elif file_path.suffix.lower() == '.sql':
                logger.info("Обрабатываем SQL файл")
                parsed_data = self.sql_parser.parse_sql_file(content)
                result['tasks'] = parsed_data['tasks']
                logger.info(f"Найдено {len(result['tasks'])} SQL команд")

        except Exception as e:
            logger.error(f"Ошибка парсинга {file_path.name}: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")

        return result

    def _parse_tags(self, tags_str: str) -> str:
        """Парсит строку теги и форматирует для Python"""
        try:
            if tags_str.startswith('[') and tags_str.endswith(']'):
                try:
                    inner = tags_str[1:-1].strip()
                    if not inner:
                        return "[]"

                    tags = []
                    for tag in inner.split(','):
                        tag = tag.strip().strip("'\"")
                        if tag:
                            tags.append(tag)
                    return json.dumps(tags)
                except:
                    return tags_str
            else:
                tags = []
                for tag in tags_str.split(','):
                    tag = tag.strip().strip("'\"")
                    if tag:
                        tags.append(tag)
                return json.dumps(tags)
        except Exception as e:
            logger.warning(f"Ошибка парсинга тегов '{tags_str}': {e}")
            return json.dumps(['auto-generated'])

    def _format_value(self, value: str) -> str:
        """Форматирует значение для вставки в Python код"""
        if value.startswith('datetime('):
            return value

        if value.replace('.', '', 1).isdigit() and value.count('.') <= 1:
            return value

        if value.lower() in ['none', 'null']:
            return 'None'

        if value.lower() in ['true', 'false']:
            return value.lower()

        schedule_values = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
        if value in schedule_values:
            return f'"{value}"'

        if re.match(r'^\d+(\s+\d+){4}$', value):
            return f'"{value}"'

        return f'"{value}"'

    def _parse_start_date(self, value: str) -> str:
        """Парсит start_date и возвращает корректный datetime объект"""
        try:
            if not value or value.lower() == 'today':
                now = datetime.now()
                return f'datetime({now.year}, {now.month}, {now.day})'

            if value.startswith('datetime('):
                return value

            if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                year, month, day = map(int, value.split('-'))
                return f'datetime({year}, {month}, {day})'

            if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', value):
                date_part, time_part = value.split(' ')
                year, month, day = map(int, date_part.split('-'))
                hour, minute, second = map(int, time_part.split(':'))
                return f'datetime({year}, {month}, {day}, {hour}, {minute}, {second})'

            if value.isdigit():
                dt_obj = datetime.fromtimestamp(int(value))
                return f'datetime({dt_obj.year}, {dt_obj.month}, {dt_obj.day})'

            now = datetime.now()
            return f'datetime({now.year}, {now.month}, {now.day})'

        except Exception as e:
            logger.warning(f"Ошибка парсинга start_date '{value}': {e}")
            now = datetime.now()
            return f'datetime({now.year}, {now.month}, {now.day})'

    def _remove_duplicate_imports(self, code: str) -> str:
        """Убирает дублирующиеся импорты"""
        return self.python_parser._remove_duplicate_imports(code)

    def _adapt_function_for_airflow(self, func_body: str, func_name: str, has_kwargs: bool = False) -> str:
        """Адаптирует функцию для работы в Airflow"""
        return self.python_parser._adapt_function_for_airflow(func_body, func_name, has_kwargs)

    def generate_dag(self, file_path: Path, parsed_data: Dict):
        """Генерация DAG файла"""
        try:
            dag_name = f"generated_{file_path.stem}"
            dag_id = re.sub(r'[^a-zA-Z0-9_]', '_', dag_name)

            config = parsed_data['config']
            original_content = parsed_data.get('original_content', '')
            imports = parsed_data.get('imports', [])
            functions_info = parsed_data.get('functions_info', [])
            global_imports = parsed_data.get('global_imports', set())

            logger.info(f"Всего функций: {len(functions_info)}")

            # Значения по умолчанию
            default_values = {
                'schedule_interval': 'None',
                'catchup': 'False',
                'tags': json.dumps([f"{file_path.suffix[1:]}-auto"]),
                'owner': '"airflow"',
                'retries': '1',
                'retry_delay': 'timedelta(seconds=30)'
            }

            now = datetime.now()
            default_values['start_date'] = f'datetime({now.year}, {now.month}, {now.day})'

            for key, default_value in default_values.items():
                if key not in config:
                    config[key] = default_value

            if 'schedule_interval' in config:
                schedule_val = config['schedule_interval']
                if schedule_val not in ['None', 'null'] and not schedule_val.startswith('"'):
                    config['schedule_interval'] = f'"{schedule_val}"'

            is_python = file_path.suffix.lower() == '.py'
            is_sql = file_path.suffix.lower() == '.sql'

            # Базовые импорты
            imports_list = [
                "from airflow import DAG",
                "from airflow.operators.python import PythonOperator",
                "from datetime import datetime, timedelta",
                "import logging"
            ]

            # Добавляем дополнительные импорты для Python файлов
            if is_python:
                common_modules = {
                    'pandas': 'import pandas as pd',
                    'numpy': 'import numpy as np',
                    'json': 'import json',
                    'logging': 'import logging',
                    'sys': 'import sys',
                    'os': 'import os',
                    'pathlib': 'from pathlib import Path',
                    'datetime': 'from datetime import datetime, timedelta',
                    'typing': 'from typing import Dict, List, Optional, Any, Union'
                }

                content_lower = original_content.lower()
                for module_name, import_stmt in common_modules.items():
                    if module_name == 'logging':
                        continue
                    if module_name in content_lower and import_stmt not in imports_list:
                        imports_list.append(import_stmt)

                for imp in imports:
                    if imp not in imports_list and 'airflow' not in imp.lower():
                        imports_list.append(imp)

            # Для SQL файлов
            if is_sql:
                imports_list.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
                imports_list.append("from airflow.models import Variable")

            imports_list = self._remove_duplicate_imports('\n'.join(imports_list)).split('\n')

            dag_code = [
                '"""',
                f'Auto-generated DAG from {file_path.name}',
                f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                f'Total tasks: {len(parsed_data["tasks"])}',
                '"""',
                '',
                '\n'.join(imports_list),
                ''
            ]

            # Default arguments
            default_args_params = {}
            if 'owner' in config:
                default_args_params['owner'] = config['owner']

            default_args_params['depends_on_past'] = 'False'
            default_args_params['email_on_failure'] = 'False'
            default_args_params['email_on_retry'] = 'False'

            if 'retries' in config:
                default_args_params['retries'] = config['retries']

            if 'retry_delay' in config:
                default_args_params['retry_delay'] = config['retry_delay']

            if default_args_params:
                dag_code.append('# Default arguments for the DAG')
                dag_code.append('default_args = {')
                for key, value in default_args_params.items():
                    dag_code.append(f'    "{key}": {value},')
                dag_code.append('}')
                dag_code.append('')

            # Для Python файлов - встраиваем весь код
            if is_python and functions_info:

                sorted_functions = [f['name'] for f in functions_info]

                for func_name in sorted_functions:
                    func_info = next((f for f in functions_info if f['name'] == func_name), None)
                    if func_info:
                        dag_code.append(f'# Функция: {func_name}')

                        adapted_func = self._adapt_function_for_airflow(
                            func_info['code'],
                            func_name,
                            func_info.get('has_kwargs', False)
                        )

                        dag_code.extend(adapted_func.split('\n'))
                        dag_code.append('')

            # Для SQL файлов
            elif is_sql and parsed_data['tasks']:
                dag_code.append('# Функции для выполнения SQL команд')

                dag_code.extend([
                    'def get_postgres_connection_id():',
                    '    """',
                    '    Получает ID соединения из переменных Airflow',
                    '    Порядок приоритета:',
                    '    1. Переменная: airflow_db_conn_id (из DAG конфигурации)',
                    '    2. Значение по умолчанию из переменной Airflow',
                    '    """',
                    '    import os',
                    '    from airflow.models import Variable',
                    '    ',
                    '    conn_id = os.environ.get("AIRFLOW_DB_CONN_ID")',
                    '    if conn_id:',
                    '        return conn_id',
                    '    ',
                    '    try:',
                    '        conn_id = Variable.get("airflow_db_conn_id", default_var=None)',
                    '        if conn_id:',
                    '            return conn_id',
                    '    except:',
                    '        pass',
                    '    ',
                    '    return os.environ.get("POSTGRES_CONN_ID", "postgres_default")',
                    ''
                ])

                for task in parsed_data['tasks']:
                    sql = task['sql']
                    task_id = task['task_id']
                    function_name = task.get('function_name')

                    # ВАЖНО: Если function_name нет, генерируем его
                    if not function_name:
                        sql_hash = hashlib.md5(sql.lower().encode()).hexdigest()[:6]
                        clean_task_id = re.sub(r'[^a-zA-Z0-9_]', '_', task_id)
                        function_name = f"sql_fn_{clean_task_id}_{sql_hash}"
                        task['function_name'] = function_name  # Сохраняем обратно в задачу

                    sql_type = task.get('sql_type', 'SQL')
                    cmd_num = task.get('command_number', 0)

                    safe_sql = sql.replace('"""', '\\"\\"\\"')

                    dag_code.extend([
                        f'def {function_name}(**kwargs):',
                        f'    """Execute {sql_type} command {cmd_num}"""',
                        f'    logger = logging.getLogger(__name__)',
                        f'    logger.info("Starting {task_id}")',
                        f'    try:',
                        f'        conn_id = get_postgres_connection_id()',
                        f'        logger.info(f"Using PostgreSQL connection ID: {{conn_id}}")',
                        f'        ',
                        f'        hook = PostgresHook(postgres_conn_id=conn_id)',
                        f'        conn = hook.get_conn()',
                        f'        cursor = conn.cursor()',
                        f'        ',
                        f'        try:',
                        f'            cursor.execute("""{safe_sql}""")',
                        f'            conn.commit()',
                        f'            logger.info(f"Successfully executed: {sql_type}")',
                        f'        except Exception as e:',
                        f'            conn.rollback()',
                        f'            logger.error(f"Error executing {sql_type}: {{e}}")',
                        f'            raise',
                        f'        finally:',
                        f'            cursor.close()',
                        f'            conn.close()',
                        f'    except Exception as e:',
                        f'        logger.error(f"Error in {task_id}: {{e}}")',
                        f'        raise',
                        f'    finally:',
                        f'        logger.info("Completed {task_id}")',
                        f''
                    ])

            # Определение DAG
            dag_code.extend([
                f'with DAG(',
                f'    dag_id="{dag_id}",',
            ])

            if default_args_params:
                dag_code.append('    default_args=default_args,')

            dag_params = []

            if 'start_date' in config:
                dag_params.append(f'    start_date={config["start_date"]},')

            if 'schedule_interval' in config:
                dag_params.append(f'    schedule_interval={config["schedule_interval"]},')

            if 'catchup' in config:
                dag_params.append(f'    catchup={config["catchup"]},')

            if 'tags' in config:
                dag_params.append(f'    tags={config["tags"]},')

            additional_params = ['description', 'max_active_runs', 'concurrency', 'default_view']
            for param in additional_params:
                if param in config:
                    dag_params.append(f'    {param}={config[param]},')

            if dag_params:
                last_param = dag_params[-1]
                if last_param.endswith(','):
                    dag_params[-1] = last_param[:-1]

            dag_code.extend(dag_params)
            dag_code.append(') as dag:')
            dag_code.append('')

            # Создание задач
            task_ids = []
            task_mapping = {}

            if is_python and parsed_data['tasks']:
                dag_code.append('    # Python задачи:')

                analysis_result = self.python_parser._analyze_functions_for_strategy(functions_info)
                strategy = analysis_result['strategy']

                logger.info(f"=== ПРИМЕНЯЕМ СТРАТЕГИЮ: {strategy} ===")

                if strategy == 'multiple_tasks':
                    logger.info("Создаем отдельные задачи для каждой функции")

                    for task in parsed_data['tasks']:
                        func_name = task['name']
                        task_id = task['task_id']
                        description = task.get('description', f'Execute {func_name}')

                        logger.info(f"  Создаем задачу: {task_id} для функции {func_name}")

                        dag_code.extend([
                            f'    {task_id} = PythonOperator(',
                            f'        task_id="{task_id}",',
                            f'        python_callable={func_name},',
                            f'        doc_md="""{description}""",',
                            f'    )'
                        ])
                        task_ids.append(task_id)
                        task_mapping[func_name] = task_id
                        dag_code.append('')

                    # Определяем зависимости между задачами
                    if len(task_ids) > 1:
                        dependencies_found = False

                        for func_info in functions_info:
                            func_name = func_info['name']
                            deps = func_info.get('dependencies', [])
                            if deps:
                                dependencies_found = True
                                for dep in deps:
                                    if dep in task_mapping and func_name in task_mapping:
                                        upstream_task = task_mapping[dep]
                                        downstream_task = task_mapping[func_name]
                                        dag_code.append(f'    {upstream_task} >> {downstream_task}')

                        if not dependencies_found and len(task_ids) > 1:
                            dag_code.append('')
                            dag_code.append('    # Линейные зависимости (по умолчанию)')
                            dag_code.append(f'    {" >> ".join(task_ids)}')
                            logger.info(f"Созданы линейные зависимости: {' >> '.join(task_ids)}")

                else:
                    logger.info("Создаем одну задачу")

                    main_func_name = None

                    for func_info in functions_info:
                        if 'pipeline' in func_info['name'].lower():
                            main_func_name = func_info['name']
                            logger.info(f"Приоритет 1: найден пайплайн - {main_func_name}")
                            break

                    if not main_func_name:
                        for func_info in functions_info:
                            calls = func_info.get('calls', [])
                            other_functions = [f['name'] for f in functions_info if f['name'] != func_info['name']]
                            called_count = sum(1 for call in calls if call in other_functions)
                            if called_count >= 2:
                                main_func_name = func_info['name']
                                logger.info(
                                    f"Приоритет 2: главный пайплайн ({called_count} вызовов) - {main_func_name}")
                                break

                    if not main_func_name and functions_info:
                        max_lines = 0
                        for func_info in functions_info:
                            line_count = func_info['code'].count('\n') + 1
                            if line_count > max_lines:
                                max_lines = line_count
                                main_func_name = func_info['name']
                        logger.info(f"Приоритет 3: самая длинная функция ({max_lines} строк) - {main_func_name}")

                    if main_func_name:
                        main_task = next((t for t in parsed_data['tasks'] if t['name'] == main_func_name), None)

                        if main_task:
                            task_id = main_task['task_id']
                            description = main_task.get('description', f'Execute {main_func_name}')

                            logger.info(f"Создаем задачу: {task_id} для {main_func_name}")

                            dag_code.extend([
                                f'    {task_id} = PythonOperator(',
                                f'        task_id="{task_id}",',
                                f'        python_callable={main_func_name},',
                                f'        doc_md="""{description}""",',
                                f'    )'
                            ])
                            task_ids.append(task_id)
                            task_mapping[main_func_name] = task_id
                    else:
                        logger.warning("Не удалось найти функцию для создания задачи")

            elif is_sql and parsed_data['tasks']:
                dag_code.append('    # SQL задачи:')
                for task in parsed_data['tasks']:
                    task_id = task['task_id']
                    function_name = task.get('function_name')

                    # ВАЖНО: Если function_name нет, генерируем его
                    if not function_name:
                        sql_hash = hashlib.md5(task['sql'].lower().encode()).hexdigest()[:6]
                        clean_task_id = re.sub(r'[^a-zA-Z0-9_]', '_', task_id)
                        function_name = f"sql_fn_{clean_task_id}_{sql_hash}"

                    sql_type = task.get('sql_type', 'SQL')
                    cmd_num = task.get('command_number', 0)

                    dag_code.extend([
                        f'    {task_id} = PythonOperator(',
                        f'        task_id="{task_id}",',
                        f'        python_callable={function_name},',
                        f'        doc_md="""Execute {sql_type} command {cmd_num}""",',
                        f'    )'
                    ])
                    task_ids.append(task_id)

                # ВАЖНОЕ ДОБАВЛЕНИЕ: Добавляем линейные зависимости для SQL задач
                if task_ids and len(task_ids) > 1:
                    dag_code.append('')
                    dag_code.append('    # Линейные зависимости для последовательного выполнения')
                    dag_code.append(f'    {" >> ".join(task_ids)}')
                    logger.info(f"Созданы линейные зависимости для SQL задач: {' >> '.join(task_ids)}")

            # Сохранение DAG файла
            output_file = self.dags_dir / f"{dag_name}.py"
            output_file.write_text('\n'.join(dag_code), encoding='utf-8')

            logger.info(f"✓ Создан DAG с {len(parsed_data['tasks'])} задачами: {output_file.name}")
            logger.info(f"  Параметры DAG: schedule_interval={config.get('schedule_interval')}, "
                        f"tags={config.get('tags')}, retries={config.get('retries')}")

        except Exception as e:
            logger.error(f"✗ Ошибка генерации DAG для {file_path.name}: {str(e)}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")

    def run(self):
        """Основной метод запуска генератора"""
        logger.info(f"Сканирую директорию: {self.scripts_dir}")
        logger.info(f"Выходная директория: {self.dags_dir}")

        if not self.scripts_dir.exists():
            logger.error(f"Директория не существует: {self.scripts_dir}")
            return

        files_found = 0
        files_processed = 0

        for file_path in self.scripts_dir.iterdir():
            if file_path.suffix.lower() in ['.py', '.sql']:
                files_found += 1
                logger.info(f"\n{'=' * 50}")
                logger.info(f"Обрабатываю файл: {file_path.name}")
                logger.info(f"{'=' * 50}")

                try:
                    parsed_data = self.parse_file(file_path)

                    if parsed_data['tasks']:
                        self.generate_dag(file_path, parsed_data)
                        files_processed += 1
                        logger.info(f"✓ Файл {file_path.name} успешно обработан")
                    else:
                        logger.warning(f"⚠ В файле {file_path.name} не найдено задач")

                except Exception as e:
                    logger.error(f"✗ Ошибка обработки {file_path.name}: {str(e)}")
                    import traceback
                    logger.error(f"Трассировка: {traceback.format_exc()}")

        logger.info(f"\n{'=' * 50}")
        logger.info(f"Готово! Найдено файлов: {files_found}, обработано: {files_processed}")
        logger.info(f"{'=' * 50}")


if __name__ == "__main__":
    generator = MultiTaskDAGGenerator()
    generator.run()