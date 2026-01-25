#!/usr/bin/env python3
"""
Парсер SQL файлов для генератора DAG
"""

import re
import hashlib
import logging
from typing import Dict, List, Set, Any, Optional

logger = logging.getLogger(__name__)


class SQLParser:
    """Класс для парсинга SQL файлов"""

    def parse_sql_file(self, content: str) -> Dict:
        """Парсинг SQL файла для извлечения команд"""
        result = {'tasks': []}

        statements = self._parse_sql_statements_improved(content)
        logger.info(f"Найдено SQL statements: {len(statements)}")

        # Словарь для отслеживания дубликатов
        seen_task_ids = set()
        command_counter = {}

        for i, sql in enumerate(statements, 1):
            sql_clean = sql.strip()
            if not sql_clean:
                continue

            if sql_clean.endswith(';'):
                sql_clean = sql_clean[:-1].strip()

            if sql_clean.startswith('--'):
                continue

            sql_type = self._get_sql_type(sql_clean)

            # Генерируем базовый task_id (ДОБАВЛЯЕМ НОМЕР)
            base_task_id = self._generate_base_task_id(i, sql_type, sql_clean)

            # Делаем task_id уникальным
            task_id = self._make_task_id_unique(base_task_id, seen_task_ids, command_counter)
            seen_task_ids.add(task_id)

            # Генерируем уникальное имя функции
            function_name = self._generate_function_name(task_id, sql_clean)

            result['tasks'].append({
                'type': 'sql_command',
                'sql': sql_clean,
                'task_id': task_id,
                'function_name': function_name,  # Добавляем уникальное имя функции
                'description': f"Execute {sql_type} command {i}",
                'sql_type': sql_type,
                'command_number': i,
                'original_sql_hash': hashlib.md5(sql_clean.encode()).hexdigest()[:8]
            })

        return result

    def _generate_function_name(self, task_id: str, sql: str) -> str:
        """Генерирует уникальное имя функции на основе task_id и хеша SQL"""
        sql_hash = hashlib.md5(sql.lower().encode()).hexdigest()[:6]
        # Убираем любые недопустимые символы из task_id для имени функции
        clean_task_id = re.sub(r'[^a-zA-Z0-9_]', '_', task_id)
        return f"sql_fn_{clean_task_id}_{sql_hash}"

    def _generate_base_task_id(self, num: int, sql_type: str, sql: str) -> str:
        """Генерирует базовый task_id для SQL команды (С НОМЕРОМ)"""
        sql_lower = sql.lower()

        # Для CREATE TABLE извлекаем имя таблицы
        if sql_type == 'CREATE TABLE':
            table_match = re.search(r'create\s+table\s+(?:if\s+not\s+exists\s+)?["\']?(\w+)[\'"]?',
                                    sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"create_table_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для DROP TABLE извлекаем имя таблицы
        elif sql_type == 'DROP TABLE':
            table_match = re.search(r'drop\s+table\s+(?:if\s+exists\s+)?["\']?(\w+)[\'"]?',
                                    sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"drop_table_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для CREATE INDEX извлекаем имя индекса
        elif sql_type == 'CREATE INDEX':
            index_match = re.search(r'create\s+index\s+(?:if\s+not\s+exists\s+)?["\']?(\w+)[\'"]?',
                                    sql_lower[:200])
            if index_match:
                index_name = index_match.group(1).lower()
                return f"create_index_{index_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для DROP INDEX извлекаем имя индекса
        elif sql_type == 'DROP INDEX':
            index_match = re.search(r'drop\s+index\s+(?:if\s+exists\s+)?["\']?(\w+)[\'"]?',
                                    sql_lower[:200])
            if index_match:
                index_name = index_match.group(1).lower()
                return f"drop_index_{index_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для INSERT INTO извлекаем имя таблицы
        elif sql_type == 'INSERT':
            table_match = re.search(r'insert\s+into\s+["\']?(\w+)[\'"]?', sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"insert_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для UPDATE извлекаем имя таблицы
        elif sql_type == 'UPDATE':
            table_match = re.search(r'update\s+["\']?(\w+)[\'"]?', sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"update_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для DELETE FROM извлекаем имя таблицы
        elif sql_type == 'DELETE':
            table_match = re.search(r'delete\s+(?:from\s+)?["\']?(\w+)[\'"]?', sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"delete_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для ALTER TABLE извлекаем имя таблицы
        elif sql_type == 'ALTER TABLE':
            table_match = re.search(r'alter\s+table\s+["\']?(\w+)[\'"]?', sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"alter_table_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # Для CTE используем номер
        elif sql_type == 'CTE':
            return f"cte_query_{num}"

        # Для SELECT извлекаем основную таблицу
        elif sql_type == 'SELECT':
            table_match = re.search(r'from\s+["\']?(\w+)[\'"]?', sql_lower[:200])
            if table_match:
                table_name = table_match.group(1).lower()
                return f"select_{table_name}_{num}"  # ДОБАВЛЯЕМ НОМЕР

        # По умолчанию используем тип и номер
        return f"{sql_type.lower().replace(' ', '_')}_{num}"

    def _make_task_id_unique(self, base_task_id: str, seen_task_ids: set, command_counter: dict) -> str:
        """Делает task_id уникальным"""
        if base_task_id not in seen_task_ids:
            command_counter[base_task_id] = 1
            return base_task_id

        # Если task_id уже существует, добавляем суффикс
        if base_task_id in command_counter:
            command_counter[base_task_id] += 1
        else:
            command_counter[base_task_id] = 2

        return f"{base_task_id}_{command_counter[base_task_id]}"

    def _parse_sql_statements_improved(self, content: str) -> List[str]:
        """Улучшенный парсер SQL команд для многострочных выражений"""
        statements = []
        current = []
        in_string = False
        string_char = None
        in_comment = False

        lines = content.split('\n')

        for line_num, line in enumerate(lines):
            i = 0
            while i < len(line):
                char = line[i]

                if not in_string:
                    if char == '-' and i + 1 < len(line) and line[i + 1] == '-':
                        break
                    elif char == '/' and i + 1 < len(line) and line[i + 1] == '*':
                        in_comment = True
                        i += 2
                        continue
                    elif char == '*' and i + 1 < len(line) and line[i + 1] == '/' and in_comment:
                        in_comment = False
                        i += 2
                        continue

                if in_comment:
                    i += 1
                    continue

                if char in ("'", '"'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif in_string and string_char == char:
                        if i == 0 or line[i - 1] != '\\':
                            in_string = False
                            string_char = None

                current.append(char)

                if not in_string:
                    if char == ';':
                        stmt = ''.join(current).strip()
                        if stmt:
                            statements.append(stmt)
                        current = []

                i += 1

            if current and not in_string:
                current.append('\n')

        if current:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)

        filtered = []
        for stmt in statements:
            stmt_clean = stmt.strip()
            if stmt_clean and not stmt_clean.startswith('--'):
                filtered.append(stmt_clean)

        return filtered

    def _get_sql_type(self, sql: str) -> str:
        """Определяет тип SQL команды"""
        sql_upper = sql[:100].upper()

        # ВАЖНО: Проверяем в правильном порядке!
        # Сначала CREATE INDEX, потом CREATE TABLE
        if 'CREATE INDEX' in sql_upper:
            return 'CREATE INDEX'
        elif 'DROP INDEX' in sql_upper:
            return 'DROP INDEX'
        elif 'CREATE TABLE' in sql_upper:
            return 'CREATE TABLE'
        elif 'DROP TABLE' in sql_upper:
            return 'DROP TABLE'
        elif 'INSERT INTO' in sql_upper:
            return 'INSERT'
        elif 'UPDATE' in sql_upper:
            return 'UPDATE'
        elif 'DELETE FROM' in sql_upper or (sql_upper.startswith('DELETE') and 'FROM' not in sql_upper[:20]):
            return 'DELETE'
        elif 'ALTER TABLE' in sql_upper:
            return 'ALTER TABLE'
        elif 'ALTER SEQUENCE' in sql_upper:
            return 'ALTER SEQUENCE'
        elif 'TRUNCATE TABLE' in sql_upper or sql_upper.startswith('TRUNCATE'):
            return 'TRUNCATE'
        elif 'WITH' in sql_upper:
            return 'CTE'
        elif 'SELECT' in sql_upper:
            return 'SELECT'
        else:
            return 'SQL'