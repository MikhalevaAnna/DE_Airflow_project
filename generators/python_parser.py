#!/usr/bin/env python3
"""
Парсер Python файлов для генератора DAG
"""

import re
import ast
import logging
from typing import Dict, List, Set, Any, Tuple, Optional

logger = logging.getLogger(__name__)


class PythonParser:
    """Класс для парсинга Python файлов"""

    def parse_python_file(self, content: str, module_name: str) -> Dict:
        """Парсинг Python файла для извлечения задач"""
        result = {
            'tasks': [],
            'imports': [],
            'functions_info': [],
            'global_imports': set()
        }

        try:
            parsed_data = self._parse_python_file_ast(content, module_name)
            result['tasks'] = parsed_data['tasks']
            result['imports'] = parsed_data['imports']
            result['functions_info'] = parsed_data['functions_info']
            result['global_imports'] = parsed_data['global_imports']
            logger.info(f"Найдено {len(result['tasks'])} функций")

        except Exception as e:
            logger.warning(f"Ошибка AST парсинга: {e}, используем простой парсинг")
            parsed_data = self._parse_python_file_simple(content, module_name)
            result['tasks'] = parsed_data['tasks']
            result['imports'] = parsed_data['imports']
            result['functions_info'] = parsed_data['functions_info']
            result['global_imports'] = parsed_data['global_imports']

        return result

    def _parse_python_file_ast(self, content: str, module_name: str) -> Dict:
        """Парсинг Python файла с использованием AST"""
        result = {
            'tasks': [],
            'imports': [],
            'functions_info': [],
            'global_imports': set()
        }

        try:
            tree = ast.parse(content)
            # Собираем все импорты
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        import_stmt = f"import {alias.name}"
                        if alias.asname:
                            import_stmt += f" as {alias.asname}"
                        result['imports'].append(import_stmt)
                        result['global_imports'].add(alias.name.split('.')[0])

                elif isinstance(node, ast.ImportFrom):
                    module = node.module or ''
                    names = []
                    for alias in node.names:
                        name = alias.name
                        if alias.asname:
                            name += f" as {alias.asname}"
                        names.append(name)

                    import_stmt = f"from {module} import {', '.join(names)}"
                    result['imports'].append(import_stmt)
                    result['global_imports'].add(module.split('.')[0] if module else names[0].split('.')[0])

            # Находим все функции
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    func_name = node.name

                    # Пропускаем приватные функции
                    if func_name.startswith('_'):
                        continue

                    # Получаем докстринг
                    docstring = ast.get_docstring(node) or ""

                    # Анализируем тело функции
                    start_line = node.lineno - 1
                    end_line = node.end_lineno if hasattr(node, 'end_lineno') else start_line

                    # Получаем исходный код функции
                    lines = content.split('\n')
                    func_code = '\n'.join(lines[start_line:end_line])

                    # Определяем, вызывает ли функция другие функции
                    calls = self._extract_function_calls(node)

                    # Анализируем аргументы функции
                    args = [arg.arg for arg in node.args.args]
                    has_args_param = any(arg in ['kwargs', '**kwargs'] for arg in args)
                    has_kwargs = has_args_param or node.args.kwarg is not None

                    # Сохраняем информацию о функции
                    func_info = {
                        'name': func_name,
                        'docstring': docstring,
                        'code': func_code,
                        'calls': calls,
                        'args': args,
                        'has_kwargs': has_kwargs,
                        'start_line': start_line,
                        'end_line': end_line,
                        'dependencies': []
                    }

                    result['functions_info'].append(func_info)

                    # Создаем задачу
                    result['tasks'].append({
                        'type': 'python_function',
                        'name': func_name,
                        'task_id': f"execute_{func_name}",
                        'description': docstring.split('\n')[0].strip() if docstring else f"Execute {func_name}",
                        'function_body': func_code,
                        'dependencies': [],
                        'has_kwargs': has_kwargs,
                        'module_name': module_name
                    })

            for func_info in result['functions_info']:
                func_name = func_info['name']
                func_code = func_info['code']

                # Ищем вызовы функций в теле функции
                call_pattern = r'(?<![\.\w])([a-zA-Z_][a-zA-Z0-9_]*)\s*\([^)]*\)'
                matches = re.findall(call_pattern, func_code)

                # Фильтруем результаты
                filtered_calls = []
                builtin_functions = ['print', 'len', 'str', 'int', 'float', 'list', 'dict',
                                     'set', 'tuple', 'range', 'enumerate', 'zip', 'map',
                                     'filter', 'isinstance', 'type', 'pd', 'np', 'json',
                                     'datetime', 'timedelta', 'Path', 'hashlib', 'logger',
                                     'logging', 'os', 'sys', 're', 'random', 'math']

                for call in matches:
                    if (call != func_name and
                            call not in builtin_functions and
                            call not in filtered_calls):
                        filtered_calls.append(call)

                # Добавляем найденные вызовы к AST результатам
                if filtered_calls:
                    logger.info(f"Regex анализ для {func_name}: {filtered_calls}")
                    existing_calls = func_info.get('calls', [])
                    all_calls = list(set(existing_calls + filtered_calls))
                    func_info['calls'] = all_calls

            # Логируем итоговые вызовы
            logger.info("Итоговые вызовы функций:")
            for func_info in result['functions_info']:
                logger.info(f"  {func_info['name']} вызывает: {func_info.get('calls', [])}")
            # Ищем явные зависимости в комментариях
            explicit_dependencies = self._check_explicit_dependencies(content)
            if explicit_dependencies:
                logger.info(f"Найдены явные зависимости в комментариях: {explicit_dependencies}")

            # Обновляем зависимости для функций
            for dep_from, dep_to in explicit_dependencies:
                # Находим функции и добавляем зависимости
                for func_info in result['functions_info']:
                    if func_info['name'] == dep_to:
                        if dep_from not in func_info['dependencies']:
                            func_info['dependencies'].append(dep_from)

        except Exception as e:
            logger.warning(f"Ошибка AST парсинга: {e}")
            raise

        return result

    def _parse_python_file_simple(self, content: str, module_name: str) -> Dict:
        """Простой парсинг Python файла через regex"""
        result = {
            'tasks': [],
            'imports': [],
            'functions_info': [],
            'global_imports': set()
        }

        # Извлекаем импорты
        import_lines = re.findall(r'^(?:from\s+\S+\s+import\s+\S+|import\s+\S+)', content, re.MULTILINE)
        for line in import_lines:
            result['imports'].append(line.strip())
            # Извлекаем имя модуля
            if line.startswith('from '):
                module = line.split()[1]
                result['global_imports'].add(module.split('.')[0])
            else:
                module = line.split()[1].split('.')[0]
                result['global_imports'].add(module)

        # Извлекаем функции
        function_pattern = r'def\s+(\w+)\s*\(([^)]*)\)\s*:\s*(?:"""([^"]*)""")?(.*?)(?=\n\s*def|\n\s*$|\Z)'
        matches = re.finditer(function_pattern, content, re.DOTALL)

        for match in matches:
            func_name = match.group(1)

            # Пропускаем приватные функции
            if func_name.startswith('_'):
                continue

            args_str = match.group(2)
            docstring = match.group(3) or ""
            func_body = f"def {func_name}({args_str}):{match.group(4)}"

            # Определяем, есть ли **kwargs
            has_kwargs = '**kwargs' in args_str or '*args, **kwargs' in args_str

            # Определяем вызовы других функций
            calls = re.findall(r'(\w+)\s*\(', func_body)
            calls = [c for c in calls if c != func_name and not c.startswith('_')]

            result['functions_info'].append({
                'name': func_name,
                'docstring': docstring,
                'code': func_body,
                'calls': calls,
                'has_kwargs': has_kwargs,
                'dependencies': []
            })

            result['tasks'].append({
                'type': 'python_function',
                'name': func_name,
                'task_id': f"execute_{func_name}",
                'description': docstring.split('\n')[0].strip() if docstring else f"Execute {func_name}",
                'function_body': func_body,
                'dependencies': [],
                'has_kwargs': has_kwargs,
                'module_name': module_name
            })

        return result

    def _extract_function_calls(self, func_node: ast.FunctionDef) -> List[str]:
        """Извлекает вызовы функций из AST узла"""
        calls = []

        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.append(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name):
                        calls.append(f"{node.func.value.id}.{node.func.attr}")

        return calls

    def _check_explicit_dependencies(self, content: str) -> List[Tuple[str, str]]:
        """Проверяет явные зависимости задач в комментариях"""
        dependencies = []

        # Паттерн для зависимостей в комментариях
        dependency_pattern = r'#.*?([a-zA-Z_][a-zA-Z0-9_]*)\s*>>\s*([a-zA-Z_][a-zA-Z0-9_]*)'

        lines = content.split('\n')
        for line in lines:
            if '>>' in line and line.strip().startswith('#'):
                # Убираем комментарий и ищем зависимости
                clean_line = line.strip('# ').strip()
                if '>>' in clean_line:
                    parts = clean_line.split('>>')
                    parts = [p.strip() for p in parts if p.strip()]

                    # Создаем пары зависимостей
                    for i in range(len(parts) - 1):
                        dependencies.append((parts[i], parts[i + 1]))

        return dependencies

    def _fix_indentation(self, code: str) -> str:
        """Исправляет отступы в коде"""
        lines = code.split('\n')
        fixed_lines = []

        for line in lines:
            stripped = line.lstrip()
            if stripped:
                indent_match = re.match(r'^(\s+)', line)
                if indent_match:
                    indent = indent_match.group(1)
                    indent_len = len(indent)
                    if indent_len % 4 != 0:
                        indent = ' ' * ((indent_len // 4) * 4)
                    fixed_lines.append(indent + stripped)
                else:
                    fixed_lines.append(stripped)
            else:
                fixed_lines.append('')

        return '\n'.join(fixed_lines)

    def _fix_duplicate_exception_blocks(self, code: str) -> str:
        """Исправляет вложенные except блоки"""
        lines = code.split('\n')
        fixed_lines = []
        in_except_block = False
        except_level = 0

        for line in lines:
            stripped = line.strip()

            current_level = len(line) - len(line.lstrip())

            if stripped.startswith('except '):
                if in_except_block and current_level > except_level:
                    continue
                else:
                    in_except_block = True
                    except_level = current_level
            elif stripped.startswith('finally:'):
                in_except_block = False
            elif not stripped.startswith('except') and not stripped.startswith('finally'):
                if current_level <= except_level:
                    in_except_block = False

            fixed_lines.append(line)

        return '\n'.join(fixed_lines)

    def _remove_duplicate_imports(self, code: str) -> str:
        """Убирает дублирующиеся импорты"""
        lines = code.split('\n')
        fixed_lines = []
        import_lines = []
        other_lines = []

        for line in lines:
            stripped = line.strip()
            if stripped.startswith('import ') or stripped.startswith('from '):
                import_lines.append(line)
            else:
                other_lines.append(line)

        unique_imports = []
        seen_imports = set()

        for imp in import_lines:
            imp_stripped = imp.strip()
            if imp_stripped.startswith('from '):
                key = imp_stripped.split(' import ')[0]
            else:
                key = imp_stripped

            if key not in seen_imports:
                seen_imports.add(key)
                unique_imports.append(imp)

        fixed_lines = unique_imports + other_lines
        return '\n'.join(fixed_lines)

    def _fix_error_messages(self, line: str, func_name: str) -> str:
        """Исправляет f-строки с ошибками в сообщениях об ошибках"""
        if 'Error in' in line and '{func_name}' in line:
            line = line.replace('{func_name}', func_name)
        return line

    def _adapt_function_for_airflow(self, func_body: str, func_name: str, has_kwargs: bool = False) -> str:
        """Адаптирует функцию для работы в Airflow"""
        func_body = self._fix_indentation(func_body)
        func_body = self._fix_duplicate_exception_blocks(func_body)

        lines = func_body.strip().split('\n')
        adapted_lines = []
        in_function = False
        has_try_block = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            if i == 0 and stripped.startswith(f'def {func_name}'):
                in_function = True

                if not has_kwargs and '**kwargs' not in line:
                    if '):' in line:
                        base_line = line.rstrip(' ):')
                        if base_line.endswith('('):
                            line = base_line + '**kwargs):'
                        else:
                            line = base_line + ', **kwargs):'
                    elif line.endswith('()'):
                        line = line[:-2] + '**kwargs):'

                adapted_lines.append(line)
                adapted_lines.append('    """Функция, адаптированная для Airflow"""')
                adapted_lines.append('    logger = logging.getLogger(__name__)')
                adapted_lines.append(f'    logger.info("Starting {func_name}")')

                has_inner_try = any('try:' in l.strip() for l in lines[i + 1:])
                has_inner_except = any('except' in l.strip() for l in lines[i + 1:])

                if not (has_inner_try or has_inner_except):
                    adapted_lines.append('    try:')
                    has_try_block = True

            elif in_function:
                original_indent = len(line) - len(line.lstrip())

                if stripped:
                    if has_try_block:
                        new_line = ' ' * (original_indent + 4) + line.lstrip()
                    else:
                        new_line = ' ' * original_indent + line.lstrip()

                    new_line = self._fix_error_messages(new_line, func_name)
                    new_line = new_line.replace('print(', 'logger.info(')

                    adapted_lines.append(new_line)
                elif original_indent > 0:
                    if has_try_block:
                        adapted_lines.append(' ' * (original_indent + 4))
                    else:
                        adapted_lines.append(' ' * original_indent)
                else:
                    adapted_lines.append('')
            else:
                adapted_lines.append(line)

        if in_function and has_try_block:
            adapted_lines.append('    except Exception as e:')
            adapted_lines.append(f'        logger.error(f"Error in {func_name}: {{e}}")')
            adapted_lines.append('        raise')
            adapted_lines.append('    finally:')
            adapted_lines.append(f'        logger.info("Completed {func_name}")')

        return '\n'.join(adapted_lines)

    def _analyze_functions_for_strategy(self, functions_info: List[Dict]) -> Dict[str, Any]:
        """Анализирует функции и определяет стратегию генерации"""
        logger.info(f"=== АНАЛИЗ ФУНКЦИЙ ДЛЯ СТРАТЕГИИ ГЕНЕРАЦИИ ===")
        logger.info(f"Всего функций: {len(functions_info)}")

        has_parameters = False
        calls_other_functions = False

        for func_info in functions_info:
            func_name = func_info['name']

            args = func_info.get('args', [])
            regular_args = [arg for arg in args if arg not in ['kwargs', '**kwargs']]
            if regular_args:
                has_parameters = True
                logger.info(f"  {func_name} имеет параметры: {regular_args}")

            calls = func_info.get('calls', [])
            other_functions_in_file = [f['name'] for f in functions_info if f['name'] != func_name]
            called_other_funcs = [call for call in calls if call in other_functions_in_file]
            if called_other_funcs:
                calls_other_functions = True
                logger.info(f"  {func_name} вызывает другие функции: {called_other_funcs}")

        if len(functions_info) <= 1:
            strategy = 'single_task'
            logger.info(f"СТРАТЕГИЯ: ОДНА ЗАДАЧА (только одна функция)")
        elif has_parameters:
            strategy = 'single_task'
            logger.info(f"СТРАТЕГИЯ: ОДНА ЗАДАЧА (есть функции с параметрами)")
        elif calls_other_functions:
            strategy = 'single_task'
            logger.info(f"СТРАТЕГИЯ: ОДНА ЗАДАЧА (функции вызывают друг друга)")
        else:
            strategy = 'multiple_tasks'
            logger.info(f"СТРАТЕГИЯ: МНОГО ЗАДАЧ (независимые функции без параметров)")

        return {
            'strategy': strategy,
            'has_parameters': has_parameters,
            'calls_other_functions': calls_other_functions
        }

    def _analyze_dependencies(self, functions_info: List[Dict]) -> Dict[str, List[str]]:
        """Анализирует зависимости между функциями"""
        func_map = {f['name']: f for f in functions_info}
        dependencies = {}

        all_calls = {}
        for func_info in functions_info:
            func_name = func_info['name']
            calls = func_info.get('calls', [])
            all_calls[func_name] = calls

        pipeline_functions = []
        for func_name, calls in all_calls.items():
            if calls:
                pipeline_functions.append(func_name)

        for func_info in functions_info:
            func_name = func_info['name']
            deps = []

            if func_name in pipeline_functions:
                for call in all_calls[func_name]:
                    if call in func_map and call != func_name:
                        deps.append(call)

            dependencies[func_name] = deps

        for func_info in functions_info:
            func_name = func_info['name']
            if func_name not in dependencies:
                dependencies[func_name] = []

        return dependencies

    def _topological_sort(self, dependencies: Dict[str, List[str]]) -> List[str]:
        """Топологическая сортировка функций по зависимостям"""
        result = []
        visited = set()
        temp = set()

        def visit(node):
            if node in temp:
                raise ValueError(f"Циклическая зависимость обнаружена: {node}")
            if node not in visited:
                temp.add(node)
                for dep in dependencies.get(node, []):
                    visit(dep)
                temp.remove(node)
                visited.add(node)
                result.append(node)

        for node in dependencies.keys():
            if node not in visited:
                visit(node)

        return result

    def _find_main_pipeline(self, functions_info: List[Dict]) -> Optional[str]:
        """Находит главную функцию-пайплайн"""
        for func_info in functions_info:
            if 'pipeline' in func_info['name'].lower():
                return func_info['name']

        for func_info in functions_info:
            calls = func_info.get('calls', [])
            if len(calls) > 2:
                return func_info['name']

        if functions_info:
            return functions_info[0]['name']

        return None