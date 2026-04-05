#!/usr/bin/env python3
"""Автоматическое преобразование neo4j.execute(f"...") в execute_labeled().

Преобразует только те случаи, где паттерн полностью совпадает:
- Labels (user_label, post_label, etc.) -> label_map
- Graph names -> ident_map  
- Значения -> params
"""

import ast
import sys
from pathlib import Path
from typing import ClassVar, TypedDict


class ChangeInfo(TypedDict):
    line: int
    original: str
    transformed: str


class SkipInfo(TypedDict):
    line: int
    reason: str
    code: str


class TransformResult(TypedDict, total=False):
    success: bool
    changes: int
    skipped: int
    changes_made: list[ChangeInfo]
    skipped_details: list[SkipInfo]
    error: str


class Neo4jExecuteTransformer(ast.NodeTransformer):
    """AST transformer для преобразования neo4j.execute(f"...") в execute_labeled()."""
    
    # Паттерны labels, которые можно безопасно преобразовать
    LABEL_PATTERNS: ClassVar[dict[str, str]] = {
        'user_label': 'user',
        'post_label': 'post', 
        'user_community_label': 'uc',
        'post_community_label': 'pc',
    }
    
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.changes_made: list[ChangeInfo] = []
        self.skipped: list[SkipInfo] = []
        # Маппинг переменных на базовые labels из вызовов neo4j.label()
        self.label_var_to_base: dict[str, str] = {}
        
    def visit_Assign(self, node: ast.Assign) -> ast.AST:
        """Обрабатывает присваивания для сбора соответствий label переменных."""
        # Ищем паттерн: var = neo4j.label('BaseLabel')
        if (len(node.targets) == 1 and 
            isinstance(node.targets[0], ast.Name) and
            isinstance(node.value, ast.Call) and
            isinstance(node.value.func, ast.Attribute) and
            isinstance(node.value.func.value, ast.Name) and
            node.value.func.value.id == 'neo4j' and
            node.value.func.attr == 'label' and
            node.value.args and
            isinstance(node.value.args[0], ast.Constant)):
            var_name = node.targets[0].id
            base_label = node.value.args[0].value
            if isinstance(base_label, str):
                self.label_var_to_base[var_name] = base_label
        return self.generic_visit(node)
    
    def visit_Call(self, node: ast.Call) -> ast.AST:
        """Обрабатывает вызовы neo4j.execute(f"...")."""
        # Сначала проверяем прямой вызов neo4j.execute(...)
        if self._is_neo4j_execute(node):
            # Проверяем, что первый аргумент - f-строка
            if node.args and isinstance(node.args[0], ast.JoinedStr):
                f_string = node.args[0]
                # Пытаемся преобразовать
                result = self._transform_to_execute_labeled(node, f_string)
                if result:
                    self.changes_made.append(ChangeInfo(
                        line=node.lineno,
                        original=self._unparse_safe(node),
                        transformed=self._unparse_safe(result),
                    ))
                    return result
                else:
                    self.skipped.append(SkipInfo(
                        line=node.lineno,
                        reason='Pattern not fully matchable',
                        code=self._unparse_safe(node)[:100],
                    ))
        
        # Проверяем случаи, где вызов обернут в другие функции (list(), iter(), etc.)
        # Например: list(neo4j.execute_and_fetch(f"..."))
        if isinstance(node.func, ast.Name) and node.func.id in ('list', 'iter', 'tuple', 'set'):
            if node.args and isinstance(node.args[0], ast.Call):
                inner_call = node.args[0]
                if self._is_neo4j_execute(inner_call):
                    if inner_call.args and isinstance(inner_call.args[0], ast.JoinedStr):
                        f_string = inner_call.args[0]
                        result = self._transform_to_execute_labeled(inner_call, f_string)
                        if result:
                            # Создаем новый вызов с оберткой
                            new_wrapped = ast.Call(
                                func=node.func,
                                args=[result],
                                keywords=node.keywords,
                            )
                            self.changes_made.append(ChangeInfo(
                                line=node.lineno,
                                original=self._unparse_safe(node),
                                transformed=self._unparse_safe(new_wrapped),
                            ))
                            return new_wrapped
        
        return self.generic_visit(node)
    
    def _is_neo4j_execute(self, node: ast.Call) -> bool:
        """Проверяет, является ли вызов neo4j.execute или neo4j.execute_and_fetch."""
        if not isinstance(node.func, ast.Attribute):
            return False
        if not isinstance(node.func.value, ast.Name):
            return False
        if node.func.value.id != 'neo4j':
            return False
        return node.func.attr in ('execute', 'execute_and_fetch', 'stream_query')
    
    def _transform_to_execute_labeled(
        self, 
        node: ast.Call, 
        f_string: ast.JoinedStr
    ) -> ast.Call | None:
        """Преобразует f-строку в execute_labeled с label_map, ident_map и params."""
        # Парсим f-строку на части
        parts: list[tuple[str, str | None]] = []
        for part in f_string.values:
            if isinstance(part, ast.Constant):
                if isinstance(part.value, str):
                    parts.append((part.value, None))
                else:
                    parts.append(('', None))
            elif isinstance(part, ast.FormattedValue):
                # Извлекаем имя переменной из FormattedValue
                var_name = self._extract_var_name(part.value)
                if var_name:
                    parts.append(('', var_name))
                else:
                    return None  # Не можем обработать сложные выражения
        
        # Собираем label_map, ident_map и params
        label_map_vars: dict[str, str] = {}
        ident_map_vars: dict[str, str] = {}
        params_vars: dict[str, str] = {}
        template_parts: list[str] = []
        
        for text, var_name in parts:
            if var_name is None:
                template_parts.append(text)
            else:
                if var_name in self.LABEL_PATTERNS:
                    key = self.LABEL_PATTERNS[var_name]
                    template_parts.append(f"__{key}__")
                    if key not in label_map_vars:
                        # Use base label if available from assignment analysis
                        label_map_vars[key] = self.label_var_to_base.get(var_name, var_name)
                elif var_name == 'graph_name' or var_name.endswith('_name') or var_name.endswith('_emb'):
                    key = var_name.replace('_name', '').replace('_emb', '')
                    if key == 'graph':
                        key = 'graph_name'
                    template_parts.append(f"__{key}__")
                    ident_map_vars[key] = var_name
                elif var_name.isupper():
                    # Constants are treated as parameters
                    template_parts.append(f"${{{var_name}}}")
                    params_vars[var_name] = var_name
                else:
                    template_parts.append(f"${{{var_name}}}")
                    params_vars[var_name] = var_name
        
        # Собираем финальный шаблон
        template = ''.join(template_parts)
        
        # Создаем новый вызов execute_labeled или execute_and_fetch_labeled
        method_name = 'execute_labeled'
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'execute_and_fetch':
            method_name = 'execute_and_fetch_labeled'
        elif isinstance(node.func, ast.Attribute) and node.func.attr == 'stream_query':
            method_name = 'stream_query_labeled'
        
        # Создаем аргументы для нового вызова
        new_args: list[ast.expr] = [ast.Constant(value=template)]
        new_keywords: list[ast.keyword] = []
        
        # Добавляем label_map если есть
        if label_map_vars:
            # Используем базовый label из вызова neo4j.label(), если известен
            label_map_values: list[ast.expr] = []
            for var_name in label_map_vars.values():
                if var_name in self.label_var_to_base:
                    # Используем базовый label (строку) вместо переменной
                    base_label: str = self.label_var_to_base[var_name]
                    label_map_values.append(ast.Constant(value=base_label))
                else:
                    # Используем переменную как есть (fallback)
                    label_map_values.append(ast.Name(id=var_name, ctx=ast.Load()))
            label_map_dict = ast.Dict(
                keys=[ast.Constant(value=k) for k in label_map_vars.keys()],
                values=label_map_values,
            )
            new_keywords.append(ast.keyword(arg='label_map', value=label_map_dict))
        
        # Добавляем ident_map если есть
        if ident_map_vars:
            ident_map_dict = ast.Dict(
                keys=[ast.Constant(value=k) for k in ident_map_vars.keys()],
                values=[ast.Name(id=v, ctx=ast.Load()) for v in ident_map_vars.values()],
            )
            new_keywords.append(ast.keyword(arg='ident_map', value=ident_map_dict))
        
        # Добавляем params если есть (или если были в оригинале)
        if params_vars:
            # Собираем params из переменных в f-строке
            params_dict = ast.Dict(
                keys=[ast.Constant(value=k) for k in params_vars.keys()],
                values=[ast.Name(id=v, ctx=ast.Load()) for v in params_vars.values()],
            )
            new_keywords.append(ast.keyword(arg='params', value=params_dict))
        elif len(node.args) > 1:
            # Если в оригинале были params, сохраняем их
            new_keywords.append(ast.keyword(arg='params', value=node.args[1]))
        
        # Определяем имя метода (execute или execute_and_fetch)
        if not isinstance(node.func, ast.Attribute):
            return None
        
        new_func = ast.Attribute(
            value=ast.Name(id='neo4j', ctx=ast.Load()),
            attr=method_name,
            ctx=ast.Load(),
        )
        
        return ast.Call(
            func=new_func,
            args=new_args,
            keywords=new_keywords,
        )
    
    def _extract_var_name(self, node: ast.AST) -> str | None:
        """Извлекает имя переменной из AST узла."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # Для случаев типа neo4j.match_all_nodes()
            if (isinstance(node.value, ast.Name) and 
                node.value.id == 'neo4j' and 
                node.attr == 'match_all_nodes'):
                return '__match_all__'
        return None
    
    def _unparse_safe(self, node: ast.AST) -> str:
        """Безопасный unparse с fallback."""
        try:
            return ast.unparse(node)
        except AttributeError:
            # Для Python < 3.9
            try:
                import astor
                result = astor.to_source(node)
                return result.strip()
            except ImportError:
                return str(node)


def transform_file(file_path: Path, dry_run: bool = False) -> TransformResult:
    """Преобразует файл, возвращает статистику."""
    content = file_path.read_text(encoding='utf-8')
    
    try:
        tree = ast.parse(content, filename=str(file_path))
    except SyntaxError as e:
        return TransformResult(
            success=False,
            error=f'Syntax error: {e}',
            changes=0,
            skipped=0,
            changes_made=[],
            skipped_details=[],
        )
    
    transformer = Neo4jExecuteTransformer(file_path)
    new_tree = transformer.visit(tree)
    
    if not transformer.changes_made:
        return TransformResult(
            success=True,
            changes=0,
            skipped=len(transformer.skipped),
            changes_made=[],
            skipped_details=transformer.skipped,
            error='',
        )
    
    if not dry_run:
        # Форматируем код обратно
        try:
            new_content = ast.unparse(new_tree)
        except AttributeError:
            # Fallback для Python < 3.9
            try:
                import astor
                new_content = astor.to_source(new_tree)
            except ImportError:
                # If astor is not available, use str representation
                new_content = str(new_tree)
        
        file_path.write_text(new_content, encoding='utf-8')
    
    return TransformResult(
        success=True,
        changes=len(transformer.changes_made),
        skipped=len(transformer.skipped),
        changes_made=transformer.changes_made,
        skipped_details=transformer.skipped,
        error='',
    )


def main() -> None:
    """Точка входа для CLI."""
    if len(sys.argv) < 2:
        print("Usage: transform_neo4j_execute.py <file_path> [--dry-run]")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    dry_run = '--dry-run' in sys.argv
    
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    print(f"Processing: {file_path}")
    if dry_run:
        print("DRY RUN MODE - no changes will be made")
    
    result = transform_file(file_path, dry_run=dry_run)
    
    success = result.get('success', False)
    if not success:
        error_msg = result.get('error', 'Unknown error')
        print(f"Error: {error_msg}")
        sys.exit(1)
    
    print("\nResults:")
    changes = result.get('changes', 0)
    skipped = result.get('skipped', 0)
    print(f"  Changes made: {changes}")
    print(f"  Skipped: {skipped}")
    
    changes_details = result.get('changes_made', [])
    if changes_details:
        print("\nChanges:")
        for change in changes_details:
            line = change.get('line', 0)
            original = change.get('original', '')[:120]
            transformed = change.get('transformed', '')[:120]
            print(f"  Line {line}:")
            print(f"    Original: {original}...")
            print(f"    Transformed: {transformed}...")
    
    skipped_details = result.get('skipped_details', [])
    if skipped_details:
        print("\nSkipped (pattern not matchable):")
        for skip in skipped_details[:5]:  # Показываем первые 5
            line = skip.get('line', 0)
            reason = skip.get('reason', '')
            code = skip.get('code', '')
            print(f"  Line {line}: {reason}")
            print(f"    {code}...")
    

if __name__ == '__main__':
    main()
