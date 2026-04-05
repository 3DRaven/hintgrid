#!/usr/bin/env python3
"""AST-based автоматическое исправление типовых ошибок типизации в тестах.

Исправляет:
1. Вызовы merge_* функций с list[dict[str, object]] -> convert_batch_decimals
2. dict[str, object] в параметрах Neo4j -> конвертация
"""

import ast
import sys
from pathlib import Path
from typing import ClassVar


class TestTypeFixer(ast.NodeTransformer):
    """AST transformer для исправления типовых ошибок типизации в тестах."""
    
    # Функции, которые требуют convert_batch_decimals
    MERGE_FUNCTIONS: ClassVar[set[str]] = {
        'merge_posts',
        'merge_favourites',
        'merge_blocks',
        'merge_mutes',
        'merge_reblogs',
        'merge_replies',
        'merge_bookmarks',
        'merge_interactions',
        'update_user_activity',
        'merge_status_stats',
    }
    
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.changes_made: int = 0
        self.imports_added: bool = False
        
    def visit_Module(self, node: ast.Module) -> ast.Module:
        """Обрабатывает модуль, добавляет импорты при необходимости."""
        result = self.generic_visit(node)
        if not isinstance(result, ast.Module):
            return node
        if self.imports_added and result.body:
            # Добавляем импорт convert_batch_decimals если его нет
            has_import = any(
                isinstance(stmt, (ast.Import, ast.ImportFrom))
                and self._has_convert_batch_import(stmt)
                for stmt in result.body
            )
            if not has_import:
                # Ищем существующий импорт из hintgrid.utils.coercion
                import_pos = -1
                for i, stmt in enumerate(result.body):
                    if isinstance(stmt, ast.ImportFrom):
                        if stmt.module == 'hintgrid.utils.coercion':
                            # Добавляем в существующий импорт
                            if not any(alias.name == 'convert_batch_decimals' for alias in stmt.names):
                                stmt.names.append(ast.alias(name='convert_batch_decimals', asname=None))
                            import_pos = -2  # Помечаем что нашли
                            break
                    elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
                        import_pos = i
                
                if import_pos == -1:
                    # Добавляем новый импорт в начало файла (после docstring если есть)
                    import_stmt = ast.ImportFrom(
                        module='hintgrid.utils.coercion',
                        names=[ast.alias(name='convert_batch_decimals', asname=None)],
                        level=0,
                    )
                    insert_pos = 0
                    # Пропускаем docstring и другие строковые константы
                    for i, stmt in enumerate(result.body):
                        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
                            if isinstance(stmt.value.value, str):
                                insert_pos = i + 1
                                continue
                        break
                    result.body.insert(insert_pos, import_stmt)
        return result
    
    def _has_convert_batch_import(self, node: ast.AST) -> bool:
        """Проверяет, есть ли импорт convert_batch_decimals."""
        if isinstance(node, ast.ImportFrom):
            if node.module == 'hintgrid.utils.coercion':
                return any(alias.name == 'convert_batch_decimals' for alias in node.names)
        return False
    
    def _is_already_converted(self, node: ast.AST) -> bool:
        """Проверяет, уже ли узел обернут в convert_batch_decimals."""
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == 'convert_batch_decimals':
                return True
        return False
    
    def visit_Call(self, node: ast.Call) -> ast.AST:
        """Обрабатывает вызовы функций."""
        # Проверяем вызовы merge_* функций
        if isinstance(node.func, ast.Name) and node.func.id in self.MERGE_FUNCTIONS:
            if len(node.args) >= 2:
                batch_arg = node.args[1]
                # Пропускаем если уже обернуто в convert_batch_decimals
                if not self._is_already_converted(batch_arg):
                    # Оборачиваем в convert_batch_decimals
                    converted = ast.Call(
                        func=ast.Name(id='convert_batch_decimals', ctx=ast.Load()),
                        args=[batch_arg],
                        keywords=[],
                    )
                    new_args = list(node.args)
                    new_args[1] = converted
                    self.changes_made += 1
                    self.imports_added = True
                    return ast.Call(
                        func=node.func,
                        args=new_args,
                        keywords=node.keywords,
                    )
        
        return self.generic_visit(node)


def fix_file(file_path: Path, dry_run: bool = False) -> tuple[bool, int]:
    """Исправляет типы в файле."""
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return False, 0
    
    try:
        tree = ast.parse(content, filename=str(file_path))
    except SyntaxError as e:
        print(f"Syntax error in {file_path}: {e}")
        return False, 0
    
    fixer = TestTypeFixer(file_path)
    new_tree = fixer.visit(tree)
    
    if fixer.changes_made == 0:
        return True, 0
    
    if not dry_run:
        try:
            new_content = ast.unparse(new_tree)
            file_path.write_text(new_content, encoding='utf-8')
        except Exception as e:
            print(f"Error writing {file_path}: {e}")
            return False, fixer.changes_made
    
    return True, fixer.changes_made


def main() -> None:
    """Точка входа."""
    if len(sys.argv) < 2:
        print("Usage: fix_test_types.py <test_file_or_dir> [--dry-run]")
        sys.exit(1)
    
    target = Path(sys.argv[1])
    dry_run = '--dry-run' in sys.argv
    
    if not target.exists():
        print(f"Error: {target} not found")
        sys.exit(1)
    
    files_to_process: list[Path] = []
    if target.is_file():
        files_to_process = [target]
    else:
        files_to_process = list(target.rglob("test_*.py"))
    
    total_changes = 0
    for file_path in files_to_process:
        success, changes = fix_file(file_path, dry_run=dry_run)
        if success:
            if changes > 0:
                print(f"{file_path}: {changes} changes")
                total_changes += changes
        else:
            print(f"{file_path}: ERROR")
    
    print(f"\nTotal changes: {total_changes}")


if __name__ == '__main__':
    main()
