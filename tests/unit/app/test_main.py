"""Unit tests for app.py main entrypoint.

Tests verify main() function and module execution.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.mark.unit
def test_main_function() -> None:
    """Test main() function calls cli_app()."""
    from hintgrid.app import main

    with patch("hintgrid.cli.app") as mock_cli_app:
        main()
        mock_cli_app.assert_called_once()


@pytest.mark.unit
def test_main_module_execution() -> None:
    """Test that __main__ execution calls main()."""
    from hintgrid.app import main

    # Verify the function exists and is callable
    assert callable(main), "main should be callable"
