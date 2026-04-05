def simple_preprocess(
    doc: str,
    deacc: bool = False,
    min_len: int = 2,
    max_len: int = 15,
) -> list[str]:
    """
    Tokenize and preprocess a string.

    Converts to lowercase, removes punctuation, and filters by length.

    Args:
        doc: Input text to process
        deacc: Remove accent marks (default False)
        min_len: Minimum token length (default 2)
        max_len: Maximum token length (default 15)

    Returns:
        List of tokens
    """
    ...
