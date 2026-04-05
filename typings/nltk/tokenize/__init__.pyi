class TweetTokenizer:
    """Tokenizer for tweets and social media text."""

    def __init__(
        self,
        preserve_case: bool = True,
        reduce_len: bool = False,
        strip_handles: bool = False,
        match_phone_numbers: bool = True,
    ) -> None: ...
    def tokenize(self, text: str) -> list[str]:
        """Tokenize a string into tokens."""
        ...
