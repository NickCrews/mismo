def is_spacy_installed() -> bool:
    try:
        import spacy  # noqa: F401
        return True
    except ImportError:
        return False
