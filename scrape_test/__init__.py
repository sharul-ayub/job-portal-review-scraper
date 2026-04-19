__all__ = ["scrape_reviews"]


def __getattr__(name):
    if name == "scrape_reviews":
        from .pipeline import scrape_reviews

        return scrape_reviews
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
