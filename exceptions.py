from __future__ import annotations


class ScraperError(Exception):
    pass


class ModalHandlingError(ScraperError):
    pass


class PageLoadError(ScraperError):
    pass


class SelectorNotFoundError(ScraperError):
    pass


class SheetsWriteError(ScraperError):
    pass


class ComparisonError(ScraperError):
    pass
