"""
Dev mode logger provider.
"""

from contextlib import asynccontextmanager, contextmanager
from typing import final

from adapta.logs._logger_interface import LoggerInterface


@final
class VoidLogger(LoggerInterface):
    """
    Logger that sends output into the void. Useful for testing or disabling logs.
    """

    def __init__(self, **options):
        pass

    def info(self, template: str, tags: dict[str, str] | None = None, **kwargs) -> None:
        pass

    def warning(
        self, template: str, exception: BaseException | None = None, tags: dict[str, str] | None = None, **kwargs
    ) -> None:
        pass

    def error(
        self, template: str, exception: BaseException | None = None, tags: dict[str, str] | None = None, **kwargs
    ) -> None:
        pass

    def debug(
        self,
        template: str,
        exception: BaseException | None = None,
        diagnostics: str | None = None,
        tags: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    @contextmanager
    def redirect(self, tags: dict[str, str] | None = None, **kwargs):
        yield

    @asynccontextmanager
    async def redirect_async(self, tags: dict[str, str] | None = None, **kwargs):
        yield
