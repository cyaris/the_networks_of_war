import logging
from typing import Literal

from colorama import Fore, Style


class LoggerFormatter(logging.Formatter):
    def format(self, record):
        italic = "\033[3m"
        bold = "\033[1m"

        level_colors = {
            "CRITICAL": {"levelname": Fore.RED, "message": Fore.RED},
            "ERROR": {"levelname": Fore.RED, "message": Fore.RED},
            "WARNING": {"levelname": Fore.YELLOW, "message": Fore.YELLOW},
            "INFO": {"levelname": Fore.CYAN, "message": Fore.WHITE},
            "DEBUG": {"levelname": Fore.BLUE, "message": italic + Fore.BLUE},
        }

        if record.levelname in level_colors:
            fmt = (
                f"{Fore.GREEN}%(asctime)s{Style.RESET_ALL} "
                + f"{bold + level_colors[record.levelname]['levelname']}%(levelname)s{Style.RESET_ALL} "
                + f"{Fore.MAGENTA}%(name)s{Style.RESET_ALL} "
                + f"{Fore.BLUE}%(funcName)s %(filename)s:%(lineno)d{Style.RESET_ALL} "
                + f"{level_colors[record.levelname]['message']}%(message)s{Style.RESET_ALL}"
            )
        else:
            fmt = "%(asctime)s %(levelname)s %(name)s %(funcName)s %(filename)s:%(lineno)d %(message)s"

        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


def initialize_logger(
    name: str,
    level: Literal["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
    frmt: str | logging.Formatter | None = None,
    propagate: bool = False,
) -> logging.Logger:
    if frmt is None:
        frmt = LoggerFormatter()

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level, logging.INFO))
    logger.propagate = propagate

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(frmt) if isinstance(frmt, str) else frmt)
        logger.addHandler(handler)

    return logger
