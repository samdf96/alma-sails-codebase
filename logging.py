import logging
from datetime import datetime
from colorama import Fore, Style, init as colorama_init

colorama_init(autoreset=True)

class PrefectStyleFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": Style.DIM + Fore.WHITE,
        "INFO": Fore.CYAN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED + Style.BRIGHT,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        level_color = self.COLORS.get(record.levelname, "")
        level_str = f"{level_color}{record.levelname:<8}{Style.RESET_ALL}"
        return f"{timestamp} | {level_str} | {record.getMessage()}"

def get_logger(name: str = "alma_ops", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(PrefectStyleFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(level)
    return logger