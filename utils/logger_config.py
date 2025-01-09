import sys

from loguru import logger

# Remove qualquer configuração padrão do loguru
logger.remove()

# Configuração de cores para o console
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{module}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True,
)

# Configuração de log para arquivos com rotação semanal
logger.add(
    "logs/file_{time}.log",
    rotation="1 week",
    retention="1 month",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} - {message}",
    colorize=False,  # Arquivos de log geralmente não precisam de cores
)

# Configuração de log para erros específicos
logger.add(
    "logs/error_{time}.log",
    level="ERROR",
    retention="1 month",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{line} - {message}",
    colorize=False,
)