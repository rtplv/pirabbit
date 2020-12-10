import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

_path = Path('./logs')
_path.mkdir(exist_ok=True)

_log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s')

default_handler = RotatingFileHandler(
    _path.joinpath('app.log'),
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=5
)

default_handler.setFormatter(_log_formatter)
default_handler.setLevel(logging.INFO)