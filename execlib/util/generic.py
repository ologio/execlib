import logging

import tqdm
import colorama
from colorama import Fore, Back, Style


def color_text(text, *colorama_args):
    return f"{''.join(colorama_args)}{text}{Style.RESET_ALL}"

def get_func_name(func):
    func_name = str(func)
    if hasattr(func, '__name__'):
        func_name = func.__name__

    return func_name


class ColorFormatter(logging.Formatter):
    _format = '%(levelname)-8s :: %(name)s %(message)s'
    colorama.init(autoreset=True)

    FORMATS = {
        'x':         Fore.YELLOW  + _format,
        'listener':  Fore.GREEN   + _format,
        'handler':   Fore.CYAN    + _format,
        'server':    Style.DIM    + Fore.CYAN + _format,
        'router':    Fore.MAGENTA + _format,
        'site':      Fore.BLUE    + _format,
        'utils':     Style.DIM    + Fore.WHITE + _format,
    }
    FORMATS = { k:logging.Formatter(v) for k,v in FORMATS.items() }
    DEFAULT_LOGGER = logging.Formatter(_format)

    def format(self, record):
        # color by high-level submodule
        name_parts   = record.name.split('.')
        package      = ''.join(name_parts[:1])
        submodule    = ''.join(name_parts[1:2])
        subsubmodule = '.'.join(name_parts[1:3])

        formatter = self.DEFAULT_LOGGER
        if subsubmodule in self.FORMATS:
            formatter = self.FORMATS[subsubmodule]
        elif submodule in self.FORMATS:
            formatter = self.FORMATS[submodule]

        name = record.name
        if package == 'execlib':
            name = f'execlib.{subsubmodule}'

        limit = 26
        name  = name[:limit]
        name  = f'[{name}]{"-"*(limit-len(name))}'
        record.name = name

        return formatter.format(record)

class TqdmLoggingHandler(logging.StreamHandler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        formatter = ColorFormatter()
        self.setFormatter(formatter)

    def emit(self, record):
        try:
            msg = self.format(record)
            #tqdm.tqdm.write(msg)
            tqdm.tqdm.write(msg, end=self.terminator)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

