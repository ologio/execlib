import re
import inspect
from glob import glob
from pathlib import Path

from wcmatch import glob as wc_glob 


camel2snake_regex = re.compile(r'(?<!^)(?=[A-Z])')

def iter_nested_paths(path: Path, ext: str = None, no_dir=False, relative=False):
    if ext is None: ext = ''
    return iter_glob_paths(f'**/!(.*|*.tmp|*~)*{ext}', path, no_dir=no_dir, relative=relative)

def iter_glob_paths(_glob, path: Path, no_dir=False, relative=False):
    '''
    wc_glob should ignore hidden files and directories by default; `**` only matches
    non-hidden directories/files, `*` only non-hidden files.

    Note: Pattern quirks
        - Under `wc_glob`, `**` (when GLOBSTAR enabled) will match all files and
          directories, recursively. Contrast this with `Path.rglob('**')`, which matches
          just directories. For `wcmatch`, use `**/` to recover the directory-only
          behavior.

    Note: pattern behavior
        - `*`: all files and dirs, non-recursive
        - `**`: all files and dirs, recursive
        - `*/`: all dirs, non-recursive
        - `**/`: all dirs, recursive
        - All file (no dir) equivalents: either of the first two, with `no_dir=True`
    '''
    flags = wc_glob.GLOBSTAR | wc_glob.EXTGLOB | wc_glob.NEGATE
    if no_dir:
        flags |= wc_glob.NODIR

    glob_list = list(map(
        Path,
        wc_glob.glob(_glob, root_dir=path, flags=flags)
    ))

    if not relative:
        glob_list = [Path(path, p) for p in glob_list]

    return glob_list

def get_running_path():
    '''
    Try to get the location of the current running script. If there are issues getting
    this, defaults to CWD.
    '''
    try:
        calling_module = inspect.getmodule(inspect.stack()[-1][0])
        return Path(calling_module.__file__).parent
    except:
        return Path().cwd()

def glob_match(filename, patterns):
    '''
    Convenience wrapper for wcmatch.glob.globmatch, with standard `**` support. `*` won't
    match across separators here, unlike `fnmatch.fnmatch`. Returns a boolean indicating
    if the filename matches at least one of the provided patterns.
    '''
    return wc_glob.globmatch(filename, patterns, flags=wc_glob.GLOBSTAR | wc_glob.EXTGLOB | wc_glob.NEGATE)

def camel_to_snake(text):
    return camel2snake_regex.sub('_', text).lower()

