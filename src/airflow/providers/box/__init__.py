# read version from version.txt
from os import path

with open(path.join(path.dirname(__file__), "_version.txt")) as _f:
    __version__ = _f.read().strip()
