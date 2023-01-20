from importlib import import_module
from inspect import isclass
import os
from os import walk
from os.path import abspath, basename, dirname, join
from sys import modules

from energy_market_etl.etls.etl import Etl

__all__ = 'get_etls'

PROJ_DIR = abspath(join(dirname(abspath(__file__)), '../..'))
APP_DIR = os.path.join(PROJ_DIR, 'energy_market_etl')
APP_MODULE = basename(APP_DIR)
CURR_DIR = os.path.join(APP_DIR, 'etls')
CURR_MODULE = basename(CURR_DIR)


def get_modules(module):
    file_dir = abspath(join(CURR_DIR, module))
    for root, dirnames, files in walk(file_dir):
        mod_path = '{}{}'.format(APP_MODULE, root.split(APP_DIR)[1]).\
            replace(os.path.sep, '.')
        for filename in files:
            if filename.endswith('.py') and \
                    not filename.startswith('__init__'):
                yield '.'.join([mod_path, filename[0:-3]])


def dynamic_loader(module, compare):
    items = []
    for mod in get_modules(module):
        module = import_module(mod)
        if hasattr(module, '__all__'):
            objs = [getattr(module, obj) for obj in module.__all__]
            items += [o for o in objs if o not in items and compare(o)]
    return items


def get_etls():
    return dynamic_loader('models', is_etl)


def is_etl(item):
    return isclass(item) and issubclass(item, Etl)


def load_etls():
    for etl in get_etls():
        setattr(modules[__name__], etl.__name__, etl)


load_etls()
