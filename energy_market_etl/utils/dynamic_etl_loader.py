from importlib import import_module
from inspect import isclass
import itertools
import os
from os import walk
from os.path import abspath, basename, dirname, join
from typing import List, Union

from energy_market_etl.etls.etl import Etl

__all__ = 'get_etls'

PROJ_DIR = abspath(join(dirname(abspath(__file__)), '../..'))
APP_DIR = os.path.join(PROJ_DIR, 'energy_market_etl')
APP_MODULE = basename(APP_DIR)


def get_modules(module):
    file_dir = abspath(join(APP_DIR, module))
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
    return dynamic_loader('etls', is_etl)


def get_etl_keys() -> List[str]:
    etls = get_etls()
    etl_key_lists = list(etl.ETL_KEYS for etl in etls)
    etl_keys = list(set(itertools.chain(*etl_key_lists)))
    return etl_keys


def is_etl(item) -> bool:
    return isclass(item) and issubclass(item, Etl)
