from __future__ import print_function, division, absolute_import

import logging
from azure.datalake.store import lib, AzureDLFileSystem

from . import core
from .utils import infer_storage_options
from ..base import tokenize

logger = logging.getLogger(__name__)

class AdlFileSystem(AzureDLFileSystem, core.FileSystem):
    """API spec for the methods a filesystem

    A filesystem must provid§e these methods, if it is to be registered as
    a backend for dask.

    Implementation for Azure Data Lake """
    sep = '/'

    def __init__(self, tenant_id=None, client_id=None, client_secret=None, store_name=None, **kwargs):
        token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        kwargs['store_name'] = store_name
        kwargs['token'] = token
        AzureDLFileSystem.__init__(self, **kwargs)

    def _trim_filename(self, fn):
        so = infer_storage_options(fn)
        return so['path']

    def glob(self, path):
        """For a template path, return matching files"""
        adl_path = self._trim_filename(path)
        return ['adl://%s' % s for s in AzureDLFileSystem.glob(self, adl_path)]

    def mkdirs(self, path):
       pass # no need to pre-make paths on ADL

    def open(self, path, mode='rb'):
        adl_path = self._trim_filename(path)
        return AzureDLFileSystem.open(self, adl_path, mode=mode)

    def ukey(self, path):
        adl_path = self._trim_filename(path)
        return tokenize(self.info(adl_path)['modificationTime'])

    def size(self, path):
        adl_path = self._trim_filename(path)
        return self.info(adl_path)['length']

    def __getstate__(self):
        dic = self.__dict__.copy()
        del dic['token']
        logger.debug("Serialize with state: %s", dic)
        return dic

core._filesystems['adl'] = AdlFileSystem
