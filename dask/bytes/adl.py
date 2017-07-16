from __future__ import print_function, division, absolute_import

from azure.datalake.store import lib, AzureDLFileSystem

from . import core
from .utils import infer_storage_options
from ..base import tokenize


class AdlFileSystem(AzureDLFileSystem, core.FileSystem):
    """API spec for the methods a filesystem

    A filesystem must provide these methods, if it is to be registered as
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
        return so.get('host', '') + so['path']

    def glob(self, path):
        """For a template path, return matching files"""
        return sorted(AzureDLFileSystem.glob(self, path))

    def mkdirs(self, path):
       pass # no need to pre-make paths on ADL

    def open(self, path, mode='rb', **kwargs):
        adl_path = path #no need to trim, as the FS does it self._trim_filename(path)
        f = AzureDLFileSystem.open(self, adl_path, mode=mode)
        return f

    def ukey(self, path):
        return self.info(path)['ETag']

    def size(self, path):
        return self.info(path)['Size']

core._filesystems['adl'] = AdlFileSystem