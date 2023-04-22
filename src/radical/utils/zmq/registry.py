
import atexit
import shelve

from typing import List, Optional, Any

from ..json_io    import write_json
from ..dict_mixin import DictMixin

from .server import Server
from .client import Client

_registries = list()


# ------------------------------------------------------------------------------
#
def _flush_registries():
    for _reg in _registries:
        _reg.stop()


atexit.register(_flush_registries)


# ------------------------------------------------------------------------------
#
class Registry(Server):
    '''
    The `ru.zmq.Registry` is a ZMQ service which provides a hierarchical
    persistent data store.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, url       : Optional[str] = None,
                       uid       : Optional[str] = None,
                       path      : Optional[str] = None,
                       persistent: bool          = False) -> None:

        super().__init__(url=url, uid=uid, path=path)

        if persistent:
            self._data = shelve.open('%s.db' % self._uid, writeback=True)
        else:
            self._data = dict()

        self.register_request('put',  self.put)
        self.register_request('get',  self.get)
        self.register_request('keys', self.keys)
        self.register_request('del',  self.delitem)


    # --------------------------------------------------------------------------
    #
    def dump(self) -> None:

        if isinstance(self._data, dict):
            write_json(self._data, '%s.json' % self._uid)


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        self.dump()

        if isinstance(self._data, shelve.Shelf):
            self._data.close()

        super().stop()


    # --------------------------------------------------------------------------
    #
    def put(self, key: str, val: Any) -> None:

        this  = self._data
        elems = key.split('.')
        path  = elems[:-1]
        leaf  = elems[-1]

        for elem in path:

            if elem not in this:
                this[elem] = dict()

            this = this[elem]

        this[leaf] = val

        if not isinstance(self._data, dict):
            self._data.sync()


    # --------------------------------------------------------------------------
    #
    def get(self, key: str) -> Optional[str]:

        this  = self._data
        elems = key.split('.')
        path  = elems[:-1]
        leaf  = elems[-1]

        for elem in path:
            this = this.get(elem, {})
            if not this:
                break

        val = this.get(leaf)
        return val


    # --------------------------------------------------------------------------
    #
    def keys(self, pwd: Optional[str] = None) -> List[str]:

        this = self._data

        if pwd:
            path = pwd.split('.')
            for elem in path:
                this = this.get(elem, {})
                if not this:
                    break

        return list(this.keys())


    # --------------------------------------------------------------------------
    #
    def delitem(self, key: str) -> None:

        this = self._data

        if key:
            path = key.split('.')
            for elem in path[:-1]:
                this = this.get(elem, {})
                if not this:
                    break

            if this:
                del this[path[-1]]


# ------------------------------------------------------------------------------
#
class RegistryClient(Client, DictMixin):
    '''
    The `ru.zmq.RegistryClient` class provides a simple dict-like interface to
    a remote `ru.zmq.Registry` service.  Note that only top-level dict-actions
    on the `RegistryClient` instance are synced with the remote service storage.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, url: str,
                       pwd: Optional[str] = None) -> None:

        self._pwd = pwd

        super().__init__(url=url)


    # --------------------------------------------------------------------------
    # verbose API
    def get(self, key    : str,
                  default: Optional[str] = None) -> Optional[Any]:

        if self._pwd:
            key = self._pwd + '.' + key

        try:
            return self.request(cmd='get', key=key)
        except:
            return default


    def put(self, key: str,
                  val: Any) -> None:

        if self._pwd:
            key = self._pwd + '.' + key
        ret = self.request(cmd='put', key=key, val=val)

        assert ret is None
        return ret


    # --------------------------------------------------------------------------
    # dict mixin API
    def __getitem__(self, key: str) -> Optional[Any]:

        return self.get(key)


    def __setitem__(self, key: str, val: Any) -> None:

        return self.put(key, val)


    def __delitem__(self, key: str) -> None:

        if self._pwd:
            key = self._pwd + '.' + key
        ret = self.request(cmd='del', key=key)
        assert ret is None


    def keys(self) -> List[str]:

        ret = self.request(cmd='keys', pwd=self._pwd)
        assert isinstance(ret, list)
        return ret


# ------------------------------------------------------------------------------

