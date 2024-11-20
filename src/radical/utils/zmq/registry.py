
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
            path = '%s/%s.db' % (self._path, self._uid)
            self._log.debug('use shelve %s', path)

            self._data = shelve.open(path, writeback=True)

        else:
            self._log.debug('use in-memory dict')
            self._data = dict()

        self.register_request('put',  self.put)
        self.register_request('get',  self.get)
        self.register_request('keys', self.keys)
        self.register_request('del',  self.delitem)
        self.register_request('dump', self.dump)


    # --------------------------------------------------------------------------
    #
    def dump(self, name: str = None) -> None:


        if not isinstance(self._data, dict):
            self._log.debug('ignore dump for non-dict %s', name)

        else:
            if name: fname = '%s/%s.%s.json' % (self._path, self._uid, name)
            else   : fname = '%s/%s.json'    % (self._path, self._uid)

            self._log.debug('dumo to %s', fname)
            write_json(self._data, fname)



    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

      # self.dump()

        self._log.debug('stop')

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

        self._log.debug_9('put %s: %s', str(key), str(val))

        for elem in path:

            if elem not in this or this[elem] is None:
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

        if this is None:
            this = dict()

        val = this.get(leaf)

        self._log.debug_9('get %s: %s', str(key), str(val))
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

        if this is None:
            this = dict()

        keys = list(this.keys())

        self._log.debug_9('keys: %s', keys)

        return keys


    # --------------------------------------------------------------------------
    #
    def delitem(self, key: str) -> None:

        self._log.debug_9('del: %s', key)

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

        self._url = url
        self._pwd = pwd

        super().__init__(url=url)


    # --------------------------------------------------------------------------
    #
    def dump(self, name: str = None) -> None:

        return self.request(cmd='dump', name=name)


    # --------------------------------------------------------------------------
    # verbose API
    def get(self, key    : str,
                  default: Optional[Any] = None) -> Optional[Any]:

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

