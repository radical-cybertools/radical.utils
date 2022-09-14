
import shelve

from typing import List, Optional, Any

from ..json_io    import write_json
from ..dict_mixin import DictMixin

from .server import Server
from .client import Client


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
                       persistent: bool          = False) -> None:

        super().__init__(url=url, uid=uid)

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
    def stop(self) -> None:

        if isinstance(self._data, dict):
            write_json(self._data, '%s.json' % self._uid)
        else:
            self._data.close()

        super().stop()


    # --------------------------------------------------------------------------
    #
    def put(self, key: str, val: Any) -> None:

        this  = self._data
        elems = key.split('.')

        for elem in elems[:-1]:
            step = this.get(elem)
            if step is None:
                step = dict()
                this[elem] = step
                this = this[elem]

        this[elems[-1]] = val

        if not isinstance(self._data, dict):
            self._data.sync()


    # --------------------------------------------------------------------------
    #
    def get(self, key: str) -> Optional[str]:

        try:
            this  = self._data
            elems = key.split('.')
            for elem in elems[:-1]:
                step = this.get(elem)
                if step is None:
                    return None
                this = step

            ret = this.get(elems[-1])
            return ret

        except AttributeError:
            return None


    # --------------------------------------------------------------------------
    #
    def keys(self) -> List[str]:

        return list(self._data.keys())


    # --------------------------------------------------------------------------
    #
    def delitem(self, key: str) -> None:

        del self._data[key]
        if not isinstance(self._data, dict):
            self._data.sync()


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
    def __init__(self, url: str) -> None:

        super().__init__(url=url)


    # --------------------------------------------------------------------------
    # verbose API
    def get(self, key: str, default: Optional[str] = None) -> Optional[Any]:

        try:
            return self.request(cmd='get', key=key)
        except:
            return default


    def put(self, key: str,
                  val: Any) -> None:
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
        ret = self.request(cmd='del', key=key)
        assert ret is None


    def keys(self) -> List[str]:
        ret = self.request(cmd='keys')
        assert isinstance(ret, list)
        return ret


# ------------------------------------------------------------------------------

