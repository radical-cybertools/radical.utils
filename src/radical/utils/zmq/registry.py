
from typing import List, Optional, Any

from ..json_io    import write_json
from ..dict_mixin import DictMixin

from .server import Server
from .client import Client


# ------------------------------------------------------------------------------
#
class Registry(Server):


    # --------------------------------------------------------------------------
    #
    def __init__(self, url=None,
                       uid=None) -> None:

        super().__init__(url=url, uid=uid)

        self._data = dict()

        self.register_request('put',  self.put)
        self.register_request('get',  self.get)
        self.register_request('keys', self.keys)
        self.register_request('del',  self.delitem)


    # --------------------------------------------------------------------------
    #
    def stop(self) -> None:

        write_json(self._data, '%s.json' % self._uid)
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

        del(self._data[key])


# ------------------------------------------------------------------------------
#
class RegistryClient(Client, DictMixin):


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
        assert(ret is None)
        return ret


    # --------------------------------------------------------------------------
    # dict mixin API
    def __getitem__(self, key: str) -> Optional[Any]:
        return self.get(key)

    def __setitem__(self, key: str, val: Any) -> None:
        return self.put(key, val)

    def __delitem__(self, key: str) -> None:
        ret = self.request(cmd='del', key=key)
        assert(ret is None)

    def keys(self) -> List[str]:
        ret = self.request(cmd='keys')
        assert(isinstance(ret, list))
        return ret


# ------------------------------------------------------------------------------

