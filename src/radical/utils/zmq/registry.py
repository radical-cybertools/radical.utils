
from ..json_io    import write_json
from ..dict_mixin import DictMixin

from .server import Server
from .client import Client


# ------------------------------------------------------------------------------
#
class Registry(Server):


    # --------------------------------------------------------------------------
    #
    def __init__(self, url=None, uid=None):

        super().__init__(url=url, uid=uid)

        self._data = dict()

        self.register_request('put',  self.put)
        self.register_request('get',  self.get)
        self.register_request('keys', self.keys)
        self.register_request('del',  self.delitem)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        write_json(self._data, '%s.json' % self._uid)
        super().stop()


    # --------------------------------------------------------------------------
    #
    def put(self, arg):

        key, value = arg

        this  = self._data
        elems = key.split('.')
        for elem in elems[:-1]:
            step = this.get(elem)
            if step is None:
                step = dict()
                this[elem] = step
                this = this[elem]
        this[elems[-1]] = value


    # --------------------------------------------------------------------------
    #
    def get(self, arg):

        key = arg[0]

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
    def keys(self, arg):

        assert(not arg)

        return list(self._data.keys())


    # --------------------------------------------------------------------------
    #
    def delitem(self, arg):

        key = arg[0]

        del(self._data[key])


# ------------------------------------------------------------------------------
#
class RegistryClient(Client, DictMixin):


    # --------------------------------------------------------------------------
    #
    def __init__(self, url):

        super().__init__(url=url)



    # --------------------------------------------------------------------------
    # verbose API
    def get(self, key, default=None):
        try:
            return self.request(cmd='get', arg=[key]).res
        except:
            return default


    def put(self, key, value):
        return self.request(cmd='put', arg=[key, value]).res


    # --------------------------------------------------------------------------
    # dict mixin API
    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.put(key, value)

    def __delitem__(self, key):
        return self.request(cmd='del', arg=[key])

    def keys(self):
        return self.request(cmd='keys').res


# ------------------------------------------------------------------------------

