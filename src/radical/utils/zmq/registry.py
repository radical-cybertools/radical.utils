
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

        k, v = arg

        this  = self._data
        elems = k.split('.')
        for elem in elems[:-1]:
            step = this.get(elem)
            if step is None:
                step = dict()
                this[elem] = step
                this = this[elem]
        this[elems[-1]] = v


    # --------------------------------------------------------------------------
    #
    def get(self, arg):

        k = arg[0]

        try:
            this  = self._data
            elems = k.split('.')
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

        return list(self._data.keys())


    # --------------------------------------------------------------------------
    #
    def delitem(self, arg):

        k = arg[0]

        del(self._data[k])


# ------------------------------------------------------------------------------
#
class RegistryClient(Client, DictMixin):


    # --------------------------------------------------------------------------
    #
    def __init__(self, url):

        super().__init__(url=url)



    # --------------------------------------------------------------------------
    # verbose API
    def get(self, k):
        return self.request(cmd='get', arg=[k]).res

    def put(self, k, v):
        return self.request(cmd='put', arg=[k, v]).res


    # --------------------------------------------------------------------------
    # dict mixin API
    def __getitem__(self, k):
        return self.get(k)

    def __setitem__(self, k, v):
        return self.put(k, v)

    def __delitem__(self, k):
        return self.request(cmd='del', arg=[k])

    def keys(self):
        return self.request(cmd='keys').res


# ------------------------------------------------------------------------------

