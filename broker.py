import numbers
from typing import Iterator, Tuple, Union

try:
    import redis
except ImportError:
    redis = None

try:
    from urllib.parse import urlparse, urljoin, quote, unquote
except ImportError:
    from urlparse import urlparse, urljoin
    from urllib import quote, unquote


class BrokerBase(object):
    def __init__(self, broker_url: str, *args, **kwargs):
        purl = urlparse(broker_url)
        self.host = purl.hostname
        self.port = purl.port
        self.vhost = purl.path[1:]

        username = purl.username
        password = purl.password

        self.username = unquote(username) if username else username
        self.password = unquote(password) if password else password


class Redis(BrokerBase):
    sep = '\x06\x16'
    last_values = {}

    def __init__(self, broker_url: str, *_, **__):
        super(Redis, self).__init__(broker_url)
        self.host = self.host or 'localhost'
        self.port = self.port or 6379
        self.vhost = self._prepare_virtual_host(self.vhost)

        if not redis:
            raise ImportError('redis library is required')

        self.redis = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.vhost,
            password=self.password,
        )

    def itercounts(self) -> Iterator[Tuple[str, int]]:
        for name in self.redis.keys():
            name = name.decode('utf-8')
            try:
                value = self.redis.llen(name)
            except:
                if name not in self.last_values:  # Skip unknown empty queues
                    continue
                else:
                    value = 0

            if 'reply.celery.pidbox' in name:
                continue

            yield name, value
            self.last_values.update({name: value})

    def _prepare_virtual_host(self, vhost: Union[str, int]) -> int:
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = 0
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError('Database is int between 0 and limit - 1, not {0}'.format(vhost))
        return vhost
