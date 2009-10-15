# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Chris Goffinet <goffinet@digg.com>
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy: Connections"""

import random
import os
import threading

from cassandra import Cassandra
from thrift import Thrift
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

from lazyboy.exceptions import ErrorCassandraClientNotFound, \
    ErrorThriftMessage, ErrorCassandraNoServersConfigured


def init(connections, client_class=None):
    """Initialize Lazyboy. Returns a function to connect to a Keyspace.


    The argument should be a dictionary of keyspace names and hosts.

    Example:

    connect = init({"keyspace_1": ["10.0.0.1:9160", "10.0.0.2:9160"],
                    "keyspace_2": ["10.0.0.3:9160", "10.0.0.4:9160"]})
    client = connect("keyspace_1")
    client.get_slice(...)
    """
    clients = {}
    client_class = client_class or RoundRobinClient

    def get_client(keyspace):
        """Return a client for a keyspace."""
        if keyspace not in connections:
            raise ErrorCassandraClientNotFound(
                "No client for keyspace `%s' defined." % keyspace)

        key = str(os.getpid()) + threading.currentThread().getName() + keyspace

        if key not in clients:
            clients[key] = client_class(connections[keyspace])

        return clients[key]

    return get_client


def _connect(client):
    """Connect to Cassandra if not connected."""
    if client.transport.isOpen():
        return True

    try:
        client.transport.open()
        return True
    except Thrift.TException, texc:
        if texc.message:
            message = texc.message
        else:
            message = "Transport error, reconnect"
        client.transport.close()
        raise ErrorThriftMessage(message)
    except Exception:
        client.transport.close()

    return False


def _build_client(host, port):
    """Create and return a Cassandra client instance."""
    socket = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client


def simple_connect(server):
    """Connect to a server and return a client instance."""
    client = _build_client(*server.split(":"))
    _connect(client)
    return client


class RoundRobinClient(object):

    """A client which uses round-robin load balancing."""

    def __init__(self, servers):
        """Initialize."""
        self._servers = servers
        self._clients = map(simple_connect, servers)
        self._current_server = random.randint(0, len(self._clients))

    def _get_server(self):
        """Return the server to send the next query to."""
        if self._clients is None or len(self._clients) == 0:
            raise ErrorCassandraNoServersConfigured()

        next_server = self._current_server % len(self._clients)
        self._current_server += 1
        return self._clients[next_server]

    def list_servers(self):
        """Return all known servers."""
        return self._clients

    def __getattr__(self, attr):

        """Wrap every __func__ call to Cassandra client and connect()"""

        def func(*args, **kwargs):
            """A wrapped function from Cassandra.Client."""
            client = self._get_server()
            if _connect(client):
                try:
                    return getattr(client, attr).__call__(*args, **kwargs)
                except Thrift.TException, texc:
                    if texc.message:
                        message = texc.message
                    else:
                        message = "Transport error, reconnect"
                    client.transport.close()
                    raise ErrorThriftMessage(message)
                except Exception:
                    client.transport.close()
                    raise

        return func
