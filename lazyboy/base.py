# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

from lazyboy.exceptions import ErrorUnknownKeyspace, ErrorIncompleteKey
import lazyboy.connection as connection

class CassandraBase(object):
    """The base class for all Cassandra-accessing objects."""

    def __init__(self, connections):
        self._keyspace = connection.init(connections)

    def _get_cas(self, keyspace=None):
        """Return the cassandra client."""
        if not keyspace and (not hasattr(self, 'key') or not self.key):
            raise ErrorIncompleteKey("Instance has no key.")

        return self._keyspace(keyspace or self.key.keyspace)
