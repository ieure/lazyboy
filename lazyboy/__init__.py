# -*- coding: utf-8 -*-
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#
"""Lazyboy, an object-non-relational-manager for Cassandra."""

from lazyboy.connection import init
from lazyboy.key import Key
from lazyboy.record import Record
from lazyboy.recordset import RecordSet, KeyRecordSet
from lazyboy.view import View, PartitionedView
