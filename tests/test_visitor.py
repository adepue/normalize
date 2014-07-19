#
# This file is a part of the normalize python library
#
# normalize is free software: you can redistribute it and/or modify
# it under the terms of the MIT License.
#
# normalize is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# MIT License for more details.
#
# You should have received a copy of the MIT license along with
# normalize.  If not, refer to the upstream repository at
# http://github.com/hearsaycorp/normalize
#

from __future__ import absolute_import

from datetime import datetime
import json
import unittest

from normalize.visitor import VisitorPattern
from testclasses import wall_one, acent, Wall, StarSystem


JSON_CAN_DUMP = (basestring, int, long, dict, list)


class SimpleDumper(VisitorPattern):

    @classmethod
    def apply(self, value, *args):
        if isinstance(value, JSON_CAN_DUMP):
            dumpable = value
        elif isinstance(value, datetime):
            dumpable = value.isoformat()
        else:
            raise Exception("Can't dump %r" % value)
        return dumpable


class TestVisitor(unittest.TestCase):
    def assertDiffs(self, a, b, expected, **kwargs):
        differences = set(str(x) for x in a.diff(b, **kwargs))
        self.assertEqual(
            differences,
            set("<DiffInfo: %s>" % x for x in expected)
        )

    def test_simple_dumper(self):
        dumpable = SimpleDumper.visit(wall_one)
        self.assertIsInstance(dumpable['posts'][0], dict)
        self.assertEqual(dumpable['posts'][0]['edited'], "2001-09-09T01:46:40")
        json.dumps(dumpable)  # assert doesn't throw :)
        wall_roundtripped = SimpleDumper.cast(Wall, dumpable)
        self.assertDiffs(wall_one, wall_roundtripped, {})
        self.assertDiffs(wall_one, Wall(dumpable), {})

    def test_intro_example(self):
        self.assertEqual(
            SimpleDumper.visit(acent),
            {'name': 'Alpha Centauri',
             'components': [{'hip_id': 71683, 'name': 'Alpha Centauri A'},
                            {'hip_id': 71681, 'name': 'Alpha Centauri B'},
                            {'hip_id': 70890, 'name': 'Alpha Centauri C'}]},
        )
        dumped = SimpleDumper.visit(acent)
        self.assertDiffs(acent, StarSystem(dumped), {})
        self.assertDiffs(acent, SimpleDumper.cast(StarSystem, dumped), {})
