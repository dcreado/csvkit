#!/usr/bin/env python

# globals for eval
# noinspection PyUnresolvedReferences
import re
# noinspection PyUnresolvedReferences
import math
# noinspection PyUnresolvedReferences
import random
# noinspection PyUnresolvedReferences
import collections
# noinspection PyUnresolvedReferences
from rex import rex
# noinspection PyUnresolvedReferences
import datetime
# noinspection PyUnresolvedReferences
import uuid
# noinspection PyUnresolvedReferences
import json

class ScriptCSVReader(object):
    """
    Given any row iterator returns header and rows after evaluation series of scripts on them.
    Each script produces a value which is concatenated as another column in csv.
    """
    returned_header = False
    column_names = None

    def __init__(self, reader, scripts, zero_based=False):
        super(ScriptCSVReader, self).__init__()
        self.zero_based = zero_based
        self.reader = reader
        self.scripts = scripts
        self.compiled_scripts = map(lambda (i, script): (script[0], compile(script[1], 'script-%d' % i, 'eval')),
                                    enumerate(self.scripts))
        self.column_names = reader.next()
        self.line_number = 0

    def __iter__(self):
        return self

    def next(self):
        if self.column_names and not self.returned_header:
            self.returned_header = True
            return self.column_names + map(lambda script: script[0], self.scripts)

        while True:
            row = self.reader.next()
            self.line_number += 1
            return row + list(run_scripts(self.compiled_scripts, row, self.column_names, self.zero_based, self.line_number))

        raise StopIteration()


def run_scripts(scripts, row, column_names, zero_based, line_number):
    for script in scripts:
        yield eval(script[1], globals(), {
            'c': row if zero_based else dict((i + 1, r) for i, r in enumerate(row)),
            'ch': dict(zip(column_names, row)),
            'line_number': line_number
        }
        )

