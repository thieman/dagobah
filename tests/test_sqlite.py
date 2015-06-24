""" Tests on the SQLite backend """

import os
import datetime
import json
import yaml

from nose.tools import nottest

from dagobah.core.dagobah import Dagobah
from dagobah.core.components import StrictJSONEncoder
from dagobah.backend.sqlite import SQLiteBackend


class TestSQLite(object):

    @classmethod
    def setup_class(self):
        location = os.path.realpath(os.path.join(os.getcwd(),
                                                 os.path.dirname(__file__)))
        config_file = open(os.path.join(location, 'test_config.yml'))
        config = yaml.load(config_file.read())
        config_file.close()

        if os.getenv('TRAVIS', 'false') == 'true':
            self.filepath = 'memory'
        else:
            self.filepath = config.get('SQLiteBackend', {}).\
                get('filepath', 'memory')

        self.dagobah = None

    @classmethod
    def teardown_class(self):
        pass

    @nottest
    def new_dagobah(self, return_instance=False):
        if not return_instance:
            self.dagobah = Dagobah(SQLiteBackend(self.filepath))
        else:
            return Dagobah(SQLiteBackend(self.filepath))

    def test_decode_json(self):
        self.new_dagobah()
        now = datetime.datetime.now()
        test_doc = {"nested": {"dt": now},
                    "array": [{"dt": now},
                              {"dt2": now,
                               "int": 5},
                              {"str": "woot"}]}
        json_doc = json.dumps(test_doc, cls=StrictJSONEncoder)
        result = self.dagobah.backend.decode_import_json(json_doc)
        print test_doc
        print result
        assert result == test_doc
