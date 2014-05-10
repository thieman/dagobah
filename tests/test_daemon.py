from dagobah.daemon import daemon

def test_replace_nones():
    d = {'a': 'NONE', 'b': ['c', 'None', 'd', {'e': {'f': 'none'}}, ['None']], 'c': 5, 'd': 'None'}
    daemon.replace_nones(d)
    assert d == {'a': None,
                 'b': ['c', None, 'd', {'e': {'f': None}}, [None]],
                 'c': 5,
                 'd': None}
