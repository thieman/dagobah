""" Tests on the DAG implementation """

from nose import with_setup
from nose.tools import nottest, raises
from dagobah.core import DAG, DAGValidationError

dag = None

@nottest
def blank_setup():
    global dag
    dag = DAG()


@nottest
def start_with_graph():
    global dag
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['d'],
                   'd': []})

@with_setup(blank_setup)
def test_add_node():
    dag.add_node('a')
    assert dag.graph == {'a': set()}


@with_setup(blank_setup)
def test_add_edge():
    dag.add_node('a')
    dag.add_node('b')
    dag.add_edge('a', 'b')
    assert dag.graph == {'a': set('b'), 'b': set()}


@with_setup(blank_setup)
def test_from_dict():
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['d'],
                   'd': []})
    assert dag.graph == {'a': set(['b', 'c']),
                         'b': set('d'),
                         'c': set('d'),
                         'd': set()}


@with_setup(blank_setup)
def test_reset_graph():
    dag.add_node('a')
    assert dag.graph == {'a': set()}
    dag.reset_graph()
    assert dag.graph == {}


@with_setup(start_with_graph)
def test_ind_nodes():
    assert dag.ind_nodes(dag.graph) == ['a']


@with_setup(start_with_graph)
def test_dependent_on():
    assert set(dag._dependencies('d',dag.graph)) == set(['b', 'c'])


@with_setup(blank_setup)
def test_topological_sort():
    dag.from_dict({'a': [],
                   'b': ['a'],
                   'c': ['b']})
    assert dag._topological_sort() == ['c', 'b', 'a']


@with_setup(start_with_graph)
def test_successful_validation():
    assert dag.validate()[0] == True


@raises(DAGValidationError)
@with_setup(blank_setup)
def test_failed_validation():
    dag.from_dict({'a': ['b'],
                   'b': ['a']})

@with_setup(start_with_graph)
def test_downstream():
    assert set(dag.downstream('a', dag.graph)) == set(['b', 'c'])
