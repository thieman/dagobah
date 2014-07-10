""" DAG implementation used in job classes """

from copy import copy, deepcopy

class DAGValidationError(Exception):
    pass

class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        """ Construct a new DAG with no nodes or edges. """
        self.graph = {}


    def add_node(self, node_name):
        """ Add a node if it does not exist yet, or error out. """
        if node_name in self.graph:
            raise KeyError('node %s already exists' % node_name)
        self.graph[node_name] = set()


    def delete_node(self, node_name):
        """ Deletes this node and all edges referencing it. """
        if node_name not in self.graph:
            raise KeyError('node %s does not exist' % node_name)
        self.graph.pop(node_name)

        for node, edges in self.graph.iteritems():
            if node_name in edges:
                edges.remove(node_name)


    def add_edge(self, ind_node, dep_node):
        """ Add an edge (dependency) between the specified nodes. """
        if ind_node not in self.graph or dep_node not in self.graph:
            raise KeyError('one or more nodes do not exist in graph')
        test_graph = deepcopy(self.graph)
        test_graph[ind_node].add(dep_node)
        is_valid, message = self.validate(test_graph)
        if is_valid:
            self.graph[ind_node].add(dep_node)
        else:
            raise DAGValidationError()


    def delete_edge(self, ind_node, dep_node):
        """ Delete an edge from the graph. """
        if dep_node not in self.graph.get(ind_node, []):
            raise KeyError('this edge does not exist in graph')
        self.graph[ind_node].remove(dep_node)


    def rename_edges(self, old_task_name, new_task_name):
        """ Change references to a task in existing edges. """
        for node, edges in self.graph.iteritems():

            if node == old_task_name:
                self.graph[new_task_name] = copy(edges)
                del self.graph[old_task_name]

            else:
                if old_task_name in edges:
                    edges.remove(old_task_name)
                    edges.add(new_task_name)


    def downstream(self, node, graph):
        """ Returns a list of all nodes this node has edges towards. """
        if graph is None:
            raise Exception("Graph given is None")
        if node not in self.graph:
            raise KeyError('node %s is not in graph' % node)
        return list(self.graph[node])


    def from_dict(self, graph_dict):
        """ Reset the graph and build it from the passed dictionary.

        The dictionary takes the form of {node_name: [directed edges]}
        """

        self.reset_graph()
        for new_node in graph_dict.iterkeys():
            self.add_node(new_node)
        for ind_node, dep_nodes in graph_dict.iteritems():
            if not isinstance(dep_nodes, list):
                raise TypeError('dict values must be lists')
            for dep_node in dep_nodes:
                self.add_edge(ind_node, dep_node)


    def reset_graph(self):
        """ Restore the graph to an empty state. """
        self.graph = {}


    def ind_nodes(self, graph):
        """ Returns a list of all nodes in the graph with no dependencies. """
        if graph is None:
            raise Exception("Graph given is None")
        all_nodes, dependent_nodes = set(graph.keys()), set()
        for downstream_nodes in graph.itervalues():
            [dependent_nodes.add(node) for node in downstream_nodes]
        return list(all_nodes - dependent_nodes)


    def validate(self, graph=None):
        """ Returns (Boolean, message) of whether DAG is valid. """
        graph = graph if graph is not None else self.graph
        if len(self.ind_nodes(graph)) == 0:
            return (False, 'no independent nodes detected')
        try:
            self._topological_sort(graph)
        except ValueError:
            return (False, 'failed topological sort')
        return (True, 'valid')


    def _dependencies(self, target_node, graph):
        """ Returns a list of all nodes from incoming edges. """
        if graph is None:
            raise Exception("Graph given is None")
        result = set()
        for node, outgoing_nodes in graph.iteritems():
            if target_node in outgoing_nodes:
                result.add(node)
        return list(result)


    def _topological_sort(self, graph=None):
        """ Returns a topological ordering of the DAG.

        Raises an error if this is not possible (graph is not valid).
        """
        graph = deepcopy(graph if graph is not None else self.graph)
        l = []
        q = deepcopy(self.ind_nodes(graph))
        while len(q) != 0:
            n = q.pop(0)
            l.append(n)
            iter_nodes = deepcopy(graph[n])
            for m in iter_nodes:
                graph[n].remove(m)
                if len(self._dependencies(m, graph)) == 0:
                    q.append(m)

        if len(l) != len(graph.keys()):
            raise ValueError('graph is not acyclic')
        return l
