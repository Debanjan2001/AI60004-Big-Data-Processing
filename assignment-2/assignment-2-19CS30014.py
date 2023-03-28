import sys
import random
import itertools

infinity = 1e9

class SpecialNode:

    id_generator = itertools.count()

    def __init__(self, values:list):
        self.id = next(SpecialNode.id_generator)
        self.values = values

    def get_node_values(self):
        return self.values
    
    def combine(self, anotherNode):
        return SpecialNode(self.values + anotherNode.get_node_values())
    
    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return (self.get_node_values() == other.get_node_values())

    def __ne__(self, other):
        return not(self == other)
    
    def __lt__(self, other):
        return self.id < other.id
    
    def __gt__(self, other):
        return self.id > other.id
    
    def __le__(self, other):
        return self.id <= other.id
    
    def __ge__(self, other):
        return self.id >= other.id

    def __repr__(self):
        return f"{self.values}"
    
    def __str__(self):
        return f"{self.values}"


class KargerMincut:
    def __init__(self, edges:list):
        self.graph = {}
        self.vertex_map = {}
        self.vertex_min = infinity
        self.vertext_max = -infinity
        self.edge_count = {}
        self.edgeset = set()
        self.construct_graph(edges)

    def construct_graph(self, edges:list):
        self.graph = {}
        for edge in edges:
            u, v = edge
            if u not in self.vertex_map:
                self.vertex_map[u] = SpecialNode([u])
            if v not in self.vertex_map:
                self.vertex_map[v] = SpecialNode([v])
            u = self.vertex_map[u]
            v = self.vertex_map[v]
            self.add_edge(u, v)

        self.vertex_min = min(self.vertex_map.keys())
        self.vertex_max = max(self.vertex_map.keys())

    def get_edge_count(self, u:SpecialNode, v:SpecialNode):
        if u > v:
            u, v = v, u
        return self.edge_count[(u, v)]

    def update_edge_count(self, u:SpecialNode, v:SpecialNode, value):
        if u > v:
            u, v = v, u
        
        if (u, v) not in self.edge_count:
            self.edge_count[(u, v)] = 0
        
        self.edge_count[(u, v)] += value

        if self.edge_count[(u, v)] == 0:
            self.edge_count.pop((u, v))

    def add_edge(self, u:SpecialNode, v:SpecialNode):
        if u > v:
            u, v = v, u
        if u not in self.graph:
            self.graph[u] = set()
        if v not in self.graph:
            self.graph[v] = set()

        self.graph[u].add(v)
        self.graph[v].add(u)
        self.edgeset.add((u, v))
        self.update_edge_count(u, v, 1)

    def remove_edge(self, u: SpecialNode, v: SpecialNode):
        if u > v:
            u, v = v, u
        self.graph[u].remove(v)
        self.graph[v].remove(u)
        self.edgeset.remove((u, v))
        self.update_edge_count(u, v, -self.get_edge_count(u, v))

    def contract_edge(self, a:SpecialNode, b:SpecialNode):
        if a > b:
            a, b = b, a
        # Remove the connection first

        self.remove_edge(a, b)
        # Generate the new node now and add it to the graph
        new_node = a.combine(b)
        self.graph[new_node] = set()

        # Keep connecting nodes which are connected 
        # by incident edges of node(a), node(b) to node(a,b)
        outgoing_from_a = list(self.graph[a])
        outgoing_from_b = list(self.graph[b])
        
        for node in outgoing_from_a:
            self.add_edge(node, new_node)

        for node in outgoing_from_b:
            self.add_edge(node, new_node)

        # Remove the edges from a and b to other nodes
        for node in outgoing_from_a:
            self.remove_edge(node, a)
        
        for node in outgoing_from_b:
            self.remove_edge(node, b)

        # Remove the old nodes
        self.graph.pop(a)
        self.graph.pop(b)

    def get_random_edge(self):
        edgelist = list(self.edgeset)
        weights = [self.get_edge_count(edge[0], edge[1]) for edge in edgelist]
        print(weights)
        return random.choices(
            population=edgelist,
            weights=weights,
            k=1,
        )[0]
    
    def run_algorithm(self):
        while len(self.graph) > 2:
            print("Before contraction")
            print(self.graph)
            u, v = self.get_random_edge()
            print("Contracting edge")
            print(u,v)
            self.contract_edge(u, v)
            
            print("After contraction")
            print(self.graph)
            print(list(self.edgeset))
            print(self.edge_count)
            print(50*"-")

            
        return list(self.graph.keys())

def main():
    try:
        edgelist_file = sys.argv[1]
    except:
        print("Please provide a valid input filename as command line argument")
        exit(0)

    edgelist = []
    with open(edgelist_file, 'r') as f:
        edgelist = [ map(int, line.strip().split())for line in f  if line.strip() != "" ]

    kargerMincutSimulator = KargerMincut(edgelist)
    # print(kargerMincutSimulator.graph)

    edgelist = kargerMincutSimulator.run_algorithm()

    print(edgelist)


if __name__ == '__main__':
    main()
    