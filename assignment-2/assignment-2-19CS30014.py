import sys
import random
import itertools

infinity = 1e9


class SpecialNode:
    """
        - This class is used to represent a node in the graph.
        - It is used to keep track of the nodes which are merged together
        - Implements all the necessary magic methods to be used as a key in a dictionary
        - autoincrements the id of the node every time a new node is created
    """
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
    """
        - This class is used to represent the graph
        - It is used to keep track of the edges in the graph
        - It is used to keep track of the nodes in the graph
        - It is used to keep track of the number of edges between two nodes
        - It is used to run the Karger's algorithm by contracting edges in the graph
    """
    def __init__(self, edges:list):
        self.graph = {}
        self.vertex_map = {}
        self.vertex_min = infinity
        self.vertext_max = -infinity
        self.edge_count = {}
        self.edgeset = set()
        self.construct_graph(edges)

    def construct_graph(self, edges:list):
        """
            - This function is used to construct the graph from the given edges
        """
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

    def reorder_nodes(self, u:SpecialNode, v:SpecialNode):
        """
            - This function is used to reorder the nodes in the graph
        """
        if u > v:
            u, v = v, u
        return u, v

    def get_edge_count(self, u:SpecialNode, v:SpecialNode):
        """
            - This function is used to get the number of edges between two nodes
        """
        u, v = self.reorder_nodes(u, v)
        return self.edge_count[(u, v)]

    def update_edge_count(self, u:SpecialNode, v:SpecialNode, value):
        """
            - This function is used to update the number of edges between two nodes
        """
        u, v = self.reorder_nodes(u, v)
        
        if (u, v) not in self.edge_count:
            self.edge_count[(u, v)] = 0
        
        self.edge_count[(u, v)] += value

        if self.edge_count[(u, v)] == 0:
            self.edge_count.pop((u, v))

    def add_edge(self, u:SpecialNode, v:SpecialNode, edge_count:int=1):
        """
            - This function is used to add an edge between two nodes and update edge_count
        """
        u, v = self.reorder_nodes(u, v)

        if u not in self.graph:
            self.graph[u] = set()
        if v not in self.graph:
            self.graph[v] = set()

        self.graph[u].add(v)
        self.graph[v].add(u)
        self.edgeset.add((u, v))
        self.update_edge_count(u, v, edge_count)

    def remove_edge(self, u: SpecialNode, v: SpecialNode):
        """
            - This function is used to remove an edge between two nodes and update edge_count
        """
        u, v = self.reorder_nodes(u, v)

        self.graph[u].remove(v)
        self.graph[v].remove(u)
        self.edgeset.remove((u, v))
        self.update_edge_count(u, v, -self.get_edge_count(u, v))

    def contract_edge(self, a:SpecialNode, b:SpecialNode):
        """
            - This function is used to contract an edge between two nodes
        """
        a, b = self.reorder_nodes(a, b)
        
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
            self.add_edge(node, new_node, self.get_edge_count(node, a))

        for node in outgoing_from_b:
            self.add_edge(node, new_node, self.get_edge_count(node, b))

        # Remove the edges from a and b to other nodes
        for node in outgoing_from_a:
            self.remove_edge(node, a)
        
        for node in outgoing_from_b:
            self.remove_edge(node, b)

        # Remove the old nodes
        self.graph.pop(a)
        self.graph.pop(b)

    def get_random_edge(self):
        """
            - This function is used to get a random edge from the graph
            - Used weighted random probability to obtain the random edge as shown in class
        """
        return random.choices(
            population=list(self.edge_count.keys()),
            weights=list(self.edge_count.values()),
            k=1,
        )[0]
        
    
    def run_algorithm(self):
        """
            - This function is used to run the Karger's algorithm and return results
        """
        while len(self.graph) > 2:
            # print("Before contraction")
            # print(self.graph)
            u, v = self.get_random_edge()
            
            # print("Contracting edge")
            # print(u,v)
            self.contract_edge(u, v)
            
            # print("After contraction")
            # print(self.graph)

        assert(len(self.edgeset) == 1)
        assert(len(self.edge_count) == 1)
        edge = list(self.edgeset)[0]
        min_cut = self.get_edge_count(edge[0], edge[1])
        return edge, min_cut

def main():
    try:
        edgelist_file = sys.argv[1]
    except:
        print("Please provide a valid input filename as command line argument")
        exit(0)

    graph_edgelist = []
    with open(edgelist_file, 'r') as f:
        graph_edgelist = [ list(map(int, line.strip().split())) for line in f  if line.strip() != "" ]

    # kargerMincutSimulator = KargerMincut(edgelist)
    # print(kargerMincutSimulator.graph)

    min_cut = infinity
    result = None
    for i in range(3):
        print(f"Running iteration-{i+1} ...")
        kargerMincutSimulator = KargerMincut(graph_edgelist)
        edge, current_min_cut = kargerMincutSimulator.run_algorithm()
        if current_min_cut < min_cut:
            min_cut = current_min_cut
            result = edge

    # print(min_cut)
    print(f"Value of probable mincut = {min_cut}") 
    community = {}
    for i in range(2):
        special_node:SpecialNode = result[i]
        for x in special_node.get_node_values():
            community[x] = i+1
    
    print("Node-ID      Community-ID")
    print("----------------------")
    for node in sorted(list(community.keys())):
        print(f"{node}              {community[node]}")


if __name__ == '__main__':
    main()
    