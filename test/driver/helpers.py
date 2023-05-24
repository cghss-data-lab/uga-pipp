def split_nodes(nodes: list) -> tuple[dict, list]:
    """
    Query returns a list of triplets, main node is repeated.
    This functions splits the node and the adjacent nodes
    """
    flunet_node = None
    adjacent_nodes = []
    for node, adjacent_node, edge_type in nodes:
        if not flunet_node:
            flunet_node = node
        adjacent_nodes.append((adjacent_node, edge_type))
    return flunet_node, adjacent_nodes
