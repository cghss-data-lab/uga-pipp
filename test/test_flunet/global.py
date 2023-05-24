from driver.create_query import count_row_numbers, node_duplication
from .errors import DuplicationError


def validate_flunet_row_duplicates(neo4j_driver) -> None:
    """
    Function validates there are no data source row number duplicates.
    """
    query_row_number = count_row_numbers("FluNet")
    row_number_count_result = neo4j_driver.run_query(query_row_number)
    if row_number_count_result:
        for row in row_number_count_result:
            row_number = dict(row)[row]
            raise DuplicationError(values=row_number, message="Duplicated row number.")


def validate_flunet_row_duplicates(neo4j_driver) -> None:
    """
    Function validates there are no duplicated nodes.
    """
    query_nodes = node_duplication("FluNet")
    node_duplicate_result = neo4j_driver.run_query(query_nodes)

    nodes = [
        {node["n"]._element_id: node["n"]._properties} for node in node_duplicate_result
    ]

    unique_nodes = []
    for node in nodes:
        # Transoforms the dictionary into a tuple of tuples,
        # each element is (key, value) from the dictionary
        item = tuple(sorted(node.items()))
        if item not in unique_nodes:
            unique_nodes.append(node)
        else:
            raise DuplicationError(values=node.keys(), message="Duplicated node.")
