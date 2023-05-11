import collections
from datetime import datetime

flunet_to_ncbi = {}
with open("./flunet/data/flunet_to_ncbi.csv", "r") as flunet_ncbi:
    for record in flunet_ncbi:
        key, value = record.split(",")
        value = value.strip()
        flunet_to_ncbi[value] = key


def is_flunet_node_accurate(row: dict, node: dict) -> bool:
    keys = [
        "",
        "Start date",
        # "End date",
        "Collected",
        "Processed",
        "Total positive",
        "Total negative",
    ]

    node["start"] = node["start"].strftime("%m/%d/%y")

    node.pop("dataSource", None)  # Remove data source
    node.pop("duration", None)  # Remove duration
    node_dictionary = {key: row[key] for key in keys}
    node_dictionary["Start date"] = datetime.strptime(
        node_dictionary["Start date"], "%m/%d/%y"
    ).strftime("%m/%d/%y")
    # Empty strings to zero
    node_dictionary = {
        key: ("0" if value == "" else value) for key, value in node_dictionary.items()
    }
    node_dictionary_values = node_dictionary.values()
    node = [str(element) for element in node.values()]
    return collections.Counter(list(node_dictionary_values)) == collections.Counter(
        list(node)
    )


def is_collected_null(line_data: dict) -> bool:
    return line_data["Collected"] in ("", "0", None)


def is_line_null(line_data: dict) -> bool:
    # Verify if line is all empty or all zeros
    keys = (
        "A (H1)",
        "A (H1N1)pdm09",
        "A (H3)",
        "A (H5)",
        "A (not subtyped)",
        "A (total)",
        "B (Yamagata)",
        "B (Victoria)",
        "B (not subtyped)",
        "B (total)",
        "Total positive",
        "Total negative",
    )
    return all(line_data[key] in ("", "0", None) for key in keys)


def is_strain_accurate(row: dict, strain_name: str) -> bool:
    strain = flunet_to_ncbi[strain_name]
    if row[strain] != 0:
        return True
    return False


def test_flunet_line_data(csv_row: dict, query_result: list) -> dict:
    # Verify node exists
    accuracy = {}
    is_node_checked = False
    for result in query_result:
        node, relationship, adjacent_node, type_relationship = result.values()
        # Check the primary node
        if not is_node_checked:
            node_accuracy = is_flunet_node_accurate(csv_row, dict(node))
            accuracy["node"] = node_accuracy
            is_node_checked = True
        # Check territorial scope
        if type_relationship == "IN":
            territory = csv_row["Territory"]
            territory_name = dict(adjacent_node)["name"]
            accuracy["territory"] = territory == territory_name
        # Check reported nodes
        if "host" not in relationship and type_relationship == "REPORTS":
            adj_name = adjacent_node["name"]
            adj_accuracy = is_strain_accurate(csv_row, adj_name)
            accuracy[adj_name] = adj_accuracy
    return accuracy
