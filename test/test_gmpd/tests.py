from collections import Counter
from driver.create_query import count_taxons


def is_node_accurate(row: dict, node: dict) -> bool:
    prunned_row = {
        "collected": row["NumSamples"],
        "dataSourceRow": row[
            ""
        ],  # Row is numbered starting at 0, but file starts at 1.
        # This will be changed so the file starts at 0
        "detectionType": row["SamplingBasis"],
        "prevalence": row["Prevalence"],
        "reference": row["Citation"],
    }
    node.pop("dataSource", None)
    return Counter(list(prunned_row)) == Counter(list(node))


def is_node_connected_to_taxa(node: dict) -> bool:
    result = count_taxons("GMPD", node[""])
    count = dict(result)["count(t)"]
    if count < 1:
        return True
    return False
