from collections import Counter


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
