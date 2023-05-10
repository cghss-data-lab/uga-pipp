from collections import Counter


def is_node_accurate(row: dict, node: dict) -> bool:
    prunned_row = {
        "collected": row["NumSamples"],
        "dataSourceRow": row[""],
        "detectionType": row["SamplingBasis"],
        "prevalence": row["Prevalence"],
        "reference": row["Citation"],
    }

    return Counter(list(prunned_row)) == Counter(list(node))
