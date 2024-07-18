import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import pandas as pd
import torch
from torch_geometric.data import Dataset, Data
import numpy as np
from sklearn import preprocessing
from sklearn.model_selection import train_test_split


load_dotenv()

# %% neo4j login config
URI = os.environ["URI"]
USER = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]


class Triples(Dataset):
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    def __init__(
        self,
        root="./analysis/triples_data",
        transform=None,
        pre_transform=None,
        pre_filter=None,
    ):
        super().__init__(root, transform, pre_transform, pre_filter)

    @property
    def raw_file_names(self) -> str:
        return "not implemented"

    @property
    def processed_file_names(self) -> str:
        return [
            "triple_data_train.pt",
            "triple_data_test.pt",
        ]

    def download(self):
        pass

    def process(self):
        self.data = self._run_query(
            "MATCH (s)-[r]->(t) RETURN id(s) as source,  id(t) as target, type(r) as relationship"
        )
        number_nodes = self._label_nodes()
        edge_type = self._label_edges()
        train_dataset, test_dataset = train_test_split(
            self.data, train_size=0.9, test_size=0.1, random_state=1001
        )
        
        train = Data(
            edge_index=self._create_coo(train_dataset),
            edge_type=self._create_type_tensor(train_dataset),
            num_nodes=number_nodes,
            num_edge_types=len(edge_type),
            edge_names=edge_type,
        )

        
        test = Data(
            edge_index=self._create_coo(test_dataset),
            edge_type=self._create_type_tensor(test_dataset),
            num_nodes=number_nodes,
            num_edge_types=len(edge_type),
            edge_names=edge_type,
        )
        
        torch.save(train, os.path.join(self.processed_dir, "triple_data_train.pt"))
        torch.save(test, os.path.join(self.processed_dir, "triple_data_test.pt"))

    def len(self) -> int:
        return len(self.processed_file_names)

    def get(self, dataset):
        data = torch.load(os.path.join(self.processed_dir, f"triple_data_{dataset}.pt"))
        return data

    @classmethod
    def _run_query(cls, query) -> pd.DataFrame:
        with cls.driver.session(database=DATABASE) as session:
            result = session.run(query)
            return pd.DataFrame([r.values() for r in result], columns=result.keys())

    ## Preprocess results from Neo4j
    def _label_nodes(self) -> None:
        label_encoder = preprocessing.LabelEncoder()
        concatenated_values = np.concatenate(
            [self.data.source.values, self.data.target.values]
        )
        label_encoder.fit(concatenated_values)

        self.data["le_source"] = label_encoder.transform(self.data["source"])
        self.data["le_target"] = label_encoder.transform(self.data["target"])
        return len(label_encoder.classes_)

    def _label_edges(self) -> torch.Tensor:
        label_encoder = preprocessing.LabelEncoder()
        self.data["le_relationship"] = label_encoder.fit_transform(
            self.data["relationship"]
        )
        return label_encoder.classes_

    def _create_coo(self, dataset) -> torch.Tensor:
        source = torch.as_tensor(dataset["le_source"].values).unsqueeze(1)
        target = torch.as_tensor(dataset["le_target"].values).unsqueeze(1)
        return torch.cat((source, target), 1).T

    def _create_type_tensor(self, dataset) -> torch.Tensor:
        return torch.as_tensor(dataset["le_relationship"].values)
