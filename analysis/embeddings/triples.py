import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import pandas as pd
import torch
from torch_geometric.data import Dataset, Data
from numpy import append, unique
from sklearn import preprocessing


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
        root="./analysis/data",
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
        return ["triple_data_0.pt"]

    def download(self):
        pass

    def process(self):
        self.data = self._run_query(
            "MATCH (s)-[r]->(t) RETURN id(s) as source,  id(t) as target, type(r) as relationship"
        )

        edge_type, edge_name = self._create_type_tensor()
        data = Data(
            edge_index=self._create_coo(),
            edge_type=edge_type,
            num_nodes=self._number_nodes(),
            num_edge_types=self._number_edges(),
            edge_names=edge_name,
        )
        torch.save(data, os.path.join(self.processed_dir, "triple_data_0.pt"))

    def len(self) -> int:
        return len(self.processed_file_names)

    def get(self, idx):
        data = torch.load(os.path.join(self.processed_dir, f"triple_data_{idx}.pt"))
        return data

    @classmethod
    def _run_query(cls, query) -> pd.DataFrame:
        with cls.driver.session(database=DATABASE) as session:
            result = session.run(query)
            return pd.DataFrame([r.values() for r in result], columns=result.keys())

    def _create_coo(self) -> torch.Tensor:
        label_encoder = preprocessing.LabelEncoder()
        source = torch.as_tensor(
            label_encoder.fit_transform(self.data["source"])
        ).unsqueeze(1)
        target = torch.as_tensor(
            label_encoder.fit_transform(self.data["target"])
        ).unsqueeze(1)
        return torch.cat((source, target), 1).T

    def _create_type_tensor(self) -> torch.Tensor:
        label_encoder = preprocessing.LabelEncoder()
        edge_type = label_encoder.fit_transform(self.data["relationship"])
        return torch.as_tensor(edge_type), label_encoder.classes_

    def _number_nodes(self) -> int:
        return len(unique(append(self.data.source.values, self.data.target.values)))

    def _number_edges(self) -> int:
        return len(pd.unique(self.data["relationship"]))
