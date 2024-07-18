import argparse
import torch
from torch.optim import Adam, Adagrad
from triples import Triples
from torch.utils.tensorboard import SummaryWriter
import matplotlib.pyplot as plt
from torch_geometric.nn.kge import TransE, DistMult, ComplEx
from rotate import RotatE


parser = argparse.ArgumentParser()
parser.add_argument("--model", type=str, required=True)
parser.add_argument("--optim", type=str, required=True)
parser.add_argument("--batch", type=int, required=True)
parser.add_argument("--lr", type=float, required=True)
parser.add_argument("--epochs", type=int, required=True)
parser.add_argument("--weight", type=float, required=False, default=0.0)
args = parser.parse_args()

# %% Load data
data = Triples()
training_set = data.get("train")
test_set = data.get("test")


models = {"transe": TransE, "distmul": DistMult, "complex": ComplEx, "rotate": RotatE}

# %% Set model
model = models[args.model](
    num_nodes=training_set.num_nodes,
    num_relations=training_set.num_edge_types,
    hidden_channels=2,
)

loader = model.loader(
    head_index=training_set.edge_index[0],
    rel_type=training_set.edge_type,
    tail_index=training_set.edge_index[1],
    batch_size=args.batch,
    shuffle=True,
)

optimizers = {
    "adam": Adam(model.parameters(), lr=args.lr, weight_decay=args.weight),
    "adagrad": Adagrad(model.parameters(), lr=args.lr, weight_decay=args.weight),
}

optimizer = optimizers[args.optim]


@torch.no_grad()
def test(dataset) -> tuple[float, float]:
    model.eval()
    return model.test(
        head_index=dataset.edge_index[0],
        rel_type=dataset.edge_type,
        tail_index=dataset.edge_index[1],
        batch_size=2000,
        k=20,
    )

# %% Train model
model.train()
for epoch in range(args.epochs + 1):
    for source, relationship, target in loader:
        optimizer.zero_grad()
        loss = model.loss(source, relationship, target)
        loss.backward()
        optimizer.step()
