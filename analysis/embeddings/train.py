import argparse
from torch_geometric.nn.kge import TransE
from torch.optim import Adam
from triples import Triples


parser = argparse.ArgumentParser()
parser.add_argument("--batch", type=str.lower, required=True)
parser.add_argument("--lr", type=float, required=True)
parser.add_argument("--epochs", type=int, required=True)
parser.add_argument("--weight", type=float, required=False, default=0.0)
args = parser.parse_args()

# %% Load data
data = Triples()[0]

# %% Set model
model = TransE(
    num_nodes=data.num_nodes,
    num_relations=data.num_edge_types,
    hidden_channels=2,
)

loader = model.loader(
    head_index=data.edge_index[0],
    rel_type=data.edge_type,
    tail_index=data.edge_index[1],
    batch_size=args.batch,
    shuffle=True,
)

optimizer = Adam(model.parameters(), lr=args.lr, weight_decay=args.weight)

# %% Train model
model.train()
for epoch in range(args.epochs):
    for source, relationship, target in loader:
        optimizer.zero_grad()
        loss = model.loss(source, relationship, target)
        loss.backward()
        optimizer.step()
