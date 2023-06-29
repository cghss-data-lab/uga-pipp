import os
from dotenv import load_dotenv

import torch
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
from sklearn import preprocessing
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import euclidean_distances
import seaborn as sns

load_dotenv()

# %% neo4j login config
URI = os.environ["URI"]
USER = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]


driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
label_encoder = preprocessing.LabelEncoder()

with driver.session(database=DATABASE) as session:
    result = session.run(
        "MATCH (s)-[r]->(t) RETURN id(s) as source,  id(t) as target, type(r) as relationship"
    )
    data = pd.DataFrame([r.values() for r in result], columns=result.keys())

data = np.concatenate([data.source.values, data.target.values])
label_encoder.fit(data)


with driver.session(database=DATABASE) as session:
    geography = session.run("MATCH (n:Geography)<-[r]->(t) Return id(n)")
    taxon = session.run("MATCH (n:Taxon)<-[r]->(t) Return id(n)")
    gmpd = session.run("MATCH (n:Report)<-[r]->(t) Return id(n)")
    flunet = session.run("MATCH (n:FluNet)<-[r]->(t) Return id(n)")

    geography = [x.values()[0] for x in geography]
    taxon = [x.values()[0] for x in taxon]
    gmpd = [x.values()[0] for x in gmpd]
    flunet = [x.values()[0] for x in flunet]

def create_vectors(model) -> tuple[torch.tensor]:
    geo = label_encoder.transform(torch.tensor(geography))
    geo = torch.tensor(geo)
    geo = model.node_emb(geo).detach().numpy()

    tax = label_encoder.transform(torch.tensor(taxon))
    tax = torch.tensor(tax)
    tax = model.node_emb(tax).detach().numpy()

    gm = label_encoder.transform(torch.tensor(gmpd))
    gm = torch.tensor(gm)
    gm = model.node_emb(gm).detach().numpy()

    flu = label_encoder.transform(torch.tensor(flunet))
    flu = torch.tensor(flu)
    flu = model.node_emb(flu).detach().numpy()

    return geo, tax, gm, flu


def create_entity_plot(model) -> None:
    fig, ax = plt.subplots()
    ax.scatter(geo[:, 0], geo[:, 1], c="#f37324", s=10, label="Geography")
    ax.scatter(tax[:, 0], tax[:, 1], c="#f8cc1b", s=10, label="Taxon")
    ax.scatter(gm[:, 0], gm[:, 1], c="#72b043", s=10, label="GMPD")
    ax.scatter(flu[:, 0], flu[:, 1], c="#4372b0", s=10, label="FluNet")
    ax.legend()
    plt.xticks([])
    plt.yticks([])
    plt.savefig(f"./{model}.png", dpi=350)


geo, tax, gm, flu = create_vectors(rotate_model)
create_entity_plot("rotate")


with driver.session(database=DATABASE) as session:
    countries = session.run(
        "MATCH (g:Geography)<-[r:CONTAINS_GEO]-(t:Geography) WHERE g.fcode = 'PCLI' RETURN id(g), g.name, t.name"
    )
    countries = pd.DataFrame([r.values() for r in countries], columns=countries.keys())

used_columns = countries["t.name"].isin(["Africa"])
countries = countries.loc[used_columns]
countries_le = label_encoder.transform(countries["id(g)"])
countries_vec = rotate_model.node_emb(torch.tensor(countries_le))

similarity = euclidean_distances(countries_vec.detach().numpy())

df = pd.DataFrame(similarity, index=[countries["t.name"], countries["g.name"]])

g = sns.clustermap(
    df,
    center=2,
    cmap="vlag",
    dendrogram_ratio=(0.1, 0.2),
    cbar_pos=(0.02, 0.32, 0.03, 0.2),
    linewidths=0.75,
    figsize=(16, 13),
    xticklabels=False,
    yticklabels=1,
)
g.ax_row_dendrogram.remove()

with driver.session(database=DATABASE) as session:
    kingdoms = session.run("MATCH (k:Kingdom) RETURN id(k), k.name")
    kingdoms = pd.DataFrame([r.values() for r in kingdoms], columns=kingdoms.keys())

kingdoms_le = label_encoder.transform(kingdoms["id(k)"])
kingdoms_vec = rotate_model.node_emb(torch.tensor(kingdoms_le))

similarity = euclidean_distances(kingdoms_vec.detach().numpy())

df = pd.DataFrame(similarity, index=[kingdoms["k.name"]])

g = sns.clustermap(
    df,
    cmap="vlag",
    center=0.4,
    dendrogram_ratio=(0.1, 0.2),
    cbar_pos=(0.02, 0.32, 0.03, 0.2),
    linewidths=0.75,
    figsize=(16, 13),
    xticklabels=False,
)
g.ax_row_dendrogram.remove()
