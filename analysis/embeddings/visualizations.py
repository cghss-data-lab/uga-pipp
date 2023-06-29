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
