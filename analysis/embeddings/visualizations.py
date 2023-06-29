import os
from dotenv import load_dotenv

import torch
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
from sklearn import preprocessing
import pandas as pd
import numpy as np

load_dotenv()

# %% neo4j login config
URI = os.environ["URI"]
USER = os.environ["AUTH"]
PASSWORD = os.environ["PASSWORD"]
DATABASE = os.environ["DATABASE"]


driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
