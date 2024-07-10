import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt


def leggi_parquet(path):
    return pd.read_parquet(path)


ds = leggi_parquet("challenge_campus_biomedico_2024.parquet")

print(ds.iloc[0])


descrizioni = ds['descrizione_attivita']

# Conto il numero di descrizioni, ovvero quanti tipi di teleassistenza ci sono.
num_descrizioni = descrizioni.nunique()
print(num_descrizioni)

# Stampo tutte le diverse descrizioni
print(descrizioni.unique())

