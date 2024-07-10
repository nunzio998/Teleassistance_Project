import pandas as pd
import sklearn as sk
import matplotlib as plt


def leggi_parquet(path):
    print("leggo parquet..\n")
    return pd.read_parquet(path)


ds = leggi_parquet("challenge_campus_biomedico_2024.parquet")

# Fase 1: Inizio il pre-processing

# Elimino le features che non mi interessano: per ora quelle relative alla residenza del paziente

ds = ds.drop(columns=['regione_residenza', 'codice_regione_residenza', 'asl_residenza', 'codice_asl_residenza',
                      'provincia_residenza', 'codice_provincia_residenza', 'comune_residenza',
                      'codice_comune_residenza'])
print("eliminazione features completata\n")

# Gestisco i missing values

ds = ds.dropna(subset=['data_nascita', 'sesso', 'tipologia_servizio',
                       'tipologia_servizio', 'descrizione_attivita', 'codice_descrizione_attivita',
                       'data_contatto', 'regione_erogazione', 'codice_regione_erogazione', 'asl_erogazione',
                       'codice_asl_erogazione', 'provincia_erogazione', 'codice_provincia_erogazione',
                       'struttura_erogazione', 'codice_struttura_erogazione', 'tipologia_struttura_erogazione',
                       'codice_tipologia_struttura_erogazione', 'id_professionista_sanitario',
                       'tipologia_professionista_sanitario', 'codice_tipologia_professionista_sanitario',
                       'data_erogazione', 'ora_inizio_erogazione', 'ora_fine_erogazione'])

print(ds.head())

# Gestione variabili categoric  he e creazione variabili dummy

ds = pd.get_dummies(ds, columns=['sesso'])

print(ds.iloc[1])

