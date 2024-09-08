import pandas as pd


def remove_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove le feature poco significative dal DataFrame.
    :param df:
    :return: df senza le colonne specificate.

    #TODO da inserire in data cleaning/feature selection? N.B. alcune cose servono per il calcolo dell'incremento

    """
    df.drop(columns=['id_prenotazione', 'data_nascita', 'asl_residenza',
                     'comune_residenza', 'descrizione_attivita',  'data_contatto', 'data_erogazione',
                     'asl_erogazione', 'ora_inizio_erogazione', 'ora_fine_erogazione', 'codice_struttura_erogazione'], inplace=True)
    return df

def define_categorical_features():
    """
    Definisce le colonne delle feature categoriche.
    :return: List of categorical features
    """
    return [
        'id_paziente', 'provincia_residenza', 'regione_residenza', 'provincia_erogazione',
        'regione_erogazione', 'tipologia_struttura_erogazione', 'tipologia_professionista_sanitario',
        'id_professionista_sanitario', 'sesso'
    ]

def define_numerical_features():
    """
    Definisce le colonne delle feature numeriche.
    :return: List of numerical features
    """
    return ['eta_paziente', 'durata_televisita']



def execute_clustering(df):
    """
    Metodo che esegue tutti i metodi del file clustering_execution
    :param df:
    :return: df

    """

    df = remove_features(df)

    return df








"""
Feature:

id_prenotazione
id_paziente
data_nascita
sesso
regione_residenza
asl_residenza
provincia_residenza
comune_residenza
descrizione_attivita
data_contatto
regione_erogazione
asl_erogazione   provincia_erogazione
struttura_erogazione
codice_struttura_erogazione
tipologia_struttura_erogazione
id_professionista_sanitario
tipologia_professionista_sanitario
data_erogazione
ora_inizio_erogazione
ora_fine_erogazione
eta_paziente
durata_televisita
year
month
incremento


Feature utili per il Clustering:

Regione Residenza
ASL Residenza
Provincia Residenza
Comune Residenza
\
Regione Erogazione
ASL Erogazione
Provincia Erogazione
Comune Erogazione
Struttura Erogazione / Codice Struttura Erogazione
Tipologia Struttura Erogazione ?
\
Professionista Sanitario
\
Sesso Paziente
Età paziente
Durata Televisita
Anno Televisita
Mese Televisita


Variabile target: Incremento

"""
