import pandas as pd
import logging
import numpy as np

# Configuro il logger
logging.basicConfig(level=logging.INFO,  # Imposto il livello minimo di log
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Formato del log


def unique_correlation_analisys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Funzione che controlla se c'è correlazione univoca tra due features e in caso affermativo ne rimuove una.
    :param df:
    :return:
    """
    features_pairs = [
        ('codice_provincia_residenza', 'provincia_residenza'),
        ('codice_provincia_erogazione', 'provincia_erogazione'),
        ('codice_regione_residenza', 'regione_residenza'),
        ('codice_asl_residenza', 'asl_residenza'),
        ('codice_comune_residenza', 'comune_residenza'),
        ('codice_descrizione_attivita', 'descrizione_attivita'),
        ('codice_regione_erogazione', 'regione_erogazione'),
        ('codice_asl_erogazione', 'asl_erogazione'),
        ('codice_struttura_erogazione', 'struttura_erogazione'),
        ('codice_tipologia_struttura_erogazione', 'tipologia_struttura_erogazione'),
        ('codice_tipologia_professionista_sanitario', 'tipologia_professionista_sanitario')
    ]

    for pair in features_pairs:
        codice_univoco = df.groupby(pair[0])[pair[1]].nunique() == 1
        descrizione_univoca = df.groupby(pair[1])[pair[0]].nunique() == 1

        # Se entrambi i controlli sono veri per tutte le righe, puoi eliminare la colonna codice
        if codice_univoco.all() and descrizione_univoca.all():
            df = df.drop(columns=[pair[0]])
            logging.info(f"Feature {pair[0]} eliminata correlazione univoca con la feature {pair[1]}")
        else:
            logging.info(f"Alcuni codici o descrizioni non sono univoci per le features {pair[0]} e {pair[1]}.")

    return df


def remove_data_disdetta(df) -> pd.DataFrame:
    """
    Rimuove i campioni con 'data_disdetta' non nullo.
    :param df:
    :return: df senza campioni con 'data_disdetta' non nullo.
    """
    logging.info("Eliminazione della feature: data_disdetta")
    df.drop(columns=['data_disdetta'], inplace=True)
    return df


def remove_regione_erogazione(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove la colonna 'regione_erogazione' dal DataFrame.
    :param df:
    :return: df senza la colonna 'regione_erogazione'.
    """
    logging.info("Eliminazione della feature: regione_erogazione")
    df.drop(columns=['regione_erogazione'], inplace=True)
    return df


def remove_id_prenotazione(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove la feature id_prenotazione
    :param df:
    :return: df senza la colonna specificate.
    """
    logging.info("Eliminazione della feature: id_prenotazione")
    df.drop(columns=['id_prenotazione'], inplace=True)
    return df


def remove_tipologia_servizio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove la colonna 'tipologia_servizio' dal DataFrame.
    :param df:
    :return: df senza la colonna 'tipologia_servizio'.
    """
    logging.info("Eliminazione della feature: tipologia_servizio")
    df.drop(columns=['tipologia_servizio'], inplace=True)
    return df


def check_regione_residenza_equals_regione_erogazione(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se 'regione_residenza' è uguale a 'regione_erogazione'per ogni campione. Se è falso, automaticamente
    anche i dati relativi a asl, comune e provincia non saranno uguali per tutti i campioni. Quindi non si potranno
    eliminare le relative feature poiché è presente contenuto informativo.
    :param df:
    :return: bool
    """
    return (df['regione_residenza'] == df['regione_erogazione']).all()


def check_tipologia_servizio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica se 'tipologia_servizio' ha sempre lo stesso valore 'Teleassistenza'.
    :param df:
    :return:
    """

    return (df['tipologia_servizio'] == 'Teleassistenza').all()


def feature_selection(df: pd.DataFrame) -> pd.DataFrame:
    """
    Esegue la feature selection
    :param df:
    :return:
    """
    df = unique_correlation_analisys(df)
    df = remove_data_disdetta(df)
    df = remove_id_prenotazione(df)

    # Se le due features hanno sempre valori uguali, rimuovo 'regione_erogazione'
    if check_regione_residenza_equals_regione_erogazione(df):
        df = remove_regione_erogazione(df)

    # Se 'tipologia_servizio' ha sempre lo stesso valore, rimuovo la colonna
    if check_tipologia_servizio(df):
        df = remove_tipologia_servizio(df)

    return df
