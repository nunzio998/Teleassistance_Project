from sklearn.preprocessing import LabelEncoder
import numpy as np
import pandas as pd


def data_transformation(df):
    """
    Esegue la trasformazione dei dati (normalizzazione e aggregazione) sul DataFrame.
    :param df: DataFrame da trasformare
    :return: DataFrame trasformato
    """

    # Elimina le feature poco significative
    df = remove_features(df)

    # Definisce quali sono le feature numeriche e quelle categoriche
    categorical_features, numerical_features = define_features_types()

    # Applica la trasformazione (encoding) delle feature categoriche
    df, label_encoders, reverse_mapping = transform_and_preprocess_data(df, categorical_features)

    return df, label_encoders, reverse_mapping, numerical_features, categorical_features


def remove_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove le feature poco significative dal DataFrame.
    :param df:
    :return: df senza le colonne specificate.
    """
    features_to_drop = [
        'id_prenotazione', 'id_paziente', 'asl_residenza', 'comune_residenza', 'descrizione_attivita',
        'asl_erogazione', 'codice_struttura_erogazione', 'provincia_residenza', 'provincia_erogazione',
        'struttura_erogazione', 'id_professionista_sanitario'
    ]
    return df.drop(columns=[col for col in features_to_drop if col in df.columns])


def define_features_types() -> (list, list):
    """
    Definisce le feature numeriche e categoriche da utilizzare nel clustering.
    :return categorical_features, numerical_features
    """
    categorical_features = ['sesso', 'regione_residenza', 'regione_erogazione',
                            'tipologia_professionista_sanitario', 'incremento', 'tipologia_struttura_erogazione']
    numerical_features = ['eta_paziente', 'month', 'year', 'durata_televisita']
    return categorical_features, numerical_features


def transform_and_preprocess_data(df: pd.DataFrame, categorical_features: list):
    """
    Effettua l'encoding delle feature categoriche con metodo LabelEncoder, e crea un dizionario
    che mappa l'encoding delle feature ad ogni feature.
    :param df: dataFrame
    :param categorical_features: colonne delle feature categoriche
    :return: df, label_encoders, reverse_mapping
    """
    label_encoders = {}
    reverse_mapping = {}

    # Gestione delle colonne temporali (converte in timestamp con utc=True)
    for col in ['data_contatto', 'data_erogazione']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            df[col] = df[col].astype('int64') // 10 ** 9  # Convert to UNIX timestamp safely

    # Applica LabelEncoder a ciascuna colonna categorica
    for col in categorical_features:
        if col in df.columns and (df[col].dtype == 'object' or isinstance(df[col].dtype, pd.StringDtype)):
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col])
            label_encoders[col] = le
            # Aggiungi la mappatura inversa per ogni feature categorica
            reverse_mapping[col] = {i: label for i, label in enumerate(le.classes_)}

    # Verifica che tutte le colonne siano numeriche dopo l'encoding
    if not all(np.issubdtype(df[col].dtype, np.number) for col in df.columns):
        raise ValueError("Ci sono ancora colonne non numeriche nel DataFrame dopo l'encoding.")

    return df, label_encoders, reverse_mapping


