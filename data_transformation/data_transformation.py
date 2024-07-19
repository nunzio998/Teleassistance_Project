from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler


def normalize_min_max(df, columns):
    """
    Normalizza le colonne specificate del DataFrame usando la normalizzazione min-max.
    :param df: DataFrame
    :param columns: Lista delle colonne da normalizzare
    :return: DataFrame con colonne normalizzate
    """
    scaler = MinMaxScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df


def standardize(df, columns):
    """
    Standardizza le colonne specificate del DataFrame usando lo Z-score.
    :param df: DataFrame
    :param columns: Lista delle colonne da standardizzare
    :return: DataFrame con colonne standardizzate
    """
    scaler = StandardScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df


def aggregate_data(df, group_by_columns, agg_columns, agg_funcs):
    """
    Esegue l'aggregazione dei dati.
    :param df: DataFrame
    :param group_by_columns: Lista delle colonne su cui raggruppare i dati
    :param agg_columns: Lista delle colonne da aggregare
    :param agg_funcs: Dizionario delle funzioni di aggregazione
    :return: DataFrame aggregato
    """
    agg_dict = {col: agg_funcs for col in agg_columns}
    df_agg = df.groupby(group_by_columns).agg(agg_dict).reset_index()
    return df_agg  # ritorno il dataset aggregato


def data_transformation(df):
    """
    Esegue la trasformazione dei dati (normalizzazione e aggregazione) sul DataFrame.
    :param df: DataFrame da trasformare
    :return: DataFrame trasformato
    """
    # Normalizzazione
    columns_to_normalize = ['colonna_da_normalizzare']
    df = normalize_min_max(df, columns_to_normalize)

    # Aggregazione
    group_by_columns = ['colonna1', 'collonna2']
    agg_columns = ['some_numeric_column1', 'some_numeric_column2']
    agg_funcs = ['mean', 'sum']
    df = aggregate_data(df, group_by_columns, agg_columns, agg_funcs)

    return df
