import pandas as pd


def remove_columns_with_unique_correlation(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Rimuove le colonne con correlazione univoca
    :param df:
    :return:
    '''

    # Lista di tuple contenenti le coppie di colonne da confrontare
    coppie_colonne = [
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

    # Verifica della correlazione univoca per ogni coppia di colonne e rimozione se necessario
    for codice, descrizione in coppie_colonne:
        gruppi_codice = df.groupby(codice)[descrizione].nunique()
        gruppi_descrizione = df.groupby(descrizione)[codice].nunique()

        correlazione_univoca_codice_descrizione = all(gruppi_codice <= 1)
        if correlazione_univoca_codice_descrizione:
            print(f"Ogni {codice} è associato al massimo a un'unica {descrizione}.")
        else:
            print(f"Esiste almeno un {codice} associato a più di una {descrizione}.")
        correlazione_univoca_descrizione_codice = all(gruppi_descrizione <= 1)
        if correlazione_univoca_descrizione_codice:
            print(f"Ogni {descrizione} è associato al massimo a un'unica {codice}.")
        else:
            print(f"Esiste almeno un {descrizione} associato a più di una {codice}.")

        if correlazione_univoca_codice_descrizione and correlazione_univoca_descrizione_codice:
            df.drop(columns=[codice], inplace=True)
            print(f"Rimossa colonna {codice} per correlazione univoca con {descrizione}.\n")
        else:
            print(f"Impossibile rimuovere colonna {codice} per correlazione NON univoca con {descrizione}.\n")

    return df


def feature_selection_execution(df) -> pd.DataFrame:
    '''
    Esegue la feature selection
    :param df:
    :return:
    '''
    df = remove_columns_with_unique_correlation(df)
    return df
