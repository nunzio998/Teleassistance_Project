import pandas as pd


def incremento(df):
    """
    Funzione principale che calcola la variabile di incremento, estende i risultati per ciascun mese
    e li unisce al DataFrame originale. Alla fine elimina i dati relativi all'anno 2019,
    poiché l'incremento si applica solo a partire dal 2020.
    :param df: DataFrame contenente i dati originali.
    :return df_finale: DataFrame finale con la variabile 'incremento' calcolata e unita ai dati originali.
    """
    # Definisco i dati da utilizzare
    tipologie, DaF, intervalli_anni_mese = dati_da_utilizzare(df)

    # Calcola le occorrenze totali di ogni professionista per l'intervallo di mesi specificato
    risultato = somma_per_intervallo_mesi(DaF, tipologie)

    # Calcola l'incremento fra anni successivi e associa ad ogni anno una label  "alta", "media", "bassa" o "costante"
    risultato_con_incremento = calcola_incremento(risultato, intervalli_anni_mese)

    # Associa la label "alta", "media", "bassa" o "costante" ad ogni mese
    risultato_esteso = estendi_incremento(risultato_con_incremento)

    # Salva il dataFrame esteso in un nuovo file
    risultato_esteso.to_parquet('datasets/df_incremento_percentuale_esteso.parquet', index=False)

    # Unisce la colonna incremento al DataFrame: associa ad ogni campione una label alta, media, bassa, costante
    df_finale = unisci_incremento(df, risultato_esteso)

    return df_finale


def dati_da_utilizzare(df):
    """
    Definisce i dati da utilizzare per calcolare l'incremento
    :param df: DataFrame contenente i dati iniziali
    :return tipologie: lista delle tipologie di professionista sanitario
    :return dF_occorrenze: DataFrame che contiene il numero di occorrenze di ogni professionista sanitario per mese
    :return intervalli_anni_mesi: lista di tuple che definiscono intervalli di anni e mesi in formato semestrale
        """
    tipologie = df['tipologia_professionista_sanitario'].unique()  # Estrae le tipologie uniche di professionista

    dF_occorrenze = pd.read_parquet('datasets/df_aggregato.parquet')  # Lettura del file contente le occorrenze

    # Lista degli intervalli di anni e mesi in formato semestrale
    intervalli_anni_mesi = [
        (2019, 2020, '1, 2, 3, 4, 5, 6'),
        (2019, 2020, '7, 8, 9, 10, 11, 12'),
        (2020, 2021, '1, 2, 3, 4, 5, 6'),
        (2020, 2021, '7, 8, 9, 10, 11, 12'),
        (2021, 2022, '1, 2, 3, 4, 5, 6'),
        (2021, 2022, '7, 8, 9, 10, 11, 12')
    ]
    return tipologie, dF_occorrenze, intervalli_anni_mesi


def get_intervallo_mesi(mese):
    """
    Restituisce l'intervallo di mesi a cui appartiene il mese specificato.
    :param mese: Mese per il quale si desidera ottenere l'intervallo
    :return intervallo: Intervallo di mesi in cui il mese rientra
       """
    intervalli_mesi = {
        (1, 2, 3, 4, 5, 6): '1, 2, 3, 4, 5, 6',
        (7, 8, 9, 10, 11, 12): '7, 8, 9, 10, 11, 12'
    }
    for mesi, intervallo in intervalli_mesi.items():
        if mese in mesi:
            return intervallo
    return "Intervallo non trovato"


def somma_per_intervallo_mesi(df, tipologie):
    """
       Calcola la somma delle occorrenze di ogni professionista sanitario per l'intervallo di
       mesi specificato (semestre). I risultati vengono raggruppati in un dataframe che ha come colonne
       'tipologia_professionista_sanitario', 'anno', 'intervallo_mesi' e 'conteggio'.
       :param df: DataFrame contenente i dati
       :param tipologie: Lista di tipologie di professionisti sanitari
       :return risultato: DataFrame con la somma delle occorrenze per tipologia, anno e intervallo di mesi
       """
    # Aggiungo la colonna 'intervallo_mesi'
    df['intervallo_mesi'] = df['mese'].apply(get_intervallo_mesi)

    # Verifico che le tipolgie di professionista sanitario siano quelle estratte in precedenza
    df_filtrato = df[df['tipologia_professionista_sanitario'].isin(tipologie)]

    # Raggruppa e somma i dati che hanno stesso tipologia di professionista sanitario, anno e intervallo di mesi
    risultato = df_filtrato.groupby(['tipologia_professionista_sanitario', 'anno',
                                     'intervallo_mesi'])['conteggio'].sum().reset_index()

    return risultato


def calcola_incremento(df, intervalli_anni_mesi):
    """
    Crea una tabella che ha come colonne 'tipologia_professionista_sanitario', 'intervallo_mesi', e 'anno',
    e calcola l'incremento percentuale fra due anni successivi.
    :param df: dataFrame contenente i dati raggruppati per anno e mese
    :param intervalli_anni_mesi: Intervalli di anni e mesi per cui calcolare l'incremento
    :return risultato_con_incremento: dataFrame contenente l'incremento percentuale per ciascun intervallo
    """

    # Crea una tabella pivot per avere anni come colonne
    df_pivot = df.pivot_table(index=['tipologia_professionista_sanitario', 'intervallo_mesi'], columns='anno',
                              values='conteggio').reset_index()

    def incremento_percentuale(row, anno1, anno2):
        """
        Calcola l'incremento percentuale del conteggio di professionisti sanitari tra due anni consecutivi.

        """
        conteggio_anno1 = row.get(anno1, 0)
        conteggio_anno2 = row.get(anno2, 0)

        # Calcolo dell'incremento percentuale
        return ((conteggio_anno2 - conteggio_anno1) / conteggio_anno1) * 100 if conteggio_anno1 > 0 else float('inf')

    risultati = []
    # Itera su ciascuna coppia di 'anno' e 'intervallo mesi' presente in 'intervalli_anni_mesi'.
    for (anno1, anno2, intervallo) in intervalli_anni_mesi:
        df_intervallo = df_pivot[df_pivot['intervallo_mesi'] == intervallo]

        # Itera su ciascuna riga del DataFrame filtrato.
        for _, row in df_intervallo.iterrows():
            incremento = incremento_percentuale(row, anno1,
                                                anno2)  # Calcola l'incremento percentuale tra 'anno1' e 'anno2'
            classificazione = classifica_incremento(
                incremento)  # Classifica l'incremento in una delle categorie: alto, medio, basso, costante

            # Aggiunge un dizionario con i risultati per la riga corrente nella lista 'risultati'.
            risultati.append({
                'tipologia_professionista_sanitario': row['tipologia_professionista_sanitario'],
                'anno': f"{anno1}-{anno2}",
                'intervallo_mesi': intervallo,
                'incremento_percentuale': incremento,
                'incremento': classificazione
            })
    risultato_con_incremento = pd.DataFrame(risultati)

    return risultato_con_incremento


def classifica_incremento(percentuale):
    """
    Classifica la percentuale dell'incremento in "alta", "media", "bassa" e "costante".
    :param percentuale: La percentuale di incremento da classificare.
    :return str: La classificazione dell'incremento ('alta', 'media', 'costante', 'bassa').
    """
    if percentuale < 0:
        return 'costante'
    elif 0 <= percentuale <= 35:
        return 'bassa'
    elif 35 < percentuale <= 80:
        return 'media'
    else:
        return 'alta'


def estendi_incremento(df):
    """
    Associa l'incremento associato a ciascun intervallo di mesi, assegnandolo a ogni mese individualmente.
    Per fare questo, considera il dataFrame che contiene l'incremento percentuale per intervalli di mesi
    e lo "estende": divide gli intervalli di mesi in mesi separati e associa l'incremento ad ognuno di essi.
    :param df: dataframe
    :return df_esteso: dataframe in cui ad ogni mese è associato un valore di incremento
    """
    # Crea un DataFrame vuoto per i dati estesi
    dati_estesi = []

    # Itera su ogni riga del DataFrame esistente
    for _, row in df.iterrows():
        # Considera due anni consecutivi e prende solo il secondo (es. 2019-2020, prende 2020)
        anno_intervallo = row['anno'].split('-')
        anno_fine = int(anno_intervallo[1])

        # Ottieni l'intervallo di mesi come lista (es. ['1', '2', '3', ...])
        mesi = row['intervallo_mesi'].split(', ')

        # Aggiungi una riga per ogni mese dell'intervallo
        for mese in mesi:
            dati_estesi.append({
                'tipologia_professionista_sanitario': row['tipologia_professionista_sanitario'],
                'year': anno_fine,
                'month': int(mese),
                'incremento_percentuale': row['incremento_percentuale'],
                'incremento': row['incremento']
            })

    # Converti la lista di dizionari in un DataFrame
    df_esteso = pd.DataFrame(dati_estesi)

    # Ordina i dati per tipologia, anno e mese
    df_esteso = df_esteso.sort_values(by=['tipologia_professionista_sanitario', 'year', 'month']).reset_index(drop=True)

    return df_esteso


def unisci_incremento(df_originale, risultato_esteso):
    """
    Unisce il DataFrame originale con il DataFrame esteso (risultato_esteso) basandosi sulle colonne
    'tipologia_professionista_sanitario', 'year', e 'month'.In questo modo, l'incremento calcolato per
     ogni combinazione di tipologia, anno e mese verrà aggiunto al DataFrame originale.
     Inoltre, associa a tutti i dati del 2019 la vari
    :param df_originale: dataFrame iniziale
    :paramrisultato_esteso: dataFrame con incremento associato ad ogni mese
    :retur df_unito: dataFrame in cui ogni campione ha associata una feature incremento
    """

    # Unisce il DataFrame originale con il risultato esteso, mantenendo tipologia, anno e mese
    df_unito = pd.merge(df_originale,
                        risultato_esteso[['tipologia_professionista_sanitario', 'year', 'month', 'incremento']],
                        on=['tipologia_professionista_sanitario', 'year', 'month'],
                        how='left')

    # Elimina i dati del 2019
    df_unito = df_unito[df_unito['year'] != 2019]

    return df_unito
