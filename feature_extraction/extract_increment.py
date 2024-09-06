import pandas as pd
import io

def conta_televisite_per_mesi(df, anni, mesi, tipologia):
    """
    Conta il numero totale di televisite per una tipologia di professionista sanitario specificata,
    in un intervallo di mesi e anni.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    anni (list): Lista degli anni di interesse.
    mesi (list): Lista dei mesi di interesse.
    tipologia (str): La tipologia di professionista sanitario di cui calcolare le televisite.

    Returns:
    dict: Un dizionario con gli anni come chiavi e il totale delle televisite per quell'anno come valori.
    """
    # Filtrare il DataFrame per includere solo gli anni e i mesi specificati
    df_filtered = df[(df['anno'].isin(anni)) & (df['mese'].isin(mesi))]

    # Filtrare ulteriormente per la tipologia di professionista sanitario specificata
    df_professionista = df_filtered[df_filtered['tipologia_professionista_sanitario'].str.lower() == tipologia.lower()]

    # Calcolare la somma delle televisite per ciascun anno
    return {anno: df_professionista[df_professionista['anno'] == anno]['conteggio'].sum() for anno in anni}

def calcola_incremento_percentuale(df, anno1, anno2, mesi, tipologia):
    """
    Calcola l'incremento percentuale delle televisite tra due anni per una tipologia di professionista sanitario
    specificata in un intervallo di mesi.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    anno1 (int): Il primo anno per il confronto.
    anno2 (int): Il secondo anno per il confronto.
    mesi (list): Lista dei mesi di interesse.
    tipologia (str): La tipologia di professionista sanitario di cui calcolare l'incremento percentuale.

    Returns:
    float: L'incremento percentuale tra i due anni per la tipologia specificata.
    """
    # Ottenere i conteggi per gli anni specificati
    conteggi = conta_televisite_per_mesi(df, [anno1, anno2], mesi, tipologia)

    # Calcolare il conteggio per i due anni
    conteggio_anno1 = conteggi.get(anno1, 0)
    conteggio_anno2 = conteggi.get(anno2, 0)

    # Calcolare l'incremento percentuale
    incremento = ((conteggio_anno2 - conteggio_anno1) / conteggio_anno1) * 100 if conteggio_anno1 > 0 else float('inf')
    return incremento


def classifica_incremento(percentuale):
    """
    Classifica l'incremento in base alla percentuale.

    Args: percentuale (float): La percentuale di incremento da classificare.

    Returns:
    str: La classificazione dell'incremento ('alta', 'media', 'costante', 'bassa').
    """
    if percentuale > 100:
        return 'alta'
    elif 50 < percentuale <= 100:
        return 'media'
    elif 0 <= percentuale <= 50:
        return 'costante'
    elif percentuale < 0:
        return 'bassa'
    else:
        return 'media'  # Per valori tra 1% e 50%, assumiamo 'media'.


def salva_incremento_percentuale_per_intervalli(df, tipologie, intervalli_anni_mesi, output_file):
    """
    Calcola e salva l'incremento percentuale delle televisite per vari intervalli di mesi e anni,
    per ogni tipologia di professionista sanitario, in un file Parquet.

    Args:
    df (pd.DataFrame): Il DataFrame contenente i dati mensili per ogni tipologia e anno.
    tipologie (list): Lista delle tipologie di professionisti sanitari di interesse.
    intervalli_anni_mesi (list): Lista degli intervalli di anni e mesi per cui calcolare l'incremento percentuale.
    output_file (str): Il percorso del file Parquet in cui salvare i risultati.
    """
    risultati = []
    for tipologia in tipologie:
        for (anno1, anno2, mesi) in intervalli_anni_mesi:
            # Calcolare l'incremento percentuale
            incremento = calcola_incremento_percentuale(df, anno1, anno2, mesi, tipologia)
            classificazione = classifica_incremento(incremento)
            risultati.append({
                'tipologia': tipologia,
                'mesi': f"{mesi[0]},{mesi[-1]}",
                'anno': f"{anno1},{anno2}",
                'percentuale': incremento,
                'incremento': classificazione
            })

    # Creare un DataFrame con i risultati
    df_risultati = pd.DataFrame(risultati)

    # Trasforma il DataFrame in un buffer Parquet senza salvarlo su disco
    incremento_percentuale_compatto = io.BytesIO()
    df_risultati.to_parquet(incremento_percentuale_compatto, index=False)

    # Sposta il puntatore all'inizio del buffer
    incremento_percentuale_compatto.seek(0)

    # Leggi il buffer Parquet in un nuovo DataFrame
    df_incremento = pd.read_parquet(incremento_percentuale_compatto)

    # Espandi i valori di 'mesi' per coprire tutti i mesi nel range e prendi solo il secondo anno
    expanded_rows = []
    for _, row in df_incremento.iterrows():
        mesi_range = row['mesi'].split(',')
        for mese in range(int(mesi_range[0]), int(mesi_range[1]) + 1):
            anno = int(row['anno'].split(',')[1])  # Prendi solo il secondo anno
            expanded_row = row.copy()
            expanded_row['month'] = mese
            expanded_row['anno'] = anno
            expanded_rows.append(expanded_row)

    df_incremento_expanded = pd.DataFrame(expanded_rows)

    # Rinomina la colonna 'anno' in 'year'
    df_incremento_expanded.rename(columns={'anno': 'year'}, inplace=True)

    # Mantieni solo le colonne necessarie
    df_incremento_expanded = df_incremento_expanded[['tipologia', 'month', 'year', 'percentuale', 'incremento']]

    # Trasforma il DataFrame in un buffer Parquet senza salvarlo su disco
    incremento_percentuale_espanso = io.BytesIO()
    df_incremento_expanded.to_parquet(incremento_percentuale_espanso, index=False)

    # Sposta il puntatore all'inizio del buffer
    incremento_percentuale_espanso.seek(0)

    # Leggi il buffer Parquet in un nuovo DataFrame
    df_incremento_percentuale_espanso = pd.read_parquet(incremento_percentuale_espanso)

    # Elimina la colonna 'index' se esiste
    if 'index' in df_incremento_percentuale_espanso.columns:
        df_incremento_percentuale_espanso.drop(columns=['index'], inplace=True)

    # Salva il DataFrame espanso in un file Parquet
    df_incremento_percentuale_espanso.to_parquet(output_file, index=False)


def incremento():
    # Carica il file df_aggregato.parquet
    file_path = 'datasets/df_aggregato.parquet'
    df_aggregato = pd.read_parquet(file_path)

    # Ottieni tutte le tipologie di professionisti sanitari presenti nel dataset
    tipologie = df_aggregato['tipologia_professionista_sanitario'].unique()

    # Definisci gli intervalli di anni e mesi di interesse
    intervalli_anni_mesi = [
        (2019, 2020, [1, 2, 3, 4]),
        (2019, 2020, [5, 6, 7, 8]),
        (2019, 2020, [9, 10, 11, 12]),
        (2020, 2021, [1, 2, 3, 4]),
        (2020, 2021, [5, 6, 7, 8]),
        (2020, 2021, [9, 10, 11, 12]),
        (2021, 2022, [1, 2, 3, 4]),
        (2021, 2022, [5, 6, 7, 8]),
        (2021, 2022, [9, 10, 11, 12])
    ]

    # Salva l'incremento percentuale per i vari intervalli di mesi e anni per ogni tipologia di professionista sanitario in un file Parquet
    output_file = 'datasets/incremento_percentuale.parquet'
    salva_incremento_percentuale_per_intervalli(df_aggregato, tipologie, intervalli_anni_mesi, output_file)
    file_incremento = pd.read_parquet(output_file)
    print(file_incremento.head(30))
    return output_file

"""
    tipologia  mesi       anno  percentuale
0  Infermiere   1,4  2019,2020   115.818092
1  Infermiere   5,8  2019,2020    38.422351
2  Infermiere  9,12  2019,2020    79.069648
3  Infermiere   1,4  2020,2021    51.094824
4  Infermiere   5,8  2020,2021     7.337388
5  Infermiere  9,12  2020,2021   -30.465416
6  Infermiere   1,4  2021,2022   -31.421295
7  Infermiere   5,8  2021,2022   -16.122280
8  Infermiere  9,12  2021,2022    -8.365598
9   Psicologo   1,4  2019,2020   140.305149

"""

