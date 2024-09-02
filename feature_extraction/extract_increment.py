import pandas as pd


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
            risultati.append({
                'tipologia': tipologia,
                'mesi': f"{mesi[0]},{mesi[-1]}",
                'anno': f"{anno1},{anno2}",
                'percentuale': incremento
            })
    # Creare un DataFrame con i risultati
    df_risultati = pd.DataFrame(risultati)

    # Salvare il DataFrame in un file Parquet
    df_risultati.to_parquet(output_file, index=False)

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