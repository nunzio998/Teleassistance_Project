import os
import matplotlib.pyplot as plt
import seaborn as sns
from feature_extraction.features_extraction import conta_professionisti_per_mese, conta_professionisti_per_sesso

def crea_grafico_sesso_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    plt.figure(figsize=(14, 8))
    sns.barplot(x='sesso', y='conteggio', hue='anno', data=df_professionista, palette="coolwarm", dodge=True)
    plt.title(f'Richieste per {tipologia_professionista} in base al Sesso', fontsize=20, weight='bold')
    plt.xlabel('Sesso', fontsize=14)
    plt.ylabel('Numero di Richieste', fontsize=14)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(tipologia_dir, f'grafico_sesso_{tipologia_professionista}.png'))
    plt.close()

def crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
       Crea e salva il grafico a barre mensile per una specifica tipologia di professionista sanitario.

       Args:
       df_professionista (pd.DataFrame): Il DataFrame filtrato per la tipologia specificata.
       tipologia_professionista (str): La tipologia di professionista sanitario con caratteri non validi sostituiti.
       tipologia_dir (str): La directory dove salvare i grafici.
       """
    plt.figure(figsize=(14, 8))
    sns.barplot(x='mese', y='conteggio', hue='anno', data=df_professionista, palette="coolwarm", dodge=True)

    # Configurazione delle etichette
    plt.title(f'Richieste Mensili per {tipologia_professionista}', fontsize=20, weight='bold')
    plt.xlabel('Mese', fontsize=14)
    plt.ylabel('Numero di Richieste', fontsize=14)
    plt.xticks(range(12), ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'],
               fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(True, linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(tipologia_dir, f'grafico_mensile_{tipologia_professionista}.png'))
    plt.close()

def crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir):
    """
    Crea e salva gli istogrammi annuali per una specifica tipologia di professionista sanitario.

    Args:
    df_professionista (pd.DataFrame): Il DataFrame filtrato per la tipologia specificata.
    tipologia_professionista (str): La tipologia di professionista sanitario con caratteri non validi sostituiti.
    tipologia_dir (str): La directory dove salvare i grafici.
    """
    # Estrai gli anni disponibili nel DataFrame
    anni = df_professionista['anno'].unique()

    # Creazione di un plot per ciascun anno
    for anno in anni:
        df_anno = df_professionista[df_professionista['anno'] == anno]

        plt.figure(figsize=(14, 8))
        sns.histplot(data=df_anno, x='mese', weights='conteggio', bins=12, kde=False, color='skyblue', discrete=True)

        # Configurazione delle etichette
        plt.title(f'Frequenza delle Richieste Mensili per {tipologia_professionista} - Anno {anno}', fontsize=20,
                  weight='bold')
        plt.xlabel('Mese', fontsize=14)
        plt.ylabel('Frequenza', fontsize=14)
        plt.xticks(range(1, 13), ['Gen', 'Feb', 'Mar', 'Apr', 'Mag', 'Giu', 'Lug', 'Ago', 'Set', 'Ott', 'Nov', 'Dic'],
                   fontsize=12)
        plt.yticks(fontsize=12)
        plt.grid(True, linestyle='--', linewidth=0.5)
        plt.tight_layout()
        plt.savefig(os.path.join(tipologia_dir, f'istogramma_{tipologia_professionista}anno{anno}.png'))
        plt.close()
def crea_grafici_e_salva(df_aggregato, output_dir='grafici_professionisti',tipo='mese'):
    """
    Crea e salva grafici a barre e istogrammi per ogni tipologia di professionista sanitario.

    Args:
    df_aggregato (pd.DataFrame): Il DataFrame aggregato contenente i dati mensili per ogni tipologia e anno.
    output_dir (str): La directory principale dove salvare i grafici.
    """
    # Creare la cartella principale se non esiste
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Ottenere tutte le tipologie di professionisti sanitari
    tipologie = df_aggregato['tipologia_professionista_sanitario'].unique()

    for tipologia in tipologie:
        # Sostituire i caratteri non validi nei nomi delle cartelle
        tipologia_professionista = tipologia.replace('/', '_')

        # Creare una sottocartella per ogni tipologia
        tipologia_dir = os.path.join(output_dir, tipologia_professionista)
        if not os.path.exists(tipologia_dir):
            os.makedirs(tipologia_dir)
        df_professionista = df_aggregato[df_aggregato['tipologia_professionista_sanitario'] == tipologia]

        if tipo == 'mese':
            df_professionista = df_professionista.sort_values(by=['mese', 'anno'])
            crea_grafico_mensile_per_anni(df_professionista, tipologia_professionista, tipologia_dir)
            crea_istogrammi_mensili_per_anni(df_professionista, tipologia_professionista, tipologia_dir)
        elif tipo == 'sesso':
            crea_grafico_sesso_per_anni(df_professionista, tipologia_professionista, tipologia_dir)


def diagrams():
    df_aggregato = conta_professionisti_per_mese('month_dataset')
    crea_grafici_e_salva(df_aggregato, output_dir='grafici_professionisti_mese', tipo='mese')
    df_aggregato_sesso = conta_professionisti_per_sesso('month_dataset')
    crea_grafici_e_salva(df_aggregato_sesso, output_dir='grafici_professionisti_per_sesso', tipo='sesso')
    return None
