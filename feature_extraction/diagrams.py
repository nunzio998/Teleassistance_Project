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


def diagrams():
    return None
