import pandas as pd
from data_prep.imputation import imputate_missing_values
from data_prep.feature_selection import feature_selection_execution


# Caricamento del dataset
file_path = 'datasets/challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

#df.to_csv('datasets/challenge_campus_biomedico_2024.csv', index=False)

# Imputazione dei valori mancanti
df = imputate_missing_values(df)

#df.to_csv('datasets/challenge_campus_biomedico_2024_imputed.csv', index=False)

# Features Selection
df = feature_selection_execution(df)


'''
Statistiche valori mancanti dopo l'imputazione:

codice_provincia_residenza      28380
comune_residenza                  135 --> da non toccare perché relativi al comune di None in provincia di Torino
codice_provincia_erogazione     28776
ora_inizio_erogazione           23652
ora_fine_erogazione             23652
data_disdetta                  460639
'''

# TODO: rimuovere colonne relative a 'codice_provincia_residenza' e 'codice_provincia_erogazione' in quanto non utili

# TODO: rimuovere tutti i campioni che hanno una data_disdetta non nulla poiché non significativi dato che la
#  televisita non è avvenuta ma è stata annullata. In questo modo non ci saranno valori mancanti per 'ora_inizio_erogazione' e 'ora_fine_erogazione'

# TODO: rimuovere colonne relative a 'data_disdetta' in quanto non utili. Dopo il secondo TODO per quella feature ci saranno solo valori mancanti

"""
DOPO TODO:
Statistiche valori mancanti dopo l'imputazione:

codice_provincia_residenza      27016
comune_residenza                  130
codice_provincia_erogazione     27396
data_disdetta                  460639
dtype: int64
-----------------------------------
"""