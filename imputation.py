import pandas as pd
from fancyimpute import IterativeImputer
from sklearn.preprocessing import LabelEncoder

# Caricamento del dataset
file_path = 'challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

# Carico il dataset relativo ai codici ISTAT dei comuni italiani in modo da poter fare imputation
df_istat = pd.read_excel('Codici-statistici-e-denominazioni-al-30_06_2024.xlsx')

codice_comune_to_nome = pd.Series(df_istat['Denominazione in italiano'].values, index=df_istat['Codice Comune formato alfanumerico'])

print(codice_comune_to_nome)


# Funzione per riempire i valori mancanti di comune_residenza
def fill_missing_comune_residenza(row):
    if pd.isna(row['comune_residenza']):
        return codice_comune_to_nome.get(row['codice_comune_residenza'], row['comune_residenza'])
    return row['comune_residenza']

# Applicazione della funzione al dataframe
df['comune_residenza'] = df.apply(fill_missing_comune_residenza, axis=1)


colonne_con_mancanti = df.columns[df.isnull().any()]

print(df[colonne_con_mancanti].isnull().sum())

print('-----------------------------------')


#df.to_csv('imputed_challenge_campus_biomedico_2024.csv', index=False)