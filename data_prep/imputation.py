import pandas as pd
from fancyimpute import IterativeImputer
from sklearn.preprocessing import LabelEncoder

def imputate_missing_values(df):
    '''
    Imputa i valori mancanti del dataset df.
    :param df:
    :return:
    '''
    # Carico il dataset relativo ai codici ISTAT dei comuni italiani in modo da poter fare imputation
    df_istat = pd.read_excel('datasets/Codici-statistici-e-denominazioni-al-30_06_2024.xlsx')

    codice_comune_to_nome = pd.Series(df_istat['Denominazione in italiano'].values,
                                      index=df_istat['Codice Comune formato alfanumerico'])

    def fill_missing_comune_residenza(row):
        if row['comune_residenza'] is None:
            return codice_comune_to_nome.get(row['codice_comune_residenza'])
        return row['comune_residenza']

    df['comune_residenza'] = df.apply(fill_missing_comune_residenza, axis=1)

    '''
    # N.B. Dopo l'imputazione i valori mancanti relativi a comune_residenza continuano a risultare mancanti 
    in quanto relativi al comune di None in provincia di Torino con codice ISTAT 1168.
    '''

    colonne_con_mancanti = df.columns[df.isnull().any()]

    print(df[colonne_con_mancanti].isnull().sum())

    print('-----------------------------------')

    return df


