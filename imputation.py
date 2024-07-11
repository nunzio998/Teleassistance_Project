import pandas as pd
from fancyimpute import IterativeImputer
from sklearn.preprocessing import LabelEncoder

# Caricamento del dataset
file_path = 'challenge_campus_biomedico_2024.parquet'
df = pd.read_parquet(file_path)

# Identificazione delle variabili numeriche e categoriche
numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

# Label Encoding delle variabili categoriche
label_encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    label_encoders[col] = le

# Imputazione dei valori mancanti
imputer = IterativeImputer()
df_imputed = imputer.fit_transform(df)
df_imputed = pd.DataFrame(df_imputed, columns=df.columns)

# Decodifica delle variabili categoriche
for col in categorical_cols:
    le = label_encoders[col]
    df_imputed[col] = le.inverse_transform(df_imputed[col].astype(int))

# Salvataggio del dataset pulito
output_file_path = 'imputed_challenge_campus_biomedico_2024.parquet'
df_imputed.to_csv('imputed_challenge_campus_biomedico_2024.csv', index=False)
df_imputed.to_parquet(output_file_path)
print(f"Dataset pulito salvato in: {output_file_path}")
