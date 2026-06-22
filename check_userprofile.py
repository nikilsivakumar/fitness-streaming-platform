import pandas as pd

df = pd.read_parquet(r'C:\Users\ASUS\Downloads\user_profile_check.parquet')
print(df[['user_id', 'gym_id', 'medical_history', 'age', 'weight_kg',
          'medical_risk_modifier', 'gym_type']].sort_values('user_id').to_string())
print()
print('Unique users:', df['user_id'].nunique(), 'out of', len(df), 'rows')