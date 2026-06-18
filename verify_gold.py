import pandas as pd
import glob

def load(table):
    files = glob.glob(f'data/gold/{table}/*.parquet', recursive=True)
    return pd.concat([pd.read_parquet(f) for f in files])

print("=== gold_fatigue_recovery (sample) ===")
df = load('gold_fatigue_recovery')
print(df[['user_id','date','recovery_score','stress_score','total_sleep_hours',
          'sleep_debt_hours','training_load_today','fatigue_score',
          'recovery_readiness','overtraining_flag']].sort_values('fatigue_score', ascending=False).head(10).to_string())
print()
print("fatigue_score range:", df['fatigue_score'].min(), "-", df['fatigue_score'].max())
print("overtraining_flag True count:", df['overtraining_flag'].sum(), "/", len(df))
print()

print("=== gold_workout_consistency (sample) ===")
df2 = load('gold_workout_consistency')
print(df2[['user_id','total_sessions','completed_sessions','completion_rate_pct',
           'avg_rpe','total_training_load']].head(5).to_string())
print()
print("muscle_group_breakdown sample (first user):")
print(df2.iloc[0]['muscle_group_breakdown'])
print()

print("=== gold_community_analytics (full, only 12 rows) ===")
df3 = load('gold_community_analytics')
print(df3.to_string())
print()

print("=== gold_user_profile_enriched (full, only 7 rows) ===")
df4 = load('gold_user_profile_enriched')
print(df4.to_string())