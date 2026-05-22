# Reference Data Schema Documentation

## user_profiles.csv
| Column | Type | Description |
|---|---|---|
| user_id | string | Primary key, format U1001–U1050 |
| name | string | Faker-generated name |
| age | int | 18–65 years |
| gender | string | male/female/other |
| weight_kg | float | 48–112 kg |
| height_cm | int | 152–198 cm |
| bmi | float | Computed: weight/(height_m^2) |
| bmi_category | string | underweight/normal/overweight/obese |
| medical_history | string | none/hypertension/diabetes |
| job_type | string | sedentary/active/mixed |
| fitness_goal | string | strength/endurance/weight_loss/longevity |
| gym_id | string | FK to gym_master |
| member_since | date | Membership start date |
| is_active | bool | Currently active member |
| city | string | Chennai/Bengaluru/Mumbai/Hyderabad/Coimbatore |

## gym_master.csv
| Column | Type | Description |
|---|---|---|
| gym_id | string | Primary key GYM1–GYM5 |
| name | string | Gym name |
| city | string | City |
| area | string | Area within city |
| gym_type | string | crossfit/commercial/yoga_studio/functional/performance |
| capacity | int | Max members |
| monthly_fee | int | INR per month |
| established | int | Year established |
| has_pool | bool | Swimming pool available |
| has_sauna | bool | Sauna available |

## workout_catalog.csv
| Column | Type | Description |
|---|---|---|
| workout_type | string | Primary key |
| category | string | resistance/cardio/flexibility/mixed/recovery |
| met_value | float | Metabolic Equivalent of Task (exercise intensity) |
| primary_benefit | string | Main physiological benefit |
| recovery_days_needed | int | Recommended rest days after session |

## medical_conditions.csv
| Column | Type | Description |
|---|---|---|
| condition | string | Primary key |
| risk_modifier | float | Multiplier applied to fatigue scoring |
| hr_max_factor | float | Max heart rate reduction factor |
| notes | string | Clinical guidance |

## date_dimension.csv
| Column | Type | Description |
|---|---|---|
| date_id | string | YYYYMMDD format, surrogate key |
| full_date | date | YYYY-MM-DD |
| year/quarter/month | int | Standard date parts |
| month_name/day_name | string | Human-readable labels |
| week_of_year | int | ISO week number |
| is_weekend | bool | Saturday or Sunday |
| day_of_month/year | int | Positional values |