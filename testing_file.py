import pandas as pd 

df = pd.read_csv("Data/december_internal_india_data/bot_in_daily_2026-10-06.csv", dtype=str, low_memory=False)
# print(df.iloc[-1])


print(df.info())
