import pandas as pd

df = pd.read_csv("data/abonnes.csv")

# répéter ~70 fois
big_df = pd.concat([df] * 70, ignore_index=True)

# rendre les IDs uniques
big_df["id_abonne"] = range(1, len(big_df) + 1)

big_df.to_csv(
    "data/abonnes_cluster_demo.csv",
    index=False
)

print(len(big_df))