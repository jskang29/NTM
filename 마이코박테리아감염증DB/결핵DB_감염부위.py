import pandas as pd

condition_occurrence = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\condition_occurrence.csv")


# 
condition_A31 = condition_occurrence[condition_occurrence["condition_source_value"].str.startswith("A31", na=False)]
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True, True])
condition_A31 = condition_A31.drop_duplicates(subset="person_id", ignore_index=True)
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]

A31_infection_site = {
    "A310": "폐",
    "A311": "피부",
    "A318": "기타",
    "A319": "상세불명"
}

condition_A31["감염부위"] = "알수없음"

for key, value in A31_infection_site.items():
    condition_A31.loc[condition_A31["condition_source_value"].str.startswith(key), "감염부위"] = value

condition_A31_site = condition_A31[["person_id", "NTM진단일", "감염부위"]]

