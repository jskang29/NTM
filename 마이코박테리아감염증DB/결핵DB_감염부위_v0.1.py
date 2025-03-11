import pandas as pd

condition_occurrence = condition_occurrence = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\condition_occurrence(regtime추가).csv")

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

condition_A31_site = condition_A31[["person_id", "NTM진단일", "감염부위", "condition_source_value", "진단명", "진료과"]]

condition_A31_site.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\감염부위_v0.1.csv", index=False)

