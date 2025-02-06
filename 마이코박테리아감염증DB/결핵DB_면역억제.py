import pandas as pd

drug_exposure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_샘플\\결핵CDM_v0.2(A31_2018)\\drug_exposure.csv")
immune = 
drug_atc = ["J01FA09", "J01FA10", "J04AB02", "J04AK02", "J04AC01", "J01GB06", "J01DC01", "J01DH51"] # 항결핵제

# 최소 2개 이상의 서로 다른 항결핵제를 사용한 최초 날짜를 구한다.(치료 시작일??, 2가지 이상이면 만족??)
Lab_drug = drug_exposure[["person_id", "drug_exposure_start_datetime", "ATC코드"]]
Lab_drug = Lab_drug.sort_values(by=["person_id", "drug_exposure_start_datetime"], ascending=[True, True])
Lab_drug = Lab_drug[Lab_drug["ATC코드"].isin(drug_atc)] # 항결핵제 처방받은 기록만 추출
Lab_drug = Lab_drug.drop_duplicates(subset= ["person_id", "drug_exposure_start_datetime", "ATC코드"])   # 600mg을 300mg 2개로 나눠 처방받은 경우 제거
Lab_drug = Lab_drug[Lab_drug.duplicated(subset= ["person_id", "drug_exposure_start_datetime"], keep=False)] 
Lab_drug = Lab_drug.drop_duplicates(subset= "person_id", ignore_index= True)

treatment_first_date = Lab_drug[["person_id", "drug_exposure_start_datetime"]],

# inhaled, systemic steroid 사용일자(ATC 코드 확인 필요)
drug_steroid = drug_exposure[["person_id", "drug_exposure_start_datetime", "drug_exposure_end_datetime", "ATC코드"]]
drug_steroid = drug_steroid[drug_steroid["ATC코드"].str.startswith("R03BA","H02", na=False)] # R03BA : inhaled, H02 : systemic







drug_inhaled = drug_inhaled[drug_inhaled["inhaled_start_date"] <= ["Lab_drug_start_datetime"]]










