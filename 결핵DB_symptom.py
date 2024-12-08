import pandas as pd

condition_occurrence = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\condition_occurrence.csv")
condition_occurrence["condition_start_date"] = pd.to_datetime(condition_occurrence["condition_start_date"])
condition_occurrence["condition_start_date"] = condition_occurrence["condition_start_date"].dt.date

# 1. NTM 초진일 구하기
# 1-1) NTM 전체 상병입력일 추출(A31 상병입력일 추출)
condition_comorbid = condition_occurrence[["person_id", "condition_start_date", "condition_source_value", "진단명", "상병구분"]]
condition_A31_first_date = condition_comorbid[condition_comorbid["condition_source_value"].str.startswith("A31", na= False)]

# 1-2) NTM 첫 상병입력일 추출(person_id를 기준으로 중복 제거)
condition_A31_first_date = condition_A31_first_date.sort_values(by=["person_id", "condition_start_date"], ascending=[True,True])
condition_A31_first_date = condition_A31_first_date.drop_duplicates(subset= "person_id", ignore_index= True)

# 1-3) 컬럼명 A31_start_date로 변경
condition_A31_first_date["A31_start_date"] = condition_A31_first_date["condition_start_date"]

# 1-4) 환자별 A31 초진일만 별도의 컬럼으로 구성
condition_A31_date = condition_A31_first_date[["person_id", "A31_start_date"]]

# 2. NTM 초진일 이전의 증상 발현일 추출(진단일 전 가장 가까운 날의 symptom 기록 추출)

# 2-1) Cough(R05), Loss of appetite(R630, R630-00), Fatigue(R53), Fever(R50), Hemoptysis(R042) 전체 상병 입력일 추출
condition_symptom = condition_comorbid[condition_comorbid["condition_source_value"].str.startswith(("R05", "R630", "R53", "R50", "R042"), na= False)]
condition_symptom = pd.merge(condition_symptom, condition_A31_date, on= 'person_id', how= 'inner')
condition_symptom.rename(columns={"condition_start_date" : "symptom_start_date"}, inplace= True)

condition_symptom = condition_symptom[condition_symptom["symptom_start_date"] <= condition_symptom["A31_start_date"]]

condition_symptom.loc[condition_symptom['condition_source_value'].str.startswith('R05'), 'condition_source_value'] = 'R05' # R05 및 하위 코드를 R05로 변경
condition_symptom.loc[condition_symptom['condition_source_value'].str.startswith('R630'), 'condition_source_value'] = 'R630'
condition_symptom.loc[condition_symptom['condition_source_value'].str.startswith('R53'), 'condition_source_value'] = 'R53'
condition_symptom.loc[condition_symptom['condition_source_value'].str.startswith('R50'), 'condition_source_value'] = 'R50'
condition_symptom.loc[condition_symptom['condition_source_value'].str.startswith('R042'), 'condition_source_value'] = 'R042'


# 2-2) NTM 초진일과 가장 가까운 증상 진단일을 추출
# "time_difference" = NTM 진단일과 Cough ~ Hemoptysis 상병 입력일의 차이
condition_symptom["time_difference"] = (condition_symptom["A31_start_date"] - condition_symptom["symptom_start_date"]).abs()
min_time_by_person = condition_symptom.groupby(["person_id", "condition_source_value"])['time_difference'].transform('min')
condition_symptom = condition_symptom[condition_symptom["time_difference"] == min_time_by_person]
condition_symptom = condition_symptom.drop_duplicates(subset= ["person_id", "condition_source_value", "time_difference"])

print(condition_symptom["symptom_start_date"].dtypes)

# print(condition_symptom.head(50))

DB_symptom = []

for person_id in condition_symptom["person_id"].drop_duplicates(): 
    NTM진단일 = condition_symptom.loc[(condition_symptom["person_id"] == person_id), "A31_start_date"].iloc[0]
    Cough = condition_symptom.loc[(condition_symptom["person_id"] == person_id) & (condition_symptom["condition_source_value"] == 'R05'), "symptom_start_date"]
    Loss_of_appetite = condition_symptom.loc[(condition_symptom["person_id"] == person_id) & (condition_symptom["condition_source_value"] == 'R630'), "symptom_start_date"]
    Fatigue = condition_symptom.loc[(condition_symptom["person_id"] == person_id) & (condition_symptom["condition_source_value"] == 'R53'), "symptom_start_date"]
    Fever = condition_symptom.loc[(condition_symptom["person_id"] == person_id) & (condition_symptom["condition_source_value"] == 'R50'), "symptom_start_date"]
    Hemoptysis = condition_symptom.loc[(condition_symptom["person_id"] == person_id) & (condition_symptom["condition_source_value"] == 'R042'), "symptom_start_date"]
    
    if Cough.empty:
        Cough = "알수없음"
    else:
        Cough = Cough.iloc[0]   # 왜 Cough[0]은 안될까? Cough 시리즈에 index가 0인 값이 없다

    if Loss_of_appetite.empty: 
        Loss_of_appetite = "알수없음"
    else:
        Loss_of_appetite = Loss_of_appetite.iloc[0]
    
    
    if Fatigue.empty:
        Fatigue = "알수없음"
    else:
        Fatigue = Fatigue.iloc[0]

    if Fever.empty:
        Fever = "알수없음"
    else:
        Fever = Fever.iloc[0]

    if Hemoptysis.empty:
        Hemoptysis = "알수없음"
    else:
        Hemoptysis = Hemoptysis.iloc[0]



    DB_symptom.append({
        "person_id": person_id,
        "NTM진단일": NTM진단일,
        "Cough": Cough,
        "Loss_of_appetite": Loss_of_appetite,
        "Fatigue": Fatigue,
        "Fever": Fever,
        "Hemoptysis": Hemoptysis
    })

DB_symptom = pd.DataFrame(DB_symptom)





    
# print(DB_symptom.head(10))

# 행은 2개인데, array는 1개만 추가가 된거임.
DB_symptom.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_전체\\DB_symptom.csv", index= False)





