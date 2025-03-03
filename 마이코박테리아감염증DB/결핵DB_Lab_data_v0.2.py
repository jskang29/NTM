import pandas as pd
import numpy as np

# measurement, drug_exposure 파일 불러오기
measurement = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\measurement.csv", low_memory= False)
condition_occurrence = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\condition_occurrence.csv")
drug_exposure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\drug_exposure.csv", low_memory= False)

# 혈액 검사, 화학 검사(코드 추가 수정 필요), 보통 동일하게 처방되는 검사들을 묶어서 생각하기
lab_code = {
    "WBC": ["L100203", "L2000301", "L602101", "L603101", "L700101"],
    "hemoglobin": ["L200203", "L602103", "L700103"],
    "platelet": ["L603109", "L200209", "L602109", "L2003209"],
    "ESR": ["L2005"],
    "CRP": ["L3052", "L5108", "L5109", "LTCRP", "L305299"],
    "glucose": ["L7115", "L100206", "L700206"],
    "albumin": ["L3021"],
    "protein": ["L3020"],
    "total bilirubin": ["L3018"],
    "AST": ["L3015", "L7118"],
    "ALT": ["L3016", "L7119"],
    "BUN": ["L3024", "L7116"],
    "creatinine": ["L3025", "L3025C", "L7125"],
    "uric_acid": ["L100312", "L2366D", "L3026", "L39040F", "L39040Q", "L700312", "L7139", "L802806", "X000506", "X000604"],
    "vitamin D": ["L32731", "N205030", "N205085"],
    "PT": ["L2102"],
    "aPTT": ["L2103"]} 
dst_code = ["L440502"] # 감수성 검사코드 추가 필요                    

condition_occurrence = condition_occurrence[["person_id", "condition_start_date", "condition_source_value", "진단명", "상병구분"]]
condition_A31 = condition_occurrence[condition_occurrence["condition_source_value"].str.startswith("A31", na= False)]

# 1-2) NTM 첫 상병입력일 추출(person_id를 기준으로 중복 제거)
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True,True])
condition_A31 = condition_A31.drop_duplicates(subset= "person_id", ignore_index= True)

# 1-3) 컬럼명 A31_start_date로 변경
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]

# 1-4) 환자별 A31 초진일만 별도의 컬럼으로 구성
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

measurement = measurement[["person_id", "measurement_date", "measurement_source_value", "value_source_value", "unit_source_value", "결과내역"]]

# 2. DST검사유무 및 결과내역 
filtered_DST = measurement[
    measurement["measurement_source_value"].astype(str).isin(dst_code)
]
measurement_dst = pd.merge(condition_A31, filtered_DST, on= 'person_id', how= 'left')

# 2-1) 측정 날짜가 동일한 행을 추출하여 검사를 1개의 행으로 구성한다.
# 2-2) 구성한 행 중 'NTM진단일'과 가장 가까운 날의 결과값을 가져온다.




# 2. ESR, CRP, Albumin, Total protein 검사결과가 포함된 행을 필터링한다.
# 2-1) measurement_Lab에 포함된 모든 measurement_source_value 리스트 생성
lab_values = [value for values in lab_code.values() for value in values]

# 2-2) measurement_source_value가 measurement_Lab에 포함되는 행만 필터링
filtered_measurement = measurement[
    measurement["measurement_source_value"].astype(str).isin(lab_values)
]

# 3. 
measurement_Lab = pd.merge(condition_A31, filtered_measurement, on= 'person_id', how= 'left')




# 3. 새로운 데이터 테이블 형성
DB_lab_data = []

for person_id in treat_measure["person_id"].drop_duplicates():
    치료시작일 = treat_measure.loc[(treat_measure["person_id"] == person_id), "drug_exposure_start_datetime"].iloc[0]
    검사일 = treat_measure.loc[(treat_measure["person_id"] == person_id), "measurement_date"].iloc[0] # 검사일시? 보고일시?
    ESR = treat_measure.loc[(treat_measure["person_id"] == person_id) & (treat_measure["measurement_source_value"] == 'L2005'), "value_source_value"]
    CRP = treat_measure.loc[(treat_measure["person_id"] == person_id) & (treat_measure["measurement_source_value"] == 'L3052'), "value_source_value"]
    Albumin = treat_measure.loc[(treat_measure["person_id"] == person_id) & (treat_measure["measurement_source_value"] == 'L3021'), "value_source_value"]
    Protein = treat_measure.loc[(treat_measure["person_id"] == person_id) & (treat_measure["measurement_source_value"] == 'L3020'), "value_source_value"]
    
    if ESR.empty:
        ESR = None
    else:
        ESR = ESR.iloc[0]
    
    if CRP.empty:
        CRP = None
    else:
        CRP = CRP.iloc[0]

    if Albumin.empty:
        Albumin = None
    else:
        Albumin = Albumin.iloc[0]

    if Protein.empty:
        Protein = None
    else:
        Protein = Protein.iloc[0]

    
    DB_lab_data.append({
        "person_id": person_id,
        "치료시작일": 치료시작일,
        "치료_시작시_ESR": ESR,
        "치료_시작시_CRP": CRP,
        "치료_시작시_Albumin": Albumin,
        "치료_시작시_Protein": Protein,
        "치료_종료시_ESR": None,
        "치료_종료시_CRP": None        
    })


DB_lab_data = pd.DataFrame(DB_lab_data)



print(DB_lab_data.head(10))


# DB_lab_data.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_전체\\DB_lab_data.csv")

#  


# print(treat_measure.loc[treat_measure["person_id"]==3])





'''
# 3. 새로운 데이터 테이블 형성(person_id와 매핑할 때, 없으면 빈칸으로 처리, 아마 merge 방식을 left로 하면 될 듯?)
DB_Lab_data = pd.DataFrame({
    "person_id" = treat_measure["person_id"],
    "drug_exposure_start_datetime" = treat_measure["drug_exposure_start_datetime"],
    "measurement_datetime" = treat_measure["measurement_datetime"],
    "ESR" = treat_measure["measurement_datetime"]


})
'''









# treat_measure.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\treat_measure.csv")

# treatment_first_date와 Lab_measure 매핑(치료시작일 이전에 검사를 한 경우가 대부분일 것이다??)

# treatment_measure = pd.merge(treatment_first_date, Lab_measure, how= 'left', left_on= ["person_id", "drug_exposure_start_datetime"], right_on= ["person_id", "measurement_datetime"])
























