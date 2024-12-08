import pandas as pd
import numpy as np

# measurement, drug_exposure 파일 불러오기
measurement = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\measurement.csv", low_memory= False)
drug_exposure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\drug_exposure.csv", low_memory= False)
measurement_L = ["L2005", "L3052", "L3021", "L3020"] # 혈액 검사, 화학 검사(코드 확정)
measurement_smear = ["L403313", "L40113"] # 도말 검사코드 추가 필요
measurement_DST = ["L440502"] # 감수성 검사코드 추가 필요                    
measurement_CUL = ["L410011"] # 배양검사 코드 추가 필요("L411162" : bronchial, "L440040" : exporated sputum)
code_atc = {
    "J01FA09": "clarithromycin",
    "J01FA10": "azithromycin",
    "J04AB02": "rifampicin",
    "J04AK02": "ethambutol",
    "J04AC01": "isoniazid",
    "J01GB06": "amikacin",
    "J01DC01": "cefoxitin",
    "J01DH51": "imipenem and cilastatin"
}

Lab_drug = drug_exposure[["person_id", "drug_source_value", "drug_exposure_start_datetime", "ATC코드", "ATC 코드명"]]

for atc_code, drug_name in code_atc.items():
    source = Lab_drug[Lab_drug["ATC코드"].str.contains(atc_code, na= False)]["drug_source_value"]
    Lab_drug.loc[Lab_drug["drug_source_value"].isin(source), ["ATC코드", "ATC 코드명"]] = [atc_code, drug_name]

# L2005(ESR), L3052(CRP), L3021(Albumin), L3020(Total protein)
# df2 테이블의 drug_exposure_datetime 이전 날짜 중 df1의 measurement_start_datetime컬럼과 가장 빠른 날의 value_source_value컬럼값을 가져온다(measurement_source_value중 L2005, L3052)
# df2의 drug_exposure_start_datetime > measurement_start_datetime

# 1. 최소 2개 이상의 서로 다른 항결핵제를 사용한 최초 날짜를 구한다.(치료 시작일??, 2가지 이상이면 만족??), 재발 후 치료를 시작했을 때는 어떻게 할 것인가?
# 1-1) 항결핵제 처방기록 추출
Lab_drug = Lab_drug.sort_values(by=["person_id", "drug_exposure_start_datetime"], ascending=[True, True])
Lab_drug = Lab_drug[Lab_drug["ATC코드"].isin(code_atc.keys())]     # 항결핵제 처방받은 기록만 추출

# 1-2) 항결핵제를 같은 날 2종류 이상 처방받은 drug_exposure_start_datetime을 추출한다.(치료 시작일)
Lab_drug = Lab_drug.groupby(["person_id", "drug_exposure_start_datetime"]).filter(lambda x: len(set(x["ATC코드"])) >= 2)
Lab_drug = Lab_drug.drop_duplicates(subset= ["person_id", "drug_exposure_start_datetime", "ATC코드"])

# 1-3) 항결핵제 첫 투약일 추출(치료 시작일)
Lab_drug = Lab_drug.drop_duplicates(subset= "person_id", ignore_index= True)

treatment_first_date = Lab_drug[["person_id", "drug_exposure_start_datetime"]]


# measurement 테이블 전처리(환자별 치료 시작일과 기본검사결과를 합친 데이터 셋)

# 1. ESR, CRP, Albumin, Total protein 검사결과가 포함된 행을 필터링한다.
# 1-1)'measurement_source_value' = 'L2005', 'L3052', 'L3021', 'L3020'인 데이터 셋 만들기
measure_total = measurement[["person_id", "measurement_date", "measurement_source_value", "value_source_value", "unit_source_value", "결과내역"]]

Lab_measure = measure_total[measure_total["measurement_source_value"].isin(measurement_L)]


# 1-2) '치료 시작일 이전의 데이터를 뽑아서 새로운 데이터 셋 만들기'
treat_measure = pd.merge(treatment_first_date, Lab_measure, on= "person_id", how= 'inner')
treat_measure['drug_exposure_start_datetime'] = pd.to_datetime(treat_measure['drug_exposure_start_datetime'], format= 'ISO8601')
treat_measure['measurement_date'] = pd.to_datetime(treat_measure["measurement_date"], format= 'ISO8601')

treat_measure = treat_measure[treat_measure["measurement_date"] <= treat_measure["drug_exposure_start_datetime"]]
treat_measure = treat_measure.sort_values(by= ["person_id", "drug_exposure_start_datetime", "measurement_date"], ascending=[True,True,True], ignore_index= True)


# 2. '치료 시작일과 가장 가까운 날의 데이터 셋 만들기'(데이터 셋에서 치료 시작일 가장 가까운 날의 검사결과를 추출하는 코드
# 2-1) 환자별로 가장 가까운 날의 데이터 셋 형성(결과가 있는 경우, 없는 경우 있음)
treat_measure['time_difference'] = (treat_measure['drug_exposure_start_datetime'] - treat_measure['measurement_date']).abs()
min_time_by_person = treat_measure.groupby('person_id')['time_difference'].transform('min')
treat_measure = treat_measure[treat_measure['time_difference'] == min_time_by_person]
treat_measure = treat_measure.drop_duplicates(subset= ["person_id", "measurement_source_value", "time_difference"]) 

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
























