import pandas as pd             # 완성된 데이터 셋을 만들자!
                                # 현재 가장 최근 날짜의 진단일을 가져오게 되어있음
                                # 진단일 이전의 가장 최근 날짜를 가져오게 수정 필요

person = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_샘플\\결핵CDM_v0.2(A31_2018)\\person.csv")
measurement = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_샘플\\결핵CDM_v0.2(A31_2018)\\measurement.csv")
condition_A31 = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\condition_A31.csv")

# person, measurement, condition_A31 전처리
person = person[["person_id", "gender_source_value"]]
measurement = measurement[["person_id", "measurement_concept_id", "measurement_source_value_name", "measurement_date", "value_source_value", "실시일시"]]
measurement["measurement_concept_id"] = measurement["measurement_concept_id"].fillna("").astype(str)
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

# 체중, 키, BMI
physical = [
    {"검사코드": ["4099154"], "검사명": "체중(kg)"},
    {"검사코드": ["4177340"], "검사명": "신장(cm)"},
    {"검사코드": ["40490382"], "검사명": "BMI"}
]

# 체중, 키, BMI 빈칸 채우기(입원 환자 제외 빈칸 채우는 방법?)
'''
for measure, values in physical.items():
    measurement.loc[measurement["measurement_concept_id"].isin(values), "measurement_concept_id"] = values
'''

# 체중, 키, BMI 추출
# 결핵 진단일 이전의 가장 빠른 검사일자 추출
measure_physical = pd.merge(measurement, condition_A31, on= "person_id", how= 'left')
measure_physical = measure_physical[["person_id", "NTM진단일", "measurement_concept_id", "measurement_source_value_name", "measurement_date", "실시일시", "value_source_value"]]

for measure in physical:
    temp_df = measure_physical[
        measure_physical["measurement_concept_id"].str.startswith(tuple(measure["검사코드"]), na=False)
        ]
    temp_df = temp_df.sort_values(by=["person_id", "measurement_date"], ascending=[True, True])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
    
    # 새로운 컬럼 생성 및 필요한 컬럼만 유지
    temp_df[measure["검사명"]] = temp_df["value_source_value"]
    temp_df = temp_df[["person_id", measure["검사명"]]] # "measurement_source_value_name", "measurement_date", 
    
    # 원래 데이터프레임과 병합
    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")




condition_A31.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\condition_A31_1.csv", index=False)


