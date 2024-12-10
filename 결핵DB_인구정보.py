import pandas as pd             # 완성된 데이터 셋을 만들자!

person = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_샘플\\결핵CDM_v0.2(A31_2018)\\person.csv")
measurement = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_샘플\\결핵CDM_v0.2(A31_2018)\\measurement.csv")

# 
person = person[["person_id", "gender_source_value"]]
measurement = measurement[["person_id", "measurement_concept_id", "measurement_date", "value_source_value"]]

# 체중, 키, BMI
physical = {
    "Weight": ["4099154"],
    "Height": ["4177340"],
    "BMI": ["40490382"]
}

# 체중, 키, BMI 빈칸 채우기(입원 환자 제외 빈칸 채우는 방법?)
for measure, values in physical:
    measurement = measurement.loc[measurement["measurement_concept_id"].isin(values), "measurement_concept_id"] = values

# 














