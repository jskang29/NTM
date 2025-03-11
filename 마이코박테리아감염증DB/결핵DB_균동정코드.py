import pandas as pd

measurement_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\measurement.csv"
condition_occurrence_path = "C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\condition_occurrence(regtime추가).csv"

# condition_occurrence 데이터 로드
condition_occurrence = pd.read_csv(condition_occurrence_path)
condition_occurrence = condition_occurrence[["person_id", "condition_start_date", "condition_source_value"]]

# A31 진단 데이터 필터링
condition_A31 = condition_occurrence[condition_occurrence["condition_source_value"].str.startswith("A31", na=False)]
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True, True])
condition_A31 = condition_A31.drop_duplicates(subset="person_id", ignore_index=True)
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

# 감수성 검사 코드
dst_code = ["L440502"] 

# 균동정 검사 코드
ID_code = ["L44042", "L8144"]

chunk_size = 100000

measurement_DST_ID = []

# 청크 단위로 measurement 데이터 읽기
for chunk in pd.read_csv(measurement_path, usecols=["person_id", "measurement_date", "measurement_source_value", "value_source_value"], chunksize=chunk_size):
    # measurement_source_value가 Lab 검사 코드에 포함되는 경우만 필터링
    chunk = chunk[chunk["measurement_source_value"].astype(str).isin(dst_code + ID_code)]
    
       
    # measurement_Lab = measurement_Lab.sort_values(by=["person_id", "measurement_source_value", "measurement_date"], ascending=[True, True, False])
    # measurement_Lab = measurement_Lab.groupby(["person_id", "measurement_source_value"]).first().reset_index()

    # 청크 결과 저장
    measurement_DST_ID.append(chunk)

measurement_DST_ID = pd.concat(measurement_DST_ID, ignore_index=True)
measurement_DST_ID = measurement_DST_ID.sort_values(by=["person_id", "measurement_date"], ascending=[True, True])

measurement_DST = measurement_DST_ID[measurement_DST_ID["measurement_source_value"].isin(dst_code)]
measurement_ID = measurement_DST_ID[measurement_DST_ID["measurement_source_value"].isin(ID_code)]

measurement_DST = pd.merge(condition_A31, measurement_ID, on= "person_id", how= "left")
measurement_ID = pd.merge(condition_A31, measurement_ID, on= "person_id", how= "left")
measurement_DST_ID = pd.merge(condition_A31, measurement_DST_ID, on= "person_id", how= "left")


measurement_DST_ID.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_ID_DST.csv", index=False)
measurement_DST.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_DST.csv", index=False)
measurement_ID.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\measurement_ID.csv", index=False)

