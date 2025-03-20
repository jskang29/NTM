import pandas as pd

measurement_after = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\Lab_결과\\labdata\\진단후\\measurement_A31_lab_진단일이후.csv")

# 총 환자 수
total_patients = 2490

measurement_after['NTM진단일'] = pd.to_datetime(measurement_after['NTM진단일'])
measurement_after['measurement_date'] = pd.to_datetime(measurement_after['measurement_date'])

# 1. 진단일 이후 4주 이내 데이터 필터링
measurement_after_1M = measurement_after[
    (measurement_after["measurement_date"] - pd.Timedelta(days=28) <= measurement_after["NTM진단일"]) &
    (measurement_after["measurement_date"] > measurement_after["NTM진단일"])
]

# 항목별 환자 수 계산
patient_count_per_test_1M = measurement_after_1M.groupby('measurement_source_value')['person_id'].nunique().reset_index()
patient_count_per_test_1M.rename(columns={'person_id': 'patient_count'}, inplace=True)

# 추출률(%) 계산
patient_count_per_test_1M['extraction_rate'] = (patient_count_per_test_1M['patient_count'] / total_patients) * 100


# 2. 진단일 이후 12주(84일) 이내 데이터 필터링
measurement_after_3M = measurement_after[
    (measurement_after["measurement_date"] - pd.Timedelta(days=84) <= measurement_after["NTM진단일"]) &
    (measurement_after["measurement_date"] > measurement_after["NTM진단일"])
]

# 항목별 환자 수 계산
patient_count_per_test_3M = measurement_after_3M.groupby('measurement_source_value')['person_id'].nunique().reset_index()
patient_count_per_test_3M.rename(columns={'person_id': 'patient_count'}, inplace=True)

# 추출률 계산
patient_count_per_test_3M['extraction_rate'] = (patient_count_per_test_3M['patient_count'] / 2490) * 100

# 3. 진단일 이후 24주(168일) 이내 데이터 필터링
measurement_after_6M = measurement_after[
    (measurement_after["measurement_date"] - pd.Timedelta(days=168) <= measurement_after["NTM진단일"]) &
    (measurement_after["measurement_date"] > measurement_after["NTM진단일"])
]

# 항목별 환자 수 계산
patient_count_per_test_6M = measurement_after_6M.groupby('measurement_source_value')['person_id'].nunique().reset_index()
patient_count_per_test_6M.rename(columns={'person_id': 'patient_count'}, inplace=True)

# 추출률 계산
patient_count_per_test_6M['extraction_rate'] = (patient_count_per_test_6M['patient_count'] / 2490) * 100

