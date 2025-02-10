import pandas as pd             # 완성된 데이터 셋을 만들자!
                                # 진단일 이전의 가장 최근 날짜를 가져오게 수정 필요
# 
person = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\person.csv")              # person 테이블 로드
# measurement = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\measurement.csv")    # measurement 테이블 로드
condition_A31 = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\증상_기저질환.csv") 

# 메모리 부족으로 인한 chunk 단위로 measurement 로드
chunksize = 100000 
chunk_list = [] 
for chunk in pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_regtime추가\\measurement.csv", chunksize=chunksize):
    chunk_list.append(chunk)

measurement = pd.concat(chunk_list, ignore_index=True)

# person, measurement, condition_A31 전처리
person = person[["person_id", "person_source_value", "gender_source_value"]]
measurement = measurement[["person_id", "measurement_concept_id", "measurement_source_value_name", "measurement_date", "value_source_value", "실시일시"]]
measurement["measurement_concept_id"] = measurement["measurement_concept_id"].fillna("").astype(str)
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

# 체중, 키, BMI
physical = [
    {"검사코드": ["4099154"], "검사명": "체중", "검사결과": "체중kg"},
    {"검사코드": ["4177340"], "검사명": "신장", "검사결과": "신장cm"},
    {"검사코드": ["40490382"], "검사명": "BMI", "검사결과": "bmi"}
]

# 체중, 키, BMI 추출
measure_physical = pd.merge(measurement, condition_A31, on= "person_id", how= 'left')
measure_physical = measure_physical[["person_id", "NTM진단일", "measurement_concept_id", "measurement_source_value_name", "measurement_date", "실시일시", "value_source_value"]]

measure_A31 = condition_A31

# 1. 진단일 이전의 키/몸무게
# 1) "measurement_date <= NTM진단일"인 데이터만 남김
measure_physical_pre = measure_physical[
    measure_physical["measurement_date"] <= measure_physical["NTM진단일"]
].copy()

# 2) measurement_concept_id -> 실제 검사명(체중kg, 신장cm, bmi) 매핑 사전 만들기 -> {"4099154" : "체중kg"}, {"4177340" : "신장cm"} 딕셔너리 생성
map_dict = {}
for measure in physical:                # "physical" list에 저장된 3개의 딕셔너리를 순차적으로 불러온다
    for code in measure["검사코드"]:     
        # 예: code="4099154" -> "체중kg"
        map_dict[code] = measure["검사결과"] # {"4099154" : "체중kg"}, {"4177340" : "신장cm"}와 같은 딕셔너리 생성

# 3) measure_physical_pre에 '결과타입(검사결과이름)' 컬럼을 만들어 매핑 -> 매핑이 안될경우 NaN
measure_physical_pre["결과타입"] = measure_physical_pre["measurement_concept_id"].map(map_dict)

# 4) person_id, measurement_date, NTM진단일을 인덱스로 피벗
#    columns='결과타입' → "체중kg", "신장cm", "bmi" 등을 열로 만듦, 값은 value_source_value
df_pivot_pre = measure_physical_pre.pivot_table(
    index=["person_id", "NTM진단일", "measurement_date"],
    columns="결과타입",
    values="value_source_value",
    aggfunc="first"  # 같은 날짜에 여러 값이 있으면 그 중 하나
).reset_index()

# 5) 체중과 신장이 동시에 존재하는(둘 다 null이 아닌) 날짜만 필터
#    (BMI도 필요하다면 조건을 확장: & df_pivot["bmi"].notnull() )
df_pivot_pre = df_pivot_pre[df_pivot_pre["체중kg"].notnull() & df_pivot_pre["신장cm"].notnull()]

# 6) 이 중에서 NTM진단일에 '가장 가깝게(= 가장 최근)' 측정된 날짜를 찾기
#    이미 measurement_date <= NTM진단일로 필터링 했으므로, "가장 큰 날짜" = 가장 가까운 과거
df_pivot_pre = df_pivot_pre.sort_values(by=["person_id", "measurement_date"], ascending=[True, False])
df_pivot_pre = df_pivot_pre.drop_duplicates(subset=["person_id"], keep="first")

# 이제 df_pivot에는 각 person_id 당
#   - 체중kg, 신장cm가 같은 날짜에서 측정된 행
#   - 그 중 NTM진단일과 가장 가까운 날짜 1건
# 이 남아있음.

# 7) 최종적으로 merge를 위해 컬럼명 정리 (선택 사항)
#    measurement_date 자체를 '체중·신장 측정일' 등으로 이름 바꿀 수도 있음
df_pivot_pre.rename(columns={"measurement_date": "신체계측일_pre"}, inplace=True)

# 8) measure_A31에 병합
#    (병합할 때 person_id 기준으로 가져오기)
measure_A31 = pd.merge(
    measure_A31,
    df_pivot_pre[["person_id", "신체계측일_pre", "체중kg", "신장cm"]],  # bmi도 필요하면 컬럼 추가
    on="person_id",
    how="left"
)

# measure_A31.to_csv("C:\\Users\JBCP_01\\Desktop\\DB_sample\\신체계측_수정.csv", index=False)
# 이제 measure_A31에는
#   '신체계측일' 컬럼, '체중kg', '신장cm' 컬럼이
#   "동일 measurement_date & NTM진단일 이전 & 가장 최근 날짜" 기준으로 채워짐.


# 2. 진단일 이후 키/몸무게
# 1) "measurement_date > NTM진단일"인 데이터만 남김
measure_physical_post = measure_physical[
    measure_physical["measurement_date"] > measure_physical["NTM진단일"]
].copy()

# 2) measure_physical_pre에 '결과타입(검사결과이름)' 컬럼을 만들어 매핑 -> 매핑이 안될경우 NaN
measure_physical_post["결과타입"] = measure_physical_post["measurement_concept_id"].map(map_dict)

# 3) person_id, measurement_date, NTM진단일을 인덱스로 피벗
#    columns='결과타입' → "체중kg", "신장cm", "bmi" 등을 열로 만듦, 값은 value_source_value
df_pivot_post = measure_physical_post.pivot_table(
    index=["person_id", "NTM진단일", "measurement_date"],
    columns="결과타입",
    values="value_source_value",
    aggfunc="first"  # 같은 날짜에 여러 값이 있으면 그 중 하나
).reset_index()

# 5) 체중과 신장이 동시에 존재하는(둘 다 null이 아닌) 날짜만 필터
#    (BMI도 필요하다면 조건을 확장: & df_pivot["bmi"].notnull() )
df_pivot_post = df_pivot_post[df_pivot_post["체중kg"].notnull() & df_pivot_post["신장cm"].notnull()]

# 6) 이 중에서 NTM진단일에 '가장 가깝게(= 가장 최근)' 측정된 날짜를 찾기
#    이미 measurement_date > NTM진단일로 필터링 했으므로, "가장 작은 날짜" = 가장 가까운 과거
df_pivot_post = df_pivot_post.sort_values(by=["person_id", "measurement_date"], ascending=[True, True])
df_pivot_post = df_pivot_post.drop_duplicates(subset=["person_id"], keep="first")

# 이제 df_pivot에는 각 person_id 당
#   - 체중kg, 신장cm가 같은 날짜에서 측정된 행
#   - 그 중 NTM진단일과 가장 가까운 날짜 1건
# 이 남아있음.

# 7) 최종적으로 merge를 위해 컬럼명 정리 (선택 사항)
#    measurement_date 자체를 '체중·신장 측정일' 등으로 이름 바꿀 수도 있음
df_pivot_post.rename(columns={"measurement_date": "신체계측일_post"}, inplace=True)

# 8) measure_A31에 병합
#    (병합할 때 person_id 기준으로 가져오기)
measure_A31 = pd.merge(
    measure_A31,
    df_pivot_post[["person_id", "신체계측일_post", "체중kg", "신장cm"]],  # bmi도 필요하면 컬럼 추가
    on="person_id",
    how="left"
)

measure_A31.to_csv("C:\\Users\JBCP_01\\Desktop\\DB_sample\\신체계측_수정_1.csv", index=False)








# 체중, 키, BMI 빈칸 채우기(입원 환자 제외 빈칸 채우는 방법?)
'''
for measure, values in physical.items():
    measurement.loc[measurement["measurement_concept_id"].isin(values), "measurement_concept_id"] = values
'''
'''
# 체중, 키, BMI 추출
# 결핵 진단일 이전의 가장 빠른 검사일자 추출
# 실시일시가 빈칸인 값(???)
measure_physical = pd.merge(measurement, condition_A31, on= "person_id", how= 'left')
measure_physical = measure_physical[["person_id", "NTM진단일", "measurement_concept_id", "measurement_source_value_name", "measurement_date", "실시일시", "value_source_value"]]

measure_A31 = condition_A31

# NTM진단일과 가장 가까운 신체계측 날짜(key = measure)
# 1. NTM진단일 이전 신체계측 날짜
for measure in physical:
    measure_physical_pre = measure_physical[measure_physical["measurement_date"] <= measure_physical["NTM진단일"]]  # "상병입력일"이전의 측정결과 필터링

    temp_df = measure_physical_pre[
        measure_physical_pre["measurement_concept_id"].str.startswith(tuple(measure["검사코드"]), na=False)         # ㅎ
        ]
    

    temp_df = temp_df.sort_values(by=["person_id", "measurement_date"], ascending=[True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
    
    # 새로운 컬럼 생성 및 필요한 컬럼만 유지
    temp_df[measure["검사명"]] = temp_df["measurement_date"]
    temp_df[measure["검사결과"]] = temp_df["value_source_value"]
    temp_df = temp_df[["person_id", measure["검사명"], measure["검사결과"]]] # "measurement_source_value_name", "measurement_date"

   
    # 원래 데이터프레임과 병합
    measure_A31 = pd.merge(measure_A31, temp_df, on="person_id", how="left")

    # 같은 날짜에 측정한 데이터만 추출되게 코드 변경








# 2. NTM진단일 이후 신체계측 날짜
for measure in physical:
    measure_physical_post = measure_physical[measure_physical["measurement_date"] > measure_physical["NTM진단일"]]

    temp_df = measure_physical_post[
        measure_physical_post["measurement_concept_id"].str.startswith(tuple(measure["검사코드"]), na=False)
        ]
    temp_df = temp_df.sort_values(by=["person_id", "measurement_date"], ascending=[True,True])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)
    
    # 새로운 컬럼 생성 및 필요한 컬럼만 유지
    temp_df[measure["검사명"]] = temp_df["measurement_date"]
    temp_df[measure["검사결과"]] = temp_df["value_source_value"]
    temp_df = temp_df[["person_id", measure["검사명"], measure["검사결과"]]] # "measurement_source_value_name", "measurement_date"
    
    # 원래 데이터프레임과 병합
    measure_A31 = pd.merge(measure_A31, temp_df, on= "person_id", how="left")


# 3. 같은 날 측정한 체중 및 신장 결과만 추출




# measure_A31 = measure_A31[["person_id", "체중kg_x", "신장cm_x", "bmi_x"]]




# measure_physical.to_csv("C:\\Users\JBCP_01\\Desktop\\DB_sample\\physical.csv", index=False)
measure_A31.to_csv("C:\\Users\JBCP_01\\Desktop\\DB_sample\\신체계측.csv", index=False)

'''
