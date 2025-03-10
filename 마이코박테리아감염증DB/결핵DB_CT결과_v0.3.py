# procedure_occurrence의 finding과 conclusion(결론 및 진단) DB확인
# 빈칸은 전부 온점(.)으로 찍히므로 대쉬(_)로 변경
# 6min walking : S40018, S40099 --> 신체정보 추출 가능여부 확인
# B.D.R --> FEV1, FEV1%, FVC, FVC%, DLCO 추출 가능여부 확인
# ctrl shift L : 같은 단어 선택
import pandas as pd 
import re
# person = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\샘플CDMV2_20241211\\person.csv")
# JBCP 경로
procedure = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\CDM\\CDM_csv\\CDM_A31_old\\CDM_A31_전체\\CDM_A31_전체 - 복사본\\procedure_occurrence.csv")
condition_A31 = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\증상_기저질환_기존.csv")

# 내컴퓨터 경로
# procedure = pd.read_csv("")
# condition_A31 = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\결핵DB_샘플\\기저질환.csv")

# 치료시작일 코드필요(추후 작성)
radiology = [
    {"검사코드": ["RC121", "RC194", "RC314", "RC363", "RC561", "RC994", "RC131", "RC200", "RCEX05", "RCEX06"], "검사명": "chestCT", "항목": "CT검사날짜"} # 외부CT판독결과 코드 추가("RCEX05", "RCEX06")
    # {"검사코드": ["RG011", "RG012", "RG308"], "검사명": "CXR", "항목": "CXR검사날짜"}          
]   

CT_result = [
    {"검사결과": ["Cavity", "Cavitary mmass", "Cavities"], "항목": "Cavity유무"},
    {"검사결과": ["Emphysema"], "항목": "Emphysema유무"},
    {"검사결과": ["Bronchiectasis"], "항목": "Bronchiectasis유무"},
    # Centrilobular nodule 추가
    {"검사결과": ["Centrilobular nodules", "Centrilobular nodule"], "항목": "Centrilobular_nodules유무"},
    {"검사결과": ["Consolidation"], "항목": "Consolidation유무"},
    {"검사결과": ["Pleural effusion"], "항목": "Pleural_effusion유무"},
    {"검사결과": ["Aggravation"], "항목": "Aggravation유무"}
]

# 진단 전 결과보고된 CT에서 cavity, bronchiectasis, emphysema 여부 확인
# 실시일시가 빈칸인 경우 해결 방법 생각
# CT검사날짜, cavity, bronchiectasis, emphysema 날짜 --> cavity, bronchiectasis, emphysema 유무
# 결과내역이 EMR의 CT결과를 그대로 저장하는지 확인 필요
# 같은 날의 CT가 아닌 경우 CT 날짜를 출력할 수 있도록??


procedure = procedure[["person_id", "procedure_date", "실시일시", "보고일시", "procedure_source_value", "procedure_source_value_name", "결과내역", "결론 및 진단"]]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]
procedure_radiology = pd.merge(procedure, condition_A31, on= "person_id", how= 'left')


# "NTM"을 포함하는 CT결과내역 추출(코드 작성)
procedure_radiology = procedure_radiology[procedure_radiology["결과내역"].str.contains("NTM", case= False, na= False)] 
# 기관지 내시경 출력되는 문제 : 첫 번째 for문에서 CT결과를 포함하게 만든 temp_df는 첫 번째 for문에서만 작동하여 두 번째 for문에서 필터링 되지 않고 
# cavity, bronchiectasis를 포함하는 모든 검사결과 중 "상병입력일"과 가장 가까운 결과값이 출력됨

# 진단 전 CT보고일 및 결과내역
# 진단일 이전에 보고된 CT기록을 필터링하고, 진단일과 가장 가까운 날의 CT기록을 필터링하고, 해당 CT기록의 "실시일시"를 새로운 컬럼 "CT검사날짜"에 저장한다.
# 보고일시가 동일하다면 실시일시가 더 나중인 CT결과를 필터링 하도록 코드 추가 필요(###) : 외부CT와 본원CT가 동시에 처방된 경우, 외부CT가 더 최근 기록으로 추출되는 경우가 발생(이문제를 해결하는 법??)
# NTM필터링 --> 진단일 이전에 보고된 CT결과 필터링 --> 특정소견을 보이는 CT결과(bronchiectasis, cavity..)필터링 --> 보고된 CT결과의 "실시일시"를 "CT검사날짜"로 입력

# 진단일 이전에 보고된 CT결과 필터링
for key in radiology:
    procedure_radiology = procedure_radiology[procedure_radiology["NTM진단일"] >= procedure_radiology["보고일시"]]
    procedure_radiology = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(key["검사코드"]), na= False)
    ]
    '''
    # 날짜 추출을 위해 temp_df를 사용하여 정렬(나중에 활용할 코드)
    temp_df = procedure_radiology
    temp_df = temp_df.sort_values(by= ["person_id", "보고일시"], ascending= [True, False])
    '''

# 특정소견을 보이는 CT결과(bronchiectasis, cavity..) 필터링
for key in CT_result:
    key_change = "|".join(map(re.escape, key["검사결과"]))
    
    temp_df = procedure_radiology[
        procedure_radiology["결과내역"].str.contains(key_change, case= False, na= False)
        ]
    temp_df = temp_df.sort_values(by= ["person_id", "보고일시"], ascending= [True, False])
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)

    temp_df[key["항목"]] = temp_df["결과내역"]
    temp_df = temp_df[["person_id", key["항목"]]]

    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")

# 
for column in ["Cavity유무", "Emphysema유무", "Bronchiectasis유무", "Centrilobular_nodules유무", "Consolidation유무", "Pleural_effusion유무", "Aggravation유무"]:
    if column in condition_A31.columns:  # 해당 컬럼이 존재하는지 확인
        condition_A31[column] = condition_A31[column].apply(lambda x: "Y" if pd.notna(x) else "N")


# 부정 표현(여기서는 "보이지 않음", "소실됨" 등)을 리스트로 정의
NEGATIVE_PHRASES = ["no", "보이지 않음", "사라졌음", "소실", "없음"]

def check_keyword(ct_report, keyword, negative_phrases=NEGATIVE_PHRASES):
    """
    - ct_report(문자열)에서 keyword가 들어있는 문장을 찾는다.
    - 그 문장 안에 negative_phrases 중 하나라도 포함되어 있으면 "N", 
      그렇지 않으면 "Y"를 반환한다.
    - keyword가 전혀 등장하지 않으면, 기본적으로 "Y"를 반환(기존 로직을 유지하기 위한 설정)
    """
    # 문장 단위로 분리
    sentences = [s.strip() for s in re.split(r'[.!?]', ct_report)]
    
    # keyword가 들어있는 문장만 필터링
    matched_sentences = [s for s in sentences if keyword.lower() in s.lower()]
    
    # 해당 문장들 중에 '보이지 않음', '소실됨' 같은 부정 표현이 있으면 "N"
    for sentence in matched_sentences:
        # 문장에 negative_phrases 중 하나라도 들어 있으면 "N"
        if any(neg in sentence for neg in negative_phrases):
            return pd.NA
    # 한 번도 부정 표현을 만나지 않았다면 "Y" 반환
    return ct_report










condition_A31.to_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\CT_CT_2.csv", index=False)
