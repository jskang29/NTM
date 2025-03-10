import pandas as pd
import re

# --------------------------------------------------------------------------------
# 1. 데이터 불러오기
# --------------------------------------------------------------------------------
procedure = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\procedure_occurrence.csv")
condition_occurrence = pd.read_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_CDM\\CDM_전체\\NTM_A31\\condition_occurrence.csv")
# condition_A31 = pd.read_csv("C:\\Users\\JBCP_01\\Desktop\\DB_sample\\증상_기저질환_기존.csv")
condition_occurrence = condition_occurrence[["person_id", "condition_start_date", "condition_source_value"]]

# A31 진단 데이터 필터링
condition_A31 = condition_occurrence[condition_occurrence["condition_source_value"].str.startswith("A31", na=False)]
condition_A31 = condition_A31.sort_values(by=["person_id", "condition_start_date"], ascending=[True, True])
condition_A31 = condition_A31.drop_duplicates(subset="person_id", ignore_index=True)
condition_A31["NTM진단일"] = condition_A31["condition_start_date"]
condition_A31 = condition_A31[["person_id", "NTM진단일"]]

radiology = [
    {
        "검사코드": ["RC121", "RC194", "RC314", "RC363", "RC561", "RC994", "RC131", "RC200", "RCEX05", "RCEX06"],
        "검사명": "chestCT", 
        "항목": "CT검사날짜"
    }
]

CT_result = [
    {"검사결과": ["Cavity", "Cavitary mass", "Cavities"], "항목": "Cavity유무"},
    {"검사결과": ["Emphysema"], "항목": "Emphysema유무"},
    {"검사결과": ["Bronchiectasis"], "항목": "Bronchiectasis유무"},
    {"검사결과": ["Centrilobular nodules", "Centrilobular nodule"], "항목": "Centrilobular_nodules유무"},
    {"검사결과": ["Consolidation"], "항목": "Consolidation유무"},
    {"검사결과": ["Pleural effusion"], "항목": "Pleural_effusion유무"},
    {"검사결과": ["Aggravation"], "항목": "Aggravation유무"}
]

# 필요한 컬럼만 추출
procedure = procedure[[
    "person_id", 
    "procedure_date", 
    "실시일시", 
    "보고일시", 
    "procedure_source_value", 
    "procedure_source_value_name", 
    "결과내역", 
    "결론 및 진단"
]]

condition_A31 = condition_A31[["person_id", "NTM진단일"]]

# procedure와 condition_A31 병합
procedure_radiology = pd.merge(procedure, condition_A31, on="person_id", how="left")


# --------------------------------------------------------------------------------
# 2. "NTM"을 포함하는 CT 결과만 필터링 + 진단일 이전의 보고일시만 남기기
# --------------------------------------------------------------------------------
# (1) "NTM" 단어가 "결과내역"에 포함된 행만 남김
procedure_radiology = procedure_radiology[
    procedure_radiology["결과내역"].str.contains("NTM", case=False, na=False)
]

# (2) radiology 리스트(CT 검사코드) 기준으로 진단일 이전에 보고된 CT만 필터링
for key in radiology:
    procedure_radiology = procedure_radiology[
        procedure_radiology["NTM진단일"] >= procedure_radiology["보고일시"]
    ]
    procedure_radiology = procedure_radiology[
        procedure_radiology["procedure_source_value"].str.startswith(tuple(key["검사코드"]), na=False)
    ]


# --------------------------------------------------------------------------------
# 3. 특정 소견(CT_result) 필터링 → "상병입력일"에 가장 가까운 결과만 남김
# --------------------------------------------------------------------------------
for key in CT_result:
    # 검사결과에 정의된 문자열을 OR로 연결
    key_change = "|".join(map(re.escape, key["검사결과"]))
    
    # 검사결과에 해당 문구가 포함된 결과만 필터링
    temp_df = procedure_radiology[
        procedure_radiology["결과내역"].str.contains(key_change, case=False, na=False)
    ]
    
    # 같은 person_id 내에서 가장 최근(보고일시가 가장 늦은) 순으로 정렬
    temp_df = temp_df.sort_values(by=["person_id", "보고일시"], ascending=[True, False])
    
    # person_id별로 하나의 행만 남김 (가장 최근 결과)
    temp_df = temp_df.drop_duplicates(subset="person_id", ignore_index=True)

    # 새 컬럼명(key["항목"])에 해당 결과 텍스트를 저장
    temp_df[key["항목"]] = temp_df["결과내역"]
    temp_df = temp_df[["person_id", key["항목"]]]
    
    # condition_A31와 병합
    condition_A31 = pd.merge(condition_A31, temp_df, on="person_id", how="left")


# --------------------------------------------------------------------------------
# 4. 부정 표현 처리: "no", "보이지 않음", "소실" 등이 포함되면 NA로
# --------------------------------------------------------------------------------
NEGATIVE_PHRASES = ["no", "보이지 않음", "사라졌음", "소실", "없음"]
VARIABLE = ["변화", "차이", "change", "changes"]

def check_keyword(ct_report, keyword, negative_phrases=NEGATIVE_PHRASES):
    """
    - ct_report(문자열)에서 keyword(문자열 혹은 리스트)가 들어있는 문장을 찾는다.
    - 그 문장 안에 negative_phrases가 하나라도 있으면 pd.NA 반환
    - 그렇지 않으면 원본문자열(ct_report) 반환
    - keyword가 전혀 등장하지 않으면 기본적으로 원본문자열(ct_report) 반환
    """
    # ct_report가 NaN이면 그대로 반환
    if pd.isna(ct_report):
        return pd.NA

    # 만약 keyword가 단일 문자열이 아니라 리스트면 처리
    if isinstance(keyword, str):
        keyword_list = [keyword]
    else:
        keyword_list = keyword
    '''
    # 문장 단위로 분리
    sentences = [s.strip() for s in re.split(r'[.!?-]', ct_report) if s.strip()]
    '''

    # 줄바꿈을 기준으로 문장 분리
    sentences = ct_report.split('\n')

    # 빈 문장 제거
    sentences = [sentence.strip() for sentence in sentences if sentence.strip()]    # .strip(): 문장 앞뒤의 공백을 제거하는 함수

    # keyword가 들어있는 문장만 필터링(셀값(ct소견)에 keyword를 1개라도 포함하는 문장은 모두 matched_sentences로 저장한다.)
    matched_sentences = [sentence for sentence in sentences if any(kw.lower() in sentence.lower() for kw in keyword_list)]
    
    # 부정적인 표현을 독립적인 단어로 감지하는 함수
    def contains_negative_phrase(sentence, negative_phrases):
        for neg in negative_phrases:
            if any('가' <= ch <= '힣' for ch in neg):  # 한글 포함 여부 확인
                if re.search(neg, sentence):  # 한글은 그대로 매칭
                    return True
            else:
                if re.search(r'\b' + re.escape(neg) + r'\b', sentence):  # 영어는 단어 경계 적용
                    return True
        
        return False
    
    # `matched_sentences` 내에서 부정어 검사 실행
    negative_found = any(contains_negative_phrase(sentence, negative_phrases) for sentence in matched_sentences)

    # 최종 결정 (부정어 포함 여부 확인 후 처리)
    if negative_found:
        if any(any(var in sentence for var in VARIABLE) for sentence in matched_sentences): # any: 1개라도 true이면 true를 반환
            return ct_report  # 예외 처리된 키워드 포함 → 원본 유지
        else:   
            return pd.NA  # 부정어 포함된 문장이 있으면 NA 변환

    '''
    # 부정적인 표현이 포함된 문장 중 Variable 키워드가 있는 경우 결과 소견 반환
    for sentence in matched_sentences:
        if contains_negative_phrase(sentence, negative_phrases):
            if any(var in sentence for var in VARIABLE):
                return ct_report
            else:
                return pd.NA
    '''
    return ct_report

    

# condition_A31.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\DB구축코드\\결핵_DB\\CT_부정어구_수정_v0.3.csv", index=False)

'''
    # keyword가 들어있는 문장만 필터링
    matched_sentences = []
    for sentence in sentences:
        sentence_lower = sentence.lower()
        if any(kw.lower() in sentence_lower for kw in keyword_list):
            matched_sentences.append(sentence)

    # matched_sentences 중 부정 표현이 있으면 pd.NA
    for sentence in matched_sentences:
        if any(neg in sentence for neg in negative_phrases):
            return pd.NA

    # 부정 표현이 없다면 원본 텍스트를 반환
    return ct_report
'''

# --------------------------------------------------------------------------------
# 5. check_keyword 적용 + 최종적으로 "Y"/"N" 변환
# --------------------------------------------------------------------------------
condition_A31["Cavity유무"] = condition_A31["Cavity유무"].apply(
    lambda x: check_keyword(x, ["cavity", "cavitary mass", "cavities"])
)
condition_A31["Emphysema유무"] = condition_A31["Emphysema유무"].apply(
    lambda x: check_keyword(x, "emphysema")
)
condition_A31["Bronchiectasis유무"] = condition_A31["Bronchiectasis유무"].apply(
    lambda x: check_keyword(x, "bronchiectasis")
)
condition_A31["Centrilobular_nodules유무"] = condition_A31["Centrilobular_nodules유무"].apply(
    lambda x: check_keyword(x, ["centrilobular nodules", "centrilobular nodule"])
)
condition_A31["Consolidation유무"] = condition_A31["Consolidation유무"].apply(
    lambda x: check_keyword(x, "consolidation")
)
condition_A31["Pleural_effusion유무"] = condition_A31["Pleural_effusion유무"].apply(
    lambda x: check_keyword(x, "pleural effusion")
)
condition_A31["Aggravation유무"] = condition_A31["Aggravation유무"].apply(
    lambda x: check_keyword(x, "aggravation")
)

condition_A31.to_csv("C:\\Users\\내컴퓨터\\Desktop\\결핵_DB\\DB구축코드\\결핵_DB\\CT_부정어구_v0.3.csv", index=False)


# 최종 변환: NaN이면 "N", NaN이 아니면 "Y"
for column in [
    "Cavity유무", 
    "Emphysema유무", 
    "Bronchiectasis유무", 
    "Centrilobular_nodules유무", 
    "Consolidation유무", 
    "Pleural_effusion유무", 
    "Aggravation유무"
]:
    if column in condition_A31.columns:
        condition_A31[column] = condition_A31[column].apply(lambda x: "Y" if pd.notna(x) else "N")


# --------------------------------------------------------------------------------
# 6. 결과 저장
# --------------------------------------------------------------------------------
# condition_A31.to_csv("C:\\\\Users\\\\JBCP_01\\\\Desktop\\\\DB_sample\\\\CT_CT_2.csv", index=False)
