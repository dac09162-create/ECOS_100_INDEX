# 📊 ECOS 경제지표 대시보드

한국은행 ECOS API에서 주요 경제지표를 매일 자동 수집하고 Streamlit 대시보드로 시각화합니다.

## 📁 파일 구조
```
ecos-dashboard/
├── .github/workflows/daily_fetch.yml  ← GitHub Actions 자동화
├── fetch_ecos.py                      ← 데이터 수집 스크립트
├── dashboard.py                       ← Streamlit 대시보드
├── indicators_config.json             ← 수집 지표 목록
└── ecos_warehouse.db                  ← DuckDB (자동 생성)
```

## 🚀 사용 방법

### 1. 패키지 설치
```bash
pip install requests pandas duckdb streamlit plotly
```

### 2. API 키 설정
`fetch_ecos.py` 안의 `YOUR_API_KEY_HERE`를 실제 ECOS API 키로 교체

### 3. 최초 데이터 수집
```bash
python fetch_ecos.py
```

### 4. 대시보드 실행
```bash
streamlit run dashboard.py
```

### 5. GitHub Actions 자동화
- GitHub 레포 → Settings → Secrets → Actions
- `ECOS_API_KEY` 이름으로 API 키 등록
- 매일 KST 09:00 자동 수집

## 📊 수집 섹터 (17개 섹터, 80개+ 지표)
시장금리 / 예금은행 / 가계금융 / 환율 / 주식·채권 / 국민계정·GDP /
산업생산 / 소비·투자 / 경기·심리 / 기업경영 / 가계·분배 / 고용·노동 /
국제수지 / 외환·대외 / 물가 / 부동산 / 원자재
