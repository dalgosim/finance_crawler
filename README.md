# finance_crawler

### 수집항목
* comp.fnguide : 재무제표
* finance.naver : 가격, 추천종목, 종목리포트
* kind.krx : 종목명
* ~~yahoo price~~ : 가격이 일정하게 수집되지 않아 제외

### 계산항목
* 수집후 학습된 모델을 통해 주가 예측

### private project
2개의 프로젝트는 서버 인증정보와 모델이 들어있기에 sub-module로 분리해서 관리
* auth
* finance_ml
