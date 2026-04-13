import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from datetime import date

DB_PATH = "ecos_warehouse.db"

st.set_page_config(page_title="부재중 4통", page_icon="📊", layout="wide")
st.title("📊 부재중 네통 | 한국 경제지표 자료")
st.caption(f"ECOS 한국은행 | 마지막 업데이트: {date.today()}")

@st.cache_data(ttl=3600)
def get_sectors():
    con = duckdb.connect(DB_PATH, read_only=True)
    sectors = con.execute("SELECT DISTINCT sector FROM timeseries ORDER BY sector").fetchdf()
    con.close()
    return sectors["sector"].tolist()

@st.cache_data(ttl=3600)
def get_indicators(sector):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(
        "SELECT DISTINCT name, cycle FROM timeseries WHERE sector=? ORDER BY name",
        [sector]
    ).fetchdf()
    con.close()
    return df

@st.cache_data(ttl=3600)
def get_timeseries(name):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(
        "SELECT time, value FROM timeseries WHERE name=? ORDER BY time",
        [name]
    ).fetchdf()
    con.close()
    return df

try:
    sectors = get_sectors()
except Exception:
    st.error("⚠️ DB 파일(ecos_warehouse.db)을 찾을 수 없어요. 먼저 fetch_ecos.py를 실행해주세요.")
    st.stop()

st.sidebar.header("🗂️ 섹터 선택")
selected_sector = st.sidebar.radio("섹터", sectors)

indicators_df = get_indicators(selected_sector)
indicator_names = indicators_df["name"].tolist()

st.subheader(f"📁 {selected_sector}")

# ── 지표 버튼 (key에 sector 포함 → 중복 방지) ─────────────────────────────
cols = st.columns(4)
if "selected_indicator" not in st.session_state or st.session_state["selected_indicator"] not in indicator_names:
    st.session_state["selected_indicator"] = indicator_names[0]

for i, name in enumerate(indicator_names):
    with cols[i % 4]:
        btn_key = f"btn_{selected_sector}_{name}"   # ✅ sector 포함으로 중복 키 해결
        if st.button(name, key=btn_key, use_container_width=True):
            st.session_state["selected_indicator"] = name

selected_indicator = st.session_state["selected_indicator"]

st.markdown("---")
df = get_timeseries(selected_indicator)

# ── 빈 데이터 처리 ────────────────────────────────────────────────────────
if df.empty:
    st.warning(f"'{selected_indicator}' 데이터가 없어요. DB를 확인해주세요.")
    st.stop()

cycle_row = indicators_df[indicators_df["name"] == selected_indicator]
cycle = cycle_row["cycle"].values[0] if not cycle_row.empty else "M"

def parse_time(t, cycle):
    try:
        t = str(t).strip()
        if cycle == "D":
            return pd.to_datetime(t, format="%Y%m%d")
        elif cycle == "M":
            return pd.to_datetime(t, format="%Y%m")
        elif cycle == "Q":
            # ECOS 분기 형식: "2015Q1" → 1월, "2015Q2" → 4월 ...
            year, q = t.split("Q")
            month = (int(q) - 1) * 3 + 1
            return pd.Timestamp(int(year), month, 1)
        else:  # A
            return pd.to_datetime(t, format="%Y")
    except:
        return pd.NaT

df["date"] = df["time"].apply(lambda t: parse_time(t, cycle))
df = df.dropna(subset=["date"]).sort_values("date")

if df.empty:
    st.warning(f"'{selected_indicator}' 날짜 파싱에 실패했어요.")
    st.stop()

# ── 최신값 표시 ───────────────────────────────────────────────────────────
latest = df["value"].iloc[-1]
prev   = df["value"].iloc[-2] if len(df) > 1 else latest
delta  = latest - prev

c1, c2, c3 = st.columns(3)
c1.metric("최신값", f"{latest:,.2f}", f"{delta:+.2f}")
c2.metric("최고값", f"{df['value'].max():,.2f}")
c3.metric("최저값", f"{df['value'].min():,.2f}")

# ── 기간 필터 (전체=9999 → min date 사용으로 안전하게) ───────────────────
period = st.select_slider("기간 선택", options=["1년","3년","5년","10년","전체"], value="5년")
years_map = {"1년": 1, "3년": 3, "5년": 5, "10년": 10, "전체": None}
years = years_map[period]

if years is None:
    df_f = df.copy()
else:
    max_date = df["date"].max()
    # ✅ 연도 범위 초과 방지: max_date.year - years가 0 미만이면 min date로 clamp
    try:
        cutoff = max_date - pd.DateOffset(years=years)
    except Exception:
        cutoff = df["date"].min()
    df_f = df[df["date"] >= cutoff]

# ── Plotly 그래프 ─────────────────────────────────────────────────────────
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=df_f["date"], y=df_f["value"],
    mode="lines", name=selected_indicator,
    line=dict(color="#01696f", width=2),
    fill="tozeroy", fillcolor="rgba(1,105,111,0.08)"
))
fig.update_layout(
    title=dict(text=selected_indicator, font=dict(size=18)),
    xaxis_title="날짜", yaxis_title="값",
    hovermode="x unified",
    plot_bgcolor="white", paper_bgcolor="white",
    height=450,
    margin=dict(l=40, r=20, t=60, b=40),
    xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
    yaxis=dict(showgrid=True, gridcolor="#f0f0f0")
)
st.plotly_chart(fig, use_container_width=True)   # 경고는 무시해도 됨 (기능은 정상)

with st.expander("📋 원본 데이터 보기"):
    st.dataframe(
        df_f[["date","value"]].rename(columns={"date":"날짜","value":"값"}).sort_values("날짜", ascending=False),
        use_container_width=True
    )

st.markdown("---")
st.caption("데이터 출처: 한국은행 경제통계시스템(ECOS) | ecos.bok.or.kr")