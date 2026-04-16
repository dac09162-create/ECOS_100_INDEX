import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from datetime import date

DB_SINGLE = "ecos_single.db"
DB_MULTI = "ecos_multi.db"

st.set_page_config(page_title="ECOS 경제지표", page_icon="📊", layout="wide")
st.title("📊 ECOS 경제지표")
st.caption(f"한국은행 ECOS 데이터 | 업데이트: {date.today()} | By 채원")

@st.cache_data(ttl=3600)
def get_all_indicators():
    rows = []
    try:
        con = duckdb.connect(DB_SINGLE, read_only=True)
        df = con.execute("SELECT DISTINCT sector, name, cycle, 'single' as db_type FROM timeseries ORDER BY sector, name").fetchdf()
        con.close()
        rows.append(df)
    except: pass
    try:
        con = duckdb.connect(DB_MULTI, read_only=True)
        df = con.execute("SELECT DISTINCT sector, name, cycle, 'multi' as db_type FROM timeseries_multi ORDER BY sector, name").fetchdf()
        con.close()
        rows.append(df)
    except: pass
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

@st.cache_data(ttl=3600)
def get_timeseries(name, cycle, db_type):
    if db_type == "single":
        con = duckdb.connect(DB_SINGLE, read_only=True)
        df = con.execute("SELECT time, value FROM timeseries WHERE name=? AND cycle=? ORDER BY time",
                        [name, cycle]).fetchdf()
    else:
        con = duckdb.connect(DB_MULTI, read_only=True)
        df = con.execute("SELECT time, value FROM timeseries_multi WHERE name=? AND cycle=? ORDER BY time",
                        [name, cycle]).fetchdf()
    con.close()
    return df

try:
    all_indicators = get_all_indicators()
    sectors = all_indicators["sector"].unique().tolist()
except:
    st.error("DB 파일이 없습니다. fetch_single.py와 fetch_multi.py를 먼저 실행하세요.")
    st.stop()

selected_sector = st.sidebar.radio("섹터 선택", sectors)
sector_df = all_indicators[all_indicators["sector"] == selected_sector]
indicator_names = sector_df["name"].tolist()

st.subheader(f"📁 {selected_sector}")
cols = st.columns(4)
if "selected_indicator" not in st.session_state or st.session_state.selected_indicator not in indicator_names:
    st.session_state.selected_indicator = indicator_names[0]

for i, name in enumerate(indicator_names):
    with cols[i % 4]:
        if st.button(name, key=f"btn_{selected_sector}_{name}", use_container_width=True):
            st.session_state.selected_indicator = name

selected_indicator = st.session_state.selected_indicator
st.markdown("---")

row = sector_df[sector_df["name"] == selected_indicator].iloc[0]
cycle = row["cycle"]
db_type = row["db_type"]

df = get_timeseries(selected_indicator, cycle, db_type)

def parse_time(t, cycle):
    try:
        if cycle == "D": return pd.to_datetime(str(t), format="%Y%m%d")
        elif cycle == "M": return pd.to_datetime(str(t), format="%Y%m")
        elif cycle == "Q": return pd.to_datetime(str(t).replace("Q1","-01-01").replace("Q2","-04-01").replace("Q3","-07-01").replace("Q4","-10-01"))
        else: return pd.to_datetime(str(t), format="%Y")
    except: return pd.NaT

df["date"] = df["time"].apply(lambda t: parse_time(t, cycle))
df = df.dropna(subset=["date"]).sort_values("date")

if df.empty:
    st.warning(f"{selected_indicator} 데이터가 없습니다.")
    st.stop()

latest = df["value"].iloc[-1]
prev = df["value"].iloc[-2] if len(df) > 1 else latest
delta = latest - prev

c1, c2, c3 = st.columns(3)
c1.metric("최신값", f"{latest:,.2f}", f"{delta:+.2f}")
c2.metric("최대값", f"{df['value'].max():,.2f}")
c3.metric("최소값", f"{df['value'].min():,.2f}")

period = st.select_slider("기간", options=["1년","3년","5년","10년","전체"], value="5년")
years_map = {"1년":1,"3년":3,"5년":5,"10년":10,"전체":None}
years = years_map[period]
dff = df.copy() if years is None else df[df["date"] >= df["date"].max() - pd.DateOffset(years=years)]

fig = go.Figure()
fig.add_trace(go.Scatter(x=dff["date"], y=dff["value"], mode="lines",
    name=selected_indicator, line=dict(color="#01696f", width=2),
    fill="tozeroy", fillcolor="rgba(1,105,111,0.08)"))
fig.update_layout(
    title=dict(text=selected_indicator, font=dict(size=18)),
    xaxis_title="날짜", yaxis_title="값", hovermode="x unified",
    plot_bgcolor="white", paper_bgcolor="white", height=450,
    margin=dict(l=40,r=20,t=60,b=40),
    xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
    yaxis=dict(showgrid=True, gridcolor="#f0f0f0"))
st.plotly_chart(fig, use_container_width=True)

with st.expander("원본 데이터 보기"):
    st.dataframe(dff[["date","value"]].rename(columns={"date":"날짜","value":"값"})
                .sort_values("날짜", ascending=False), use_container_width=True)
