import os, json, requests, pandas as pd, duckdb
from datetime import date, datetime

API_KEY = os.environ.get("ECOS_API_KEY", "G2QX7DH1PVZJ8XZKYJ3L")
DB_PATH = "ecos_single.db"

def get_cycle_dates(cycle):
    today = datetime.today()
    if cycle == "D": return "20150101", today.strftime("%Y%m%d")
    elif cycle == "M": return "201501", today.strftime("%Y%m")
    elif cycle == "Q":
        q = (today.month - 1) // 3 + 1
        return "2015Q1", f"{today.year}Q{q}"
    else: return "2015", str(today.year)

def fetch(api_key, stat_code, item_code, cycle):
    start, end = get_cycle_dates(cycle)
    url = (f"https://ecos.bok.or.kr/api/StatisticSearch/"
           f"{api_key}/json/kr/1/10000/"
           f"{stat_code}/{cycle}/{start}/{end}/{item_code}")
    try:
        data = requests.get(url, timeout=30).json()
        if "StatisticSearch" not in data:
            print(f"  ⚠️  {data.get('RESULT',{}).get('MESSAGE', str(data))}")
            return pd.DataFrame()
        df = pd.DataFrame(data["StatisticSearch"]["row"])
        df["value"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")
        df = df[["TIME","value"]].dropna(subset=["value"])
        df = df.drop_duplicates(subset=["TIME"], keep="first")
        return df
    except Exception as e:
        print(f"  ❌ {e}")
        return pd.DataFrame()

def init_db(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS timeseries (
            sector VARCHAR, name VARCHAR, stat_code VARCHAR,
            item_code VARCHAR, cycle VARCHAR,
            time VARCHAR, value DOUBLE, updated_at DATE
        )
    """)

def upsert(con, sector, name, stat_code, item_code, cycle, df):
    if df.empty: return 0
    df = df.copy()
    df["sector"] = sector; df["name"] = name
    df["stat_code"] = stat_code; df["item_code"] = item_code
    df["cycle"] = cycle; df["updated_at"] = date.today().isoformat()
    df = df.rename(columns={"TIME": "time"})
    df = df[["sector","name","stat_code","item_code","cycle","time","value","updated_at"]]
    con.execute("DELETE FROM timeseries WHERE stat_code=? AND item_code=? AND name=? AND cycle=?",
                [stat_code, item_code, name, cycle])
    con.execute("INSERT INTO timeseries SELECT * FROM df")
    return len(df)

def main():
    with open("indicators_config_single.json", encoding="utf-8") as f:
        config = json.load(f)
    con = duckdb.connect(DB_PATH)
    init_db(con)
    total, failed = 0, []
    for sec in config["sectors"]:
        print(f"\n📂 [{sec['sector']}]")
        for ind in sec["indicators"]:
            print(f"  → {ind['name']} ...", end=" ", flush=True)
            df = fetch(API_KEY, ind["stat_code"], ind["item_code"], ind["cycle"])
            cnt = upsert(con, sec["sector"], ind["name"], ind["stat_code"],
                        ind["item_code"], ind["cycle"], df)
            print(f"{cnt}건 저장" if cnt > 0 else "0건")
            if cnt == 0: failed.append(f"{sec['sector']} / {ind['name']}")
            total += cnt
    con.close()
    print(f"\n✅ 완료! 총 {total}건 ({date.today()})")
    if failed:
        print(f"\n⚠️  0건 지표 {len(failed)}개:")
        for f in failed: print(f"   - {f}")

if __name__ == "__main__":
    main()