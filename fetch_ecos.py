import os
import json
import requests
import pandas as pd
import duckdb
from datetime import date, datetime

API_KEY = os.environ.get("ECOS_API_KEY", "G2QX7DH1PVZJ8XZKYJ3L")
DB_PATH = "ecos_warehouse.db"

def get_cycle_dates(cycle):
    today = datetime.today()
    if cycle == "D":
        return "20150101", today.strftime("%Y%m%d")
    elif cycle == "M":
        return "201501", today.strftime("%Y%m")
    elif cycle == "Q":
        q = (today.month - 1) // 3 + 1
        return "2015Q1", f"{today.year}Q{q}"
    else:  # A
        return "2015", str(today.year)

def fetch(api_key, stat_code, i1, i2, i3, cycle):
    start, end = get_cycle_dates(cycle)

    # 있는 코드만 붙이기
    item_part = i1 if i1 else "?"
    if i2:
        item_part += f"/{i2}"
    if i3:
        item_part += f"/{i3}"

    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch/"
        f"{api_key}/json/kr/1/10000/"
        f"{stat_code}/{cycle}/{start}/{end}/{item_part}"
    )
    try:
        res = requests.get(url, timeout=30)
        data = res.json()
        if "StatisticSearch" not in data:
            msg = data.get("RESULT", {}).get("MESSAGE", str(data))
            print(f"  ⚠️  {stat_code}/{i1}/{i2} → {msg}")
            return pd.DataFrame()
        df = pd.DataFrame(data["StatisticSearch"]["row"])[["TIME","DATA_VALUE"]]
        df["value"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")
        return df[["TIME","value"]].dropna(subset=["value"])
    except Exception as e:
        print(f"  ❌ {stat_code}/{i1} → {e}")
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
    df = df.rename(columns={"TIME":"time"})
    df = df[["sector","name","stat_code","item_code","cycle","time","value","updated_at"]]
    con.execute(
        "DELETE FROM timeseries WHERE stat_code=? AND item_code=? AND time IN (SELECT time FROM df)",
        [stat_code, item_code]
    )
    con.execute("INSERT INTO timeseries SELECT * FROM df")
    return len(df)

def main():
    with open("indicators_config.json", encoding="utf-8") as f:
        config = json.load(f)
    con = duckdb.connect(DB_PATH)
    init_db(con)
    total, failed = 0, []

    for sec in config["sectors"]:
        print(f"\n📂 [{sec['sector']}]")
        for ind in sec["indicators"]:
            print(f"  → {ind['name']} ...", end=" ", flush=True)

            # item_code1/2/3 우선, 없으면 item_code fallback
            i1 = ind.get("item_code1", ind.get("item_code", ""))
            i2 = ind.get("item_code2", "")
            i3 = ind.get("item_code3", "")

            # DB 저장용 item_code 문자열 (i1/i2/i3 조합)
            item_code_str = "/".join(filter(None, [i1, i2, i3]))

            df = fetch(API_KEY, ind["stat_code"], i1, i2, i3, ind["cycle"])
            cnt = upsert(con, sec["sector"], ind["name"], ind["stat_code"], item_code_str, ind["cycle"], df)
            if cnt > 0:
                print(f"{cnt}건 저장")
            else:
                failed.append(f"{sec['sector']} / {ind['name']}")
            total += cnt

    con.close()
    print(f"\n✅ 완료! 총 {total}건 저장 ({date.today()})")
    if failed:
        print(f"\n⚠️  0건 지표 ({len(failed)}개):")
        for f in failed:
            print(f"   - {f}")

if __name__ == "__main__":
    main()