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

def fetch(api_key, stat_code, i1, i2, cycle):
    start, end = get_cycle_dates(cycle)

    item_part = i1 if i1 else "?"
    if i2:
        item_part += f"/{i2}"

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

        df = pd.DataFrame(data["StatisticSearch"]["row"])
        df["value"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")
        df = df[["TIME", "value"]].dropna(subset=["value"])

        # TIME당 첫 번째 값만 유지
        df = df.drop_duplicates(subset=["TIME"], keep="first")

        return df
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
    if df.empty:
        return 0
    df = df.copy()
    df["sector"] = sector
    df["name"] = name
    df["stat_code"] = stat_code
    df["item_code"] = item_code
    df["cycle"] = cycle
    df["updated_at"] = date.today().isoformat()
    df = df.rename(columns={"TIME": "time"})
    df = df[["sector", "name", "stat_code", "item_code", "cycle", "time", "value", "updated_at"]]

    # ✅ placeholder 방식으로 DELETE (df 참조 오류 방지)
    times = df["time"].tolist()
    placeholders = ", ".join(["?" for _ in times])
    con.execute(f"""
        DELETE FROM timeseries
        WHERE stat_code=? AND item_code=? AND name=?
        AND time IN ({placeholders})
    """, [stat_code, item_code, name] + times)

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

            i1 = ind.get("item_code1", ind.get("item_code", ""))
            i2 = ind.get("item_code2", "")
            item_code_str = "/".join(filter(None, [i1, i2]))

            df = fetch(API_KEY, ind["stat_code"], i1, i2, ind["cycle"])
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