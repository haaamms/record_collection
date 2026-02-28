import os, time, json, re, discogs_client, duckdb
from discogs_client.exceptions import HTTPError
import pandas as pd
from dotenv import load_dotenv


# ---------- CONFIG ----------
load_dotenv()

USER_TOKEN = os.getenv("DISCOGS_TOKEN")
USER_AGENT = os.getenv("USER_AGENT")
USERNAME = os.getenv("USERNAME")

# ----- SET SLEEP TIME BETWEEN CALLS ----------
SLEEP_BETWEEN_CALLS = 0.25

# Fetch full release to get exact release date & identifiers (set False for speed)
INCLUDE_FULL_RELEASE = True

def clean_text(s):
    if s is None:
        return ""
    s = str(s).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", s).strip()


def join_names(objs):
    """list[{name:..}] -> 'Name1|Name2'"""
    if not isinstance(objs, list):
        return ""
    return "|".join([o.get("name","") for o in objs if isinstance(o, dict) and o.get("name")])


def summarize_formats(fmt_list):
    """
    Build a compact human summary: 'Vinyl LP Album (Red) x1|Vinyl 7" Single x1'
    and return also a good 'variant' like color or edition text.
    """
    if not isinstance(fmt_list, list):
        return "", ""

    parts = []
    variant_bits = []
    for fmt in fmt_list:
        if not isinstance(fmt, dict):
            continue
        name = (fmt.get("name") or "").strip()          # Vinyl, CD, File...
        qty  = (fmt.get("qty") or "").strip()           # "1", "2"
        text = (fmt.get("text") or "").strip()          # e.g. "Red", "Limited Edition"
        descs = fmt.get("descriptions", []) or []       # ["Album","LP","Reissue"]
        # variant: prefer color/edition info
        if text:
            variant_bits.append(text)
        # crude color/edition sniff from descriptions
        for d in descs:
            d = (d or "").strip()
            if d and any(k in d.lower() for k in ["colored", "red", "blue", "green", "marbled", "splatter", "limited", "reissue", "remastered", "club"]):
                variant_bits.append(d)

        piece = " ".join([name] + [d for d in descs if d]).strip()
        if text:
            piece = f"{piece} ({text})".strip()
        if qty:
            piece = f"{piece} x{qty}".strip()
        if piece:
            parts.append(piece)

    # Variant = unique bits joined
    seen = set()
    variant = "|".join([b for b in variant_bits if not (b in seen or seen.add(b))]) or ""
    return "|".join(parts), clean_text(variant)


def first_barcode(identifiers):
    """pluck the first Barcode value if present"""
    if not isinstance(identifiers, list):
        return ""
    for obj in identifiers:
        if isinstance(obj, dict) and obj.get("type","").lower() == "barcode":
            return clean_text(obj.get("value",""))
    return ""


def pick(key, full, basic, default=None):
    """Prefer full release JSON, fallback to collection basic_information"""
    return (full or {}).get(key) or (basic or {}).get(key, default)


def fetch_rows():
    d = discogs_client.Client(USER_AGENT, user_token=USER_TOKEN)
    user = d.user(USERNAME)
    folder = user.collection_folders[0]  # "All"
    total  = folder.releases.count
    print(f"User {user.username} - items: {total}")

    rows = []
    i = 0
    for item in folder.releases:
        i += 1
        time.sleep(SLEEP_BETWEEN_CALLS)

        ci = getattr(item, "data", {}) or {}
        basic = ci.get("basic_information", {}) or {}

        rel_id = getattr(item.release, "id", None) or basic.get("id")
        if not rel_id:
            continue

        full = {}
        if INCLUDE_FULL_RELEASE:
            try:
                rel = d.release(rel_id)
                full = rel.data or {}
            except HTTPError as e:
                full = {"_fetch_error": str(e)}

        # ---- assemble output fields ----
        artists = pick("artists", full, basic, [])
        labels  = pick("labels",  full, basic, [])
        formats = pick("formats", full, basic, [])
        genres  = pick("genres",  full, basic, [])
        styles  = pick("styles",  full, basic, [])
        identifiers = (full or {}).get("identifiers", [])

        fmt_summary, variant = summarize_formats(formats)

        row = {
            "release_id":      rel_id,
            "master_id":       pick("master_id", full, basic),
            "artist":          clean_text(join_names(artists)),
            "title":           clean_text(pick("title", full, basic, "")),
            "date_added":      clean_text(ci.get("date_added")),
            "variant":         variant,                 # e.g. 'Red|Limited Edition'
            "format":          fmt_summary,             # compact format summary
            "release_date":    clean_text(pick("released", full, basic, basic.get("released_formatted") or basic.get("year"))),
            "country":         clean_text(pick("country", full, basic, "")),
            "label":           clean_text(join_names(labels)),
            "catno":           "|".join([l.get("catno","") for l in (labels or []) if isinstance(l, dict) and l.get("catno")]),
            "genres":          "|".join(genres or []),
            "styles":          "|".join(styles or []),
#            "barcode":         first_barcode(identifiers),
#            "resource_url":    clean_text(pick("resource_url", full, basic, "")),
        }

        rows.append(row)

        if i % 25 == 0:
            print(f"Fetched {i}/{total}…")

    return rows

def main():
    rows = fetch_rows()
    df = pd.DataFrame(rows)
    con = duckdb.connect()
    con.execute("CREATE TABLE collection AS SELECT * FROM df")
    
    #   checks
    print(con.execute("SELECT COUNT(*) FROM collection").fetchone())
    print(con.execute("SELECT * FROM collection LIMIT 5").df())
    

    print(f"fetched {len(rows)} rows")


if __name__ == "__main__":
    main()
