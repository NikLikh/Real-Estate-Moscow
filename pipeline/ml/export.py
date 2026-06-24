import os

import pandas as pd
import xlsxwriter

CIAN_LINK = '=HYPERLINK("https://www.cian.ru/sale/flat/{0}/","{0}")'
HOT_THRESHOLD = 0.5
EXPORT_CHUNK = 20000
EXPORT_QUERY = """
select w.*, h.hot_score
from marts.hot_listings h
join marts.ml_listings_wide w using (cian_id)
"""


def _is_bool_col(s):
    if s.dtype == bool:
        return True
    if s.dtype == object:
        non_null = s.dropna()
        return len(non_null) > 0 and non_null.map(lambda v: isinstance(v, bool)).all()
    return False


def _prepare_chunk(df):
    df = df.drop(columns=["first_seen", "last_seen", "event_closed"])
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)
    df["is_hot"] = (df["hot_score"] >= HOT_THRESHOLD).astype(int)
    for col in df.columns:
        if _is_bool_col(df[col]):
            df[col] = df[col].map({True: 1, False: 0})
    df["cian_id"] = df["cian_id"].map(CIAN_LINK.format)
    return df


def build_export(engine, path):
    tmp = path + ".tmp"
    book = xlsxwriter.Workbook(tmp, {"constant_memory": True, "default_date_format": "yyyy-mm-dd"})
    sheet = book.add_worksheet()
    row = 0
    with engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(EXPORT_QUERY, conn, chunksize=EXPORT_CHUNK):
            chunk = _prepare_chunk(chunk)
            if row == 0:
                sheet.write_row(0, 0, list(chunk.columns))
                row = 1
            chunk = chunk.astype(object).where(pd.notna(chunk), None)
            for record in chunk.itertuples(index=False, name=None):
                sheet.write_row(row, 0, record)
                row += 1
    book.close()
    os.replace(tmp, path)
    return path
