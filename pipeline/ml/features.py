import os
import re
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

CENTER_LAT, CENTER_LON = 55.7520, 37.6175
REGION_CENTERS = {
    "Москва": (55.7520, 37.6175),
    "Московская область": (55.7520, 37.6175),
    "Санкт-Петербург": (59.9386, 30.3141),
    "Ленинградская область": (59.9386, 30.3141),
}
GEOJSON_PATH = os.path.join(os.path.dirname(__file__), "moscow_raions.geojson")
HOT_DAYS = 14

NUMERIC = [
    "mortgage_allowed", "n_metro", "nearest_metro_time", "nearest_metro_walk", "rooms",
    "is_studio", "total_area", "living_area", "kitchen_area", "floor", "total_floors",
    "ceiling_height", "is_apartments", "is_new_building", "phone_protected", "price_first",
    "lat", "lon", "dist_to_center", "price_per_m2", "building_age", "is_ready",
    "is_first_floor", "is_last_floor", "floor_ratio", "living_to_total", "kitchen_to_total",
    "area_per_room", "ppm2_to_district", "ppm2_to_municipality", "total_lifts", "has_lift",
    "bath_separate", "bath_combined", "balcony_count", "loggia_count", "completion_year",
    "years_to_completion", "is_presale", "has_completion", "demolished_in_renovation",
    "is_penthouse", "seller_is_owner",
]
CATEGORICAL = [
    "region", "flat_type", "renovation", "window_view", "building_type", "parking",
    "seller_type", "seller_user_type", "room_type", "deal_conditions", "municipality", "district",
]

INPUT_COLUMNS = [
    "cian_id", "days_on_market", "price", "price_first", "total_area", "living_area",
    "kitchen_area", "rooms", "is_studio", "floor", "total_floors", "ceiling_height",
    "lat", "lon", "n_metro", "nearest_metro", "nearest_metro_time", "nearest_metro_walk",
    "mortgage_allowed", "is_apartments", "is_new_building", "phone_protected",
    "completion_date", "passenger_lifts", "cargo_lifts", "bathrooms", "balcony",
    "year_built", "is_penthouse", "seller_is_owner", "demolished_in_renovation",
] + CATEGORICAL

BOOL_COLS = ["is_apartments", "is_new_building", "phone_protected", "is_studio",
             "mortgage_allowed", "nearest_metro_walk", "demolished_in_renovation",
             "is_penthouse", "seller_is_owner"]
BOOL_MAP = {"t": 1, "f": 0, "True": 1, "False": 0, True: 1, False: 0, 1: 1, 0: 0}
CAT_UNKNOWN = ["municipality", "region", "district", "parking", "window_view", "renovation",
               "building_type", "flat_type", "seller_user_type", "room_type"]
CAT_MODE = ["deal_conditions", "seller_type"]

_RAIONS = None


def _load_raions():
    global _RAIONS
    if _RAIONS is None:
        r = gpd.read_file(GEOJSON_PATH)[["name", "geometry"]].set_crs(4326, allow_override=True)
        r["name"] = (r["name"].str.replace(r"^(р-н|район)\s+", "", regex=True)
                     .str.replace(r"\s+район$", "", regex=True).str.strip())
        _RAIONS = r
    return _RAIONS


def _count_token(value, token):
    if pd.isna(value):
        return -1
    m = re.search(r"(\d+)\s*" + token, str(value))
    return int(m.group(1)) if m else 0


def _num(s):
    return pd.to_numeric(s, errors="coerce")


def make_target(df):
    return (_num(df["days_on_market"]) < HOT_DAYS).astype(int)


class FeatureBuilder(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        self.impute_ = {}
        self.ratio_ = {}
        self._build(X, fitting=True)
        return self

    def transform(self, X):
        return self._build(X, fitting=False)

    @staticmethod
    def _dist_to_center(f):
        r = 6371.0
        lat = np.radians(_num(f["lat"]))
        lon = np.radians(_num(f["lon"]))
        center_lat = np.radians(f["region"].map({k: v[0] for k, v in REGION_CENTERS.items()}).fillna(CENTER_LAT))
        center_lon = np.radians(f["region"].map({k: v[1] for k, v in REGION_CENTERS.items()}).fillna(CENTER_LON))
        dlat = lat - center_lat
        dlon = lon - center_lon
        a = np.sin(dlat / 2) ** 2 + np.cos(lat) * np.cos(center_lat) * np.sin(dlon / 2) ** 2
        return 2 * r * np.arcsin(np.sqrt(a))

    @staticmethod
    def _restore_district(f):
        raions = _load_raions()
        pts = gpd.GeoDataFrame(
            f[["district"]].copy(),
            geometry=gpd.points_from_xy(_num(f["lon"]), _num(f["lat"])),
            crs=4326,
        )
        joined = gpd.sjoin(pts, raions, how="left", predicate="within")
        joined = joined[~joined.index.duplicated(keep="first")].reindex(f.index)
        return f["district"].where(f["district"].notna(), joined["name"])

    @staticmethod
    def _lookup(lk, idx_frame, keys):
        lkdf = lk.rename("_m").reset_index()
        return idx_frame.merge(lkdf, on=keys, how="left")["_m"].to_numpy()

    def _impute(self, f, col, key_groups, fitting, name):
        s = f[col].copy()
        if fitting:
            self.impute_[name] = {"global": float(np.nanmedian(_num(s))), "tables": []}
            for keys in key_groups:
                lk = f.assign(_t=s).groupby(keys)["_t"].median()
                self.impute_[name]["tables"].append((keys, lk))
        state = self.impute_[name]
        for keys, lk in state["tables"]:
            mask = s.isna()
            if mask.any():
                s.loc[mask] = self._lookup(lk, f.loc[mask, keys], keys)
        return s.fillna(state["global"])

    def _ratio(self, f, geo, fitting, name):
        ppm = f["price_per_m2"]
        if fitting:
            lk = f.assign(_p=ppm).groupby([geo, "rooms"])["_p"].median()
            self.ratio_[name] = {"keys": [geo, "rooms"], "lk": lk, "global": float(np.nanmedian(ppm))}
        st = self.ratio_[name]
        med = pd.Series(self._lookup(st["lk"], f[[geo, "rooms"]], st["keys"]),
                        index=f.index).fillna(st["global"])
        return ppm / med.replace(0, np.nan)

    def _build(self, X, fitting):
        f = X.copy()
        for c in BOOL_COLS:
            f[c] = f[c].map(BOOL_MAP)
        for c in CAT_UNKNOWN + CAT_MODE:
            s = f[c].astype("object")
            blank = s.map(lambda v: isinstance(v, str) and v.strip().lower() in ("", "nan", "none"))
            f.loc[blank.fillna(False), c] = np.nan

        f["district"] = self._restore_district(f)
        f["dist_to_center"] = self._dist_to_center(f)
        area = _num(f["total_area"]).replace(0, np.nan)
        f["price_per_m2"] = _num(f["price_first"]) / area

        ch = _num(f["ceiling_height"])
        f["ceiling_height"] = ch.where((ch >= 2) & (ch <= 8))
        bad = (_num(f["living_area"]).fillna(0) + _num(f["kitchen_area"]).fillna(0)) > _num(f["total_area"])
        f.loc[bad, ["living_area", "kitchen_area"]] = np.nan

        for c in CAT_UNKNOWN:
            f[c] = f[c].astype("object").where(f[c].notna(), "unknown")
        if fitting:
            self.mode_ = {c: (f[c].dropna().mode().iloc[0] if f[c].notna().any() else "unknown")
                          for c in CAT_MODE}
        for c in CAT_MODE:
            f[c] = f[c].fillna(self.mode_[c])

        f["rooms"] = _num(f["rooms"])
        f.loc[f["is_studio"] == 1, "rooms"] = 0
        if fitting:
            self.rooms_median_ = float(np.nanmedian(f["rooms"]))
        f["rooms"] = f["rooms"].fillna(self.rooms_median_)

        f["year_built"] = _num(f["year_built"])
        f["total_floors"] = _num(f["total_floors"])
        f["ceiling_height"] = self._impute(
            f, "ceiling_height", [["building_type", "year_built", "total_floors"]], fitting, "ch")

        f["area_bin"] = (_num(f["total_area"]) // 5) * 5
        f["kitchen_area"] = _num(f["kitchen_area"])
        f["living_area"] = _num(f["living_area"])
        f["kitchen_area"] = self._impute(f, "kitchen_area", [["building_type", "area_bin"]], fitting, "kit")
        f["living_area"] = self._impute(f, "living_area", [["building_type", "area_bin"]], fitting, "liv")

        f.loc[f["year_built"] < 1500, "year_built"] = np.nan
        f["_district_yb"] = f["district"].where(f["district"] != "unknown")
        f["year_built"] = self._impute(
            f, "year_built", [["building_type"], ["_district_yb"], ["municipality"]], fitting, "yb")
        f = f.drop(columns=["_district_yb", "area_bin"])

        if fitting:
            self.reference_year_ = datetime.now(timezone.utc).year
        ry = self.reference_year_
        f["building_age"] = ry - f["year_built"]
        comp = f["completion_date"].astype("object").str.extract(r"(\d{4})")[0].astype("float")
        f["completion_year"] = comp
        f["years_to_completion"] = comp - ry
        f["is_presale"] = (comp > ry).astype("int8")
        f["has_completion"] = comp.notna().astype("int8")
        f["is_ready"] = (~((f["is_new_building"] == 1) & (comp > ry))).astype("int8")

        fl, tf = _num(f["floor"]), f["total_floors"]
        f["is_first_floor"] = (fl == 1).astype("int8")
        f["is_last_floor"] = (fl == tf).astype("int8")
        f["floor_ratio"] = (fl / tf).replace([np.inf, -np.inf], np.nan)
        f["living_to_total"] = f["living_area"] / _num(f["total_area"])
        f["kitchen_to_total"] = f["kitchen_area"] / _num(f["total_area"])
        f["area_per_room"] = _num(f["total_area"]) / f["rooms"].replace(0, 1)
        f["total_lifts"] = _num(f["passenger_lifts"]).fillna(0) + _num(f["cargo_lifts"]).fillna(0)
        f["has_lift"] = (f["total_lifts"] > 0).astype("int8")
        f["bath_separate"] = f["bathrooms"].map(lambda v: _count_token(v, "разд"))
        f["bath_combined"] = f["bathrooms"].map(lambda v: _count_token(v, "совм"))
        f["balcony_count"] = f["balcony"].map(lambda v: _count_token(v, "балк"))
        f["loggia_count"] = f["balcony"].map(lambda v: _count_token(v, "лодж"))

        f["nearest_metro_time"] = _num(f["nearest_metro_time"])
        if fitting:
            self.metro_time_median_ = float(np.nanmedian(f["nearest_metro_time"]))
        f["nearest_metro_time"] = f["nearest_metro_time"].fillna(self.metro_time_median_)

        f["ppm2_to_district"] = self._ratio(f, "district", fitting, "ppm_d")
        f["ppm2_to_municipality"] = self._ratio(f, "municipality", fitting, "ppm_m")

        for c in NUMERIC:
            f[c] = _num(f[c])
        return f[NUMERIC + CATEGORICAL]
