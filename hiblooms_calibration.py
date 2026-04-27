from __future__ import annotations

import io
import json
from typing import Any, Callable

import ee
import joblib
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
from sklearn.model_selection import train_test_split, KFold, TimeSeriesSplit, cross_validate, GridSearchCV
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error


CANDIDATE_INDICES = [
    "NDCI_705_665",
    "MCI_705",
    "R705_R665",
    "R740_R665",
    "R783_R665",
    "TB_740",
    "TB_783",
    "NDRE_783_705",
    "NDRE_740_705",
    "B5_B4_diff",
    "B6_B5_diff",
    "B7_B5_diff",
]


def _rmse(y_true, y_pred):
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def _mae(y_true, y_pred):
    return float(mean_absolute_error(y_true, y_pred))


def _r2(y_true, y_pred):
    return float(r2_score(y_true, y_pred))


def prepare_insitu(df: pd.DataFrame, target_variable: str, start_hour: int, end_hour: int) -> tuple[pd.DataFrame, list[str]]:
    required_cols = ["date", "time", target_variable]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Available columns: {list(df.columns)}")

    out = df.copy()
    out["datetime"] = pd.to_datetime(
        out["date"].astype(str) + " " + out["time"].astype(str),
        errors="coerce",
        utc=True,
    )
    out["value"] = pd.to_numeric(out[target_variable], errors="coerce")
    out = out.dropna(subset=["datetime", "value"]).copy()
    if out.empty:
        raise ValueError("No valid in situ rows remain after parsing date/time and target values.")

    out["hour"] = out["datetime"].dt.hour
    out = out[(out["hour"] >= int(start_hour)) & (out["hour"] <= int(end_hour))].copy()
    if out.empty:
        raise ValueError("No in situ rows remain after the hour filter.")

    out["date"] = out["datetime"].dt.strftime("%Y-%m-%d")
    out["time"] = out["datetime"].dt.strftime("%H:%M:%S")
    out["variable"] = target_variable
    out = out[["date", "datetime", "time", "variable", "value"]].reset_index(drop=True)

    return out, sorted(out["date"].unique().tolist())


def _init_ee():
    try:
        ee.Initialize()
    except Exception:
        ee.Authenticate()
        ee.Initialize()


def _coverage_percentage(image: ee.Image, aoi: ee.Geometry, scale_m: int) -> float:
    total_area = ee.Image.constant(1).clip(aoi).multiply(ee.Image.pixelArea()).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=aoi,
        scale=scale_m,
        maxPixels=1e13,
    ).get("constant")

    valid_area = image.select("B4").mask().multiply(ee.Image.pixelArea()).reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=aoi,
        scale=scale_m,
        maxPixels=1e13,
    ).get("B4")

    try:
        total = float(total_area.getInfo() or 0)
        valid = float(valid_area.getInfo() or 0)
    except Exception:
        total, valid = 0.0, 0.0

    return 0.0 if total <= 0 else (100.0 * valid / total)


def compute_satellite_features(
    aoi: ee.Geometry,
    start_date: str,
    end_date: str,
    max_cloud_percentage: float,
    min_coverage_percentage: float,
    scale_m: int,
    candidate_indices: list[str],
    priority_dates: list[str] | None = None,
    progress_bar=None,
    progress_text=None,
) -> pd.DataFrame:
    _init_ee()

    expected_cols = [
        "date",
        "datetime_utc",
        "overpass_hour_utc",
        "image_id",
        "cloudy_pixel_percentage",
        "coverage_percentage_aoi",
        "valid_by_cloud",
        "valid_by_coverage",
        "valid_final",
        *candidate_indices,
    ]

    def add_indices(img):
        b4 = img.select("B4").toFloat().divide(10000)
        b5 = img.select("B5").toFloat().divide(10000)
        b6 = img.select("B6").toFloat().divide(10000)
        b7 = img.select("B7").toFloat().divide(10000)

        out = img.addBands(b4.rename("B4s"), overwrite=True)
        out = out.addBands(b5.rename("B5s"), overwrite=True)
        out = out.addBands(b6.rename("B6s"), overwrite=True)
        out = out.addBands(b7.rename("B7s"), overwrite=True)
        out = out.addBands(b5.subtract(b4).divide(b5.add(b4)).rename("NDCI_705_665"))
        out = out.addBands(
            b5.subtract(
                b4.add(
                    b6.subtract(b4).multiply((705 - 665) / (740 - 665))
                )
            ).rename("MCI_705")
        )
        out = out.addBands(b5.divide(b4).rename("R705_R665"))
        out = out.addBands(b6.divide(b4).rename("R740_R665"))
        out = out.addBands(b7.divide(b4).rename("R783_R665"))
        out = out.addBands(b6.subtract(b5).rename("TB_740"))
        out = out.addBands(b7.subtract(b5).rename("TB_783"))
        out = out.addBands(b7.subtract(b5).divide(b7.add(b5)).rename("NDRE_783_705"))
        out = out.addBands(b6.subtract(b5).divide(b6.add(b5)).rename("NDRE_740_705"))
        out = out.addBands(b5.subtract(b4).rename("B5_B4_diff"))
        out = out.addBands(b6.subtract(b5).rename("B6_B5_diff"))
        out = out.addBands(b7.subtract(b5).rename("B7_B5_diff"))
        return out

    rows: list[dict[str, Any]] = []

    dates_to_process = sorted(set(priority_dates or []))
    if not dates_to_process:
        if progress_bar is not None:
            progress_bar.progress(1.0)
        if progress_text is not None:
            progress_text.warning("No priority dates were provided from the uploaded CSV.")
        return pd.DataFrame(columns=expected_cols)

    n_dates = len(dates_to_process)

    for i, day_str in enumerate(dates_to_process):
        if progress_bar is not None:
            progress_bar.progress((i + 1) / n_dates)

        if progress_text is not None:
            progress_text.info(
                f"Processing in situ dates: {i + 1}/{n_dates} | current date: {day_str}"
            )

        day_start = ee.Date(day_str)
        day_end = day_start.advance(1, "day")

        day_collection = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterBounds(aoi)
            .filterDate(day_start, day_end)
            .filter(ee.Filter.lte("CLOUDY_PIXEL_PERCENTAGE", float(max_cloud_percentage)))
            .sort("system:time_start")
            .map(add_indices)
        )

        n_day_images = int(day_collection.size().getInfo())
        if n_day_images == 0:
            continue

        img_list = day_collection.toList(n_day_images)
        best_row = None

        for j in range(n_day_images):
            img = ee.Image(img_list.get(j))

            image_id = img.id().getInfo()
            cloud_pct = float(img.get("CLOUDY_PIXEL_PERCENTAGE").getInfo() or 0.0)
            coverage_pct = _coverage_percentage(img, aoi, scale_m)

            valid_by_cloud = cloud_pct <= max_cloud_percentage
            valid_by_coverage = coverage_pct >= min_coverage_percentage
            valid_final = bool(valid_by_cloud and valid_by_coverage)

            row = {
                "date": day_str,
                "datetime_utc": ee.Date(img.get("system:time_start")).format("YYYY-MM-dd HH:mm:ss").getInfo(),
                "overpass_hour_utc": float(ee.Date(img.get("system:time_start")).get("hour").getInfo()),
                "image_id": image_id,
                "cloudy_pixel_percentage": cloud_pct,
                "coverage_percentage_aoi": coverage_pct,
                "valid_by_cloud": valid_by_cloud,
                "valid_by_coverage": valid_by_coverage,
                "valid_final": valid_final,
            }

            if valid_final:
                reducer_dict = img.select(candidate_indices).reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=aoi,
                    scale=scale_m,
                    maxPixels=1e13,
                ).getInfo() or {}

                for idx in candidate_indices:
                    row[idx] = reducer_dict.get(idx, np.nan)
            else:
                for idx in candidate_indices:
                    row[idx] = np.nan

            if best_row is None:
                best_row = row
            else:
                current_key = (
                    row["valid_final"],
                    -row["cloudy_pixel_percentage"],
                    row["coverage_percentage_aoi"],
                )
                best_key = (
                    best_row["valid_final"],
                    -best_row["cloudy_pixel_percentage"],
                    best_row["coverage_percentage_aoi"],
                )
                if current_key > best_key:
                    best_row = row

        if best_row is not None:
            rows.append(best_row)

    if not rows:
        if progress_text is not None:
            progress_text.warning("No Sentinel-2 rows matched the uploaded in situ dates.")
        return pd.DataFrame(columns=expected_cols)

    df = pd.DataFrame(rows, columns=expected_cols)
    df = df.sort_values("date").reset_index(drop=True)

    if progress_text is not None:
        n_valid = int(df["valid_final"].sum()) if "valid_final" in df.columns else 0
        progress_text.success(
            f"Satellite extraction completed. Dates processed: {len(df)} | valid_final: {n_valid}"
        )

    return df


def match_insitu_to_overpass(
    insitu_df: pd.DataFrame,
    sat_df: pd.DataFrame,
    overpass_window_hours: float = 1.5,
) -> pd.DataFrame:
    expected_out = [
        "date",
        "variable",
        "value_mean",
        "value_median",
        "value_min",
        "value_max",
        "n_obs",
        "overpass_hour_utc",
    ]

    if sat_df is None or sat_df.empty or "valid_final" not in sat_df.columns:
        return pd.DataFrame(columns=expected_out)

    sat = sat_df[sat_df["valid_final"] == True].copy()
    if sat.empty:
        return pd.DataFrame(columns=expected_out)

    common_dates = sorted(set(insitu_df["date"]).intersection(set(sat["date"])))
    rows = []

    tmp = insitu_df.copy()
    tmp["hour_float"] = (
        tmp["datetime"].dt.hour
        + tmp["datetime"].dt.minute / 60
        + tmp["datetime"].dt.second / 3600
    )

    for d in common_dates:
        insitu_day = tmp[tmp["date"] == d].copy()
        sat_day = sat[sat["date"] == d].copy()

        if insitu_day.empty or sat_day.empty:
            continue

        overpass_hour = float(sat_day["overpass_hour_utc"].iloc[0])
        insitu_day["hour_diff"] = (insitu_day["hour_float"] - overpass_hour).abs()
        matched = insitu_day[insitu_day["hour_diff"] <= float(overpass_window_hours)].copy()

        if matched.empty:
            continue

        rows.append(
            {
                "date": d,
                "variable": matched["variable"].iloc[0],
                "value_mean": float(matched["value"].mean()),
                "value_median": float(matched["value"].median()),
                "value_min": float(matched["value"].min()),
                "value_max": float(matched["value"].max()),
                "n_obs": int(len(matched)),
                "overpass_hour_utc": overpass_hour,
            }
        )

    return pd.DataFrame(rows, columns=expected_out)


def _build_model_catalog(n_samples: int):
    models = {
        "linear": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("model", LinearRegression()),
            ]),
            {},
        ),
        "ridge": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", Ridge(random_state=42)),
            ]),
            {"model__alpha": [0.01, 0.1, 1.0, 10.0, 100.0]},
        ),
        "lasso": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", Lasso(max_iter=10000, random_state=42)),
            ]),
            {"model__alpha": [0.001, 0.01, 0.1, 1.0]},
        ),
        "elastic_net": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", ElasticNet(max_iter=10000, random_state=42)),
            ]),
            {"model__alpha": [0.001, 0.01, 0.1, 1.0], "model__l1_ratio": [0.2, 0.5, 0.8]},
        ),
        "poly2": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("poly", PolynomialFeatures(degree=2, include_bias=False)),
                ("scaler", StandardScaler()),
                ("model", LinearRegression()),
            ]),
            {},
        ),
        "poly3": (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("poly", PolynomialFeatures(degree=3, include_bias=False)),
                ("scaler", StandardScaler()),
                ("model", Ridge(random_state=42)),
            ]),
            {"model__alpha": [0.01, 0.1, 1.0, 10.0]},
        ),
    }

    if n_samples >= 12:
        models["svr_rbf"] = (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", SVR(kernel="rbf")),
            ]),
            {
                "model__C": [0.1, 1, 10, 100],
                "model__gamma": ["scale", 0.01, 0.1, 1.0],
                "model__epsilon": [0.01, 0.1, 1.0],
            },
        )

    if n_samples >= 18:
        models["random_forest"] = (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("model", RandomForestRegressor(random_state=42)),
            ]),
            {
                "model__n_estimators": [100, 300],
                "model__max_depth": [None, 3, 5, 8],
                "model__min_samples_leaf": [1, 2, 4],
            },
        )
        models["gradient_boosting"] = (
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("model", GradientBoostingRegressor(random_state=42)),
            ]),
            {
                "model__n_estimators": [100, 300],
                "model__learning_rate": [0.01, 0.05, 0.1],
                "model__max_depth": [2, 3, 4],
            },
        )

    return models


def fit_calibration_model(
    insitu_daily_df: pd.DataFrame,
    sat_df: pd.DataFrame,
    target_variable: str,
    predictor_set: list[str],
    candidate_models: list[str],
    outlier_method: str = "iqr",
    cv_scheme: str = "kfold",
    cv_folds: int = 5,
    test_size: float = 0.25,
    min_samples_required: int = 6,
) -> dict[str, Any]:
    merged = sat_df.merge(insitu_daily_df[["date", "value_mean"]], on="date", how="inner")
    merged = merged.rename(columns={"value_mean": "y_true"})
    merged = merged.dropna(subset=["y_true"] + predictor_set).copy()
    merged = merged.sort_values("date").reset_index(drop=True)

    if len(merged) < min_samples_required:
        raise ValueError(
            f"Too few matched observations after merging in situ and satellite data. "
            f"At least {min_samples_required} are needed."
        )

    n_samples = len(merged)
    if n_samples < 8:
        test_size = 0.20

    train_idx, test_idx = train_test_split(
        np.arange(n_samples),
        test_size=test_size,
        random_state=42,
        shuffle=True,
    )
    train_df = merged.iloc[train_idx].copy().reset_index(drop=True)
    test_df = merged.iloc[test_idx].copy().reset_index(drop=True)

    removed_outliers = pd.DataFrame(columns=["date", "y_true", "reason"])
    if outlier_method != "none" and len(train_df) >= 8:
        if outlier_method == "iqr":
            q1 = train_df["y_true"].quantile(0.25)
            q3 = train_df["y_true"].quantile(0.75)
            iqr = q3 - q1
            low = q1 - 1.5 * iqr
            high = q3 + 1.5 * iqr
            mask = (train_df["y_true"] >= low) & (train_df["y_true"] <= high)
            removed_outliers = train_df.loc[~mask, ["date", "y_true"]].copy()
            removed_outliers["reason"] = "iqr"
            train_df = train_df.loc[mask].copy().reset_index(drop=True)

    if len(train_df) < 5:
        raise ValueError("Too few training samples remain after outlier filtering.")

    models_catalog = _build_model_catalog(len(merged))
    candidate_models_used = [m for m in candidate_models if m in models_catalog]
    if not candidate_models_used:
        raise ValueError("No valid candidate models selected.")

    if cv_scheme == "timeseries":
        cv = TimeSeriesSplit(n_splits=min(cv_folds, max(2, len(train_df) - 1)))
    else:
        cv = KFold(n_splits=min(cv_folds, len(train_df)), shuffle=True, random_state=42)

    scoring = {
        "r2": "r2",
        "neg_rmse": "neg_root_mean_squared_error",
        "neg_mae": "neg_mean_absolute_error",
    }

    results = []
    best_obj = None
    best_score = -np.inf

    for model_name in candidate_models_used:
        estimator, grid = models_catalog[model_name]
        search_estimator = estimator
        best_params = {}

        if grid:
            gs = GridSearchCV(estimator, grid, scoring="r2", cv=cv, n_jobs=None)
            gs.fit(train_df[predictor_set], train_df["y_true"])
            search_estimator = gs.best_estimator_
            best_params = gs.best_params_

        cv_res = cross_validate(
            search_estimator,
            train_df[predictor_set],
            train_df["y_true"],
            cv=cv,
            scoring=scoring,
        )

        search_estimator.fit(train_df[predictor_set], train_df["y_true"])
        y_train_pred = search_estimator.predict(train_df[predictor_set])
        y_test_pred = search_estimator.predict(test_df[predictor_set])

        row = {
            "model_name": model_name,
            "cv_r2_mean": float(np.nanmean(cv_res["test_r2"])),
            "cv_r2_std": float(np.nanstd(cv_res["test_r2"])),
            "cv_rmse_mean": float(np.nanmean(-cv_res["test_neg_rmse"])),
            "cv_rmse_std": float(np.nanstd(-cv_res["test_neg_rmse"])),
            "cv_mae_mean": float(np.nanmean(-cv_res["test_neg_mae"])),
            "cv_mae_std": float(np.nanstd(-cv_res["test_neg_mae"])),
            "train_r2": _r2(train_df["y_true"], y_train_pred),
            "train_rmse": _rmse(train_df["y_true"], y_train_pred),
            "train_mae": _mae(train_df["y_true"], y_train_pred),
            "test_r2": _r2(test_df["y_true"], y_test_pred) if len(test_df) >= 2 else np.nan,
            "test_rmse": _rmse(test_df["y_true"], y_test_pred),
            "test_mae": _mae(test_df["y_true"], y_test_pred),
            "best_params": json.dumps(best_params, default=str),
            "n_samples_after_merge": int(len(merged)),
            "train_samples_before_outliers": int(len(train_idx)),
            "train_samples_after_outliers": int(len(train_df)),
            "test_samples": int(len(test_df)),
            "n_outliers_removed_train": int(len(removed_outliers)),
            "predictor_set": ", ".join(predictor_set),
            "target_variable": target_variable,
        }
        results.append(row)

        score = row["cv_r2_mean"]
        if score > best_score:
            best_score = score
            best_obj = (
                model_name,
                search_estimator,
                row,
                best_params,
                y_train_pred,
                y_test_pred,
                train_df.copy(),
                test_df.copy(),
            )

    metrics_df = pd.DataFrame(results).sort_values(["cv_r2_mean", "test_r2"], ascending=False).reset_index(drop=True)
    assert best_obj is not None

    (
        model_name,
        best_estimator,
        best_row,
        best_params,
        y_train_pred,
        y_test_pred,
        best_train_df,
        best_test_df,
    ) = best_obj

    pred_train = best_train_df[["date", "y_true"]].copy()
    pred_train["set"] = "train"
    pred_train["y_pred"] = y_train_pred

    pred_test = best_test_df[["date", "y_true"]].copy()
    pred_test["set"] = "test"
    pred_test["y_pred"] = y_test_pred

    predictions_df = pd.concat([pred_train, pred_test], ignore_index=True)
    predictions_df["residual"] = predictions_df["y_true"] - predictions_df["y_pred"]
    predictions_df["target_variable"] = target_variable
    predictions_df["model_name"] = model_name
    predictions_df["predictor_set"] = ", ".join(predictor_set)

    final_estimator = best_estimator.fit(merged[predictor_set], merged["y_true"])

    config = {
        "calibration_available": True,
        "target_variable": target_variable,
        "best_model_name": model_name,
        "predictor_set": predictor_set,
        "n_samples_after_merge": int(len(merged)),
        "train_samples": int(len(best_train_df)),
        "test_samples": int(len(best_test_df)),
        "n_outliers_removed_train": int(len(removed_outliers)),
        "cv_scheme": cv_scheme,
        "cv_folds": int(min(cv_folds, len(best_train_df))),
        "best_params": best_params,
        "cv_r2_mean": best_row["cv_r2_mean"],
        "cv_rmse_mean": best_row["cv_rmse_mean"],
        "cv_mae_mean": best_row["cv_mae_mean"],
        "test_r2": best_row["test_r2"],
        "test_rmse": best_row["test_rmse"],
        "test_mae": best_row["test_mae"],
    }

    artifact = {
        "calibration_available": True,
        "model": final_estimator,
        "predictor_set": predictor_set,
        "target_variable": target_variable,
        "best_model_name": model_name,
        "config": config,
    }

    return {
        "config": config,
        "artifact": artifact,
        "metrics_df": metrics_df,
        "predictions_df": predictions_df.sort_values(["set", "date"]).reset_index(drop=True),
        "removed_outliers_df": removed_outliers.reset_index(drop=True),
        "merged_df": merged,
        "sat_df": sat_df,
        "insitu_daily_df": insitu_daily_df,
    }


def build_diagnostics_figure(predictions_df: pd.DataFrame, title: str):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    ax = axes[0]
    ax.scatter(predictions_df["y_true"], predictions_df["y_pred"])
    lo = float(min(predictions_df["y_true"].min(), predictions_df["y_pred"].min()))
    hi = float(max(predictions_df["y_true"].max(), predictions_df["y_pred"].max()))
    ax.plot([lo, hi], [lo, hi])
    ax.set_xlabel("Observed")
    ax.set_ylabel("Predicted")
    ax.set_title("Observed vs predicted")

    ax = axes[1]
    plot_df = predictions_df.copy()
    plot_df["date"] = pd.to_datetime(plot_df["date"], errors="coerce")
    plot_df = plot_df.sort_values("date")
    ax.plot(plot_df["date"], plot_df["y_true"], marker="o", label="Observed")
    ax.plot(plot_df["date"], plot_df["y_pred"], marker="o", label="Predicted")
    ax.set_xlabel("Date")
    ax.set_ylabel("Value")
    ax.set_title("Temporal comparison")
    ax.legend()

    fig.suptitle(title)
    fig.tight_layout()
    return fig


def _pack_download_bytes(result: dict[str, Any]) -> dict[str, bytes]:
    files = {}
    files["calibration_config.json"] = json.dumps(result["config"], indent=2, default=str).encode("utf-8")
    files["calibration_metrics.csv"] = result["metrics_df"].to_csv(index=False).encode("utf-8")
    files["calibration_predictions.csv"] = result["predictions_df"].to_csv(index=False).encode("utf-8")
    files["calibration_removed_outliers.csv"] = result["removed_outliers_df"].to_csv(index=False).encode("utf-8")

    bio = io.BytesIO()
    joblib.dump(result["artifact"], bio)
    files["best_model.joblib"] = bio.getvalue()

    return files


def render_calibration_tab(
    obtener_nombres_embalses: Callable[..., list[str]],
    load_reservoir_shapefile: Callable[..., Any],
    gdf_to_ee_geometry: Callable[..., Any],
    lang: str = "es",
):
    from i18n import STR as _CAL_STR
    def _t(key):
        return _CAL_STR.get(lang, {}).get(key) or _CAL_STR["es"].get(key, key)

    st.subheader(_t("cal.title"))
    st.caption(_t("cal.caption"))

    reservoir_name = st.selectbox(_t("cal.reservoir"), obtener_nombres_embalses(), key="cal_reservoir")
    csv_file = st.file_uploader(_t("cal.csv"), type=["csv"], key="cal_csv")

    if csv_file is None:
        st.info(_t("cal.info"))
        return

    df_raw = pd.read_csv(csv_file)
    st.write(_t("cal.preview"))
    st.dataframe(df_raw.head(10), use_container_width=True)

    excluded = {"date", "time"}
    numeric_candidates = [
        c for c in df_raw.columns
        if c not in excluded and pd.to_numeric(df_raw[c], errors="coerce").notna().sum() > 0
    ]
    if not numeric_candidates:
        st.error(_t("cal.no_numeric"))
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        target_variable = st.selectbox(_t("cal.target"), numeric_candidates, key="cal_target")
        start_hour, end_hour = st.slider(_t("cal.hours"), 0, 23, (8, 15), key="cal_hours")
        overpass_window = st.slider(
            _t("cal.match_window"),
            0.25,
            4.0,
            1.5,
            0.25,
            key="cal_match_window",
        )
    with col2:
        max_cloud = st.slider(_t("cal.max_cloud"), 0, 100, 20, key="cal_max_cloud")
        min_coverage = st.slider(_t("cal.min_coverage"), 0, 100, 50, key="cal_min_coverage")
    with col3:
        predictor_set = st.multiselect(
            _t("cal.predictors"),
            CANDIDATE_INDICES,
            default=["NDCI_705_665", "R705_R665", "MCI_705"],
            key="cal_predictors",
        )
        model_names = ["linear", "ridge", "lasso", "elastic_net", "poly2", "poly3", "svr_rbf", "random_forest", "gradient_boosting"]
        selected_models = st.multiselect(
            _t("cal.models"),
            model_names,
            default=["linear", "ridge", "poly2", "svr_rbf"],
            key="cal_models",
        )
        cv_scheme = st.selectbox(_t("cal.cv"), ["kfold", "timeseries"], index=0, key="cal_cv")
        outlier_method = st.selectbox(_t("cal.outliers"), ["iqr", "none"], index=0, key="cal_outliers")
        min_samples_required = st.slider(
            _t("cal.min_samples"),
            min_value=4,
            max_value=20,
            value=6,
            step=1,
            key="cal_min_samples_required",
        )

    if not predictor_set:
        st.warning(_t("cal.warn_predictor"))
        return

    if not selected_models:
        st.warning(_t("cal.warn_model"))
        return

    # ── IMPORTS ASÍNCRONOS (solo necesarios aquí) ────────────────────────────
    import requests as _cal_requests
    try:
        from streamlit_autorefresh import st_autorefresh as _cal_autorefresh
        _CAL_AUTOREFRESH = True
    except ImportError:
        _CAL_AUTOREFRESH = False

    _CAL_API_URL = st.secrets.get("api_url", "http://localhost:8000")

    # ── BOTÓN: solo construye el payload y envía a la API ────────────────────
    if st.button("Run calibration", key="cal_run"):
        try:
            insitu_clean, priority_dates = prepare_insitu(
                df_raw, target_variable, start_hour, end_hour
            )
        except Exception as e:
            st.error(f"Error preparing in situ data: {e}")
            return

        if not priority_dates:
            st.warning("No valid in situ dates found after filtering.")
            return

        gdf = load_reservoir_shapefile(reservoir_name)
        if gdf is None:
            st.error("Could not load reservoir shapefile.")
            return

        _run_config = {
            "workflow": "calibration",
            "reservoir": reservoir_name,
            "aoi_geojson": gdf.to_crs(epsg=4326).to_json(),
            "target_variable": target_variable,
            "start_hour": int(start_hour),
            "end_hour": int(end_hour),
            "overpass_window": float(overpass_window),
            "max_cloud": int(max_cloud),
            "min_coverage": int(min_coverage),
            "predictor_set": predictor_set,
            "selected_models": selected_models,
            "cv_scheme": cv_scheme,
            "outlier_method": outlier_method,
            "min_samples_required": int(min_samples_required),
            # CSV en base64 para que el worker lo tenga disponible
            "insitu_csv_b64": __import__("base64").b64encode(
                csv_file.getvalue()
            ).decode("utf-8"),
        }

        try:
            _resp = _cal_requests.post(
                f"{_CAL_API_URL}/jobs/submit",
                json=_run_config,
                timeout=60,
            )
            if _resp.ok:
                _job_id = _resp.json()["job_id"]
                st.session_state["cal_job_id"] = _job_id
                st.session_state.pop("cal_job_results", None)
                st.success(
                    f"✅ Calibration job submitted (`{_job_id}`). "
                    "Results will appear here when ready."
                )
            else:
                st.error(f"❌ Error submitting job: {_resp.status_code} – {_resp.text}")
        except Exception as e:
            st.error(f"❌ Could not reach the jobs API: {e}")

    # ── PANEL DE POLLING ─────────────────────────────────────────────────────
    if "cal_job_id" in st.session_state and "cal_job_results" not in st.session_state:
        if _CAL_AUTOREFRESH:
            _cal_autorefresh(interval=5000, key="cal_job_poller")

        _job_id = st.session_state["cal_job_id"]
        try:
            _status = _cal_requests.get(
                f"{_CAL_API_URL}/jobs/{_job_id}/status", timeout=5
            ).json()
        except Exception:
            _status = {"state": "unknown"}

        _state = _status.get("state", "unknown")

        if _state == "running":
            _pct  = _status.get("progress", 0)
            _step = _status.get("step", "Processing…")
            st.progress(_pct / 100, text=f"⏳ {_step}")
            if not _CAL_AUTOREFRESH:
                st.info("Reload the page to update the calibration status.")

        elif _state == "done":
            st.session_state["cal_job_results"] = _status.get("results", {})
            del st.session_state["cal_job_id"]
            st.rerun()

        elif _state == "error":
            st.error(f"❌ Calibration failed: {_status.get('error', 'unknown error')}")
            del st.session_state["cal_job_id"]

        else:
            st.info("⏳ Waiting for job server response…")

    # ── RENDER DE RESULTADOS ─────────────────────────────────────────────────
    # Se activa cuando los resultados ya están en session_state
    if "cal_job_results" in st.session_state:
        _res = st.session_state["cal_job_results"]
        _config      = _res.get("config", {})
        _metrics     = _res.get("metrics_df", [])
        _predictions = _res.get("predictions_df", [])
        _outliers    = _res.get("removed_outliers_df", [])
        _diag_b64    = _res.get("diagnostics_png_b64", None)

        st.success("Calibration finished successfully.")

        c1, c2, c3 = st.columns(3)
        c1.metric("Matched samples", _config.get("n_samples_after_merge", "—"))
        c2.metric("Best model",      _config.get("best_model_name", "—"))
        c3.metric("CV R²",           f"{_config.get('cv_r2_mean', 0):.3f}")

        st.markdown("#### Best calibration summary")
        st.json(_config, expanded=False)

        if _metrics:
            st.markdown("#### Model comparison")
            st.dataframe(pd.DataFrame(_metrics), use_container_width=True)

        if _predictions:
            st.markdown("#### Predictions")
            st.dataframe(pd.DataFrame(_predictions), use_container_width=True)

        if _outliers:
            st.markdown("#### Removed outliers")
            st.dataframe(pd.DataFrame(_outliers), use_container_width=True)

        if _diag_b64:
            import base64
            from PIL import Image
            import io as _io
            _img_bytes = base64.b64decode(_diag_b64)
            _img = Image.open(_io.BytesIO(_img_bytes))
            st.image(_img, use_column_width=True)

        # Botones de descarga — los artefactos los devuelve el worker como base64
        _downloads = _res.get("download_files", {})
        if _downloads:
            d1, d2, d3, d4, d5 = st.columns(5)
            if "calibration_config.json" in _downloads:
                d1.download_button(
                    "Config JSON",
                    __import__("base64").b64decode(_downloads["calibration_config.json"]),
                    file_name="calibration_config.json",
                    mime="application/json",
                )
            if "calibration_metrics.csv" in _downloads:
                d2.download_button(
                    "Metrics CSV",
                    __import__("base64").b64decode(_downloads["calibration_metrics.csv"]),
                    file_name="calibration_metrics.csv",
                    mime="text/csv",
                )
            if "calibration_predictions.csv" in _downloads:
                d3.download_button(
                    "Predictions CSV",
                    __import__("base64").b64decode(_downloads["calibration_predictions.csv"]),
                    file_name="calibration_predictions.csv",
                    mime="text/csv",
                )
            if "calibration_removed_outliers.csv" in _downloads:
                d4.download_button(
                    "Outliers CSV",
                    __import__("base64").b64decode(_downloads["calibration_removed_outliers.csv"]),
                    file_name="calibration_removed_outliers.csv",
                    mime="text/csv",
                )
            if "best_model.joblib" in _downloads:
                d5.download_button(
                    "Model joblib",
                    __import__("base64").b64decode(_downloads["best_model.joblib"]),
                    file_name="best_model.joblib",
                    mime="application/octet-stream",
                )

        st.info(
            "The calibration model is stored in session state. "
            "A full pixel-wise calibrated map requires translating the selected "
            "model to an Earth Engine raster expression."
        )
