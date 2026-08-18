import numpy as np
import logging
from typing import List, Dict, Any, Optional
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger("ml_agent")

class MLAgent:
    """Specialized ML Agent executing real statistical analysis, regression forecasting, anomaly detection, and clustering."""

    def __init__(self):
        self.name = "ML Agent"

    def analyze(self, query: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Entry point for ML operations based on user query and extracted dataset."""
        q_lower = query.lower()
        results = {
            "model_type": None,
            "metrics": {},
            "predictions": [],
            "anomalies": [],
            "clusters": []
        }

        if not data:
            return results

        # 1. Forecasting & Trend Regression Analysis
        if any(w in q_lower for w in ["predict", "forecast", "growth", "trend", "future"]):
            return self.forecast_content_growth(data)

        # 2. Anomaly Detection
        elif any(w in q_lower for w in ["unusual", "anomaly", "anomalies", "outlier", "outliers", "pattern"]):
            return self.detect_anomalies(data)

        # 3. Clustering & Market Segmentation
        elif any(w in q_lower for w in ["cluster", "clustering", "segment", "segmentation", "group"]):
            return self.cluster_countries(data)

        # Fallback default: run anomaly detection or regression depending on dataset features
        if "release_year" in data[0]:
            return self.forecast_content_growth(data)
        elif "titles_count" in data[0] and "country_name" in data[0]:
            return self.detect_anomalies(data)

        return results

    def forecast_content_growth(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fits an Ordinary Least Squares Linear Regression model on release year counts to forecast content growth."""
        # Extract valid features
        valid_rows = [r for r in data if "release_year" in r and "total_releases" in r]
        if len(valid_rows) < 5:
            return {"model_type": "LinearRegression", "error": "Insufficient data points for regression"}

        X = np.array([[float(r["release_year"])] for r in valid_rows])
        y = np.array([float(r["total_releases"]) for r in valid_rows])

        # Fit Model
        model = LinearRegression()
        model.fit(X, y)

        r2_score = float(model.score(X, y))
        slope = float(model.coef_[0])
        intercept = float(model.intercept_)

        # Forecast next 5 years (2022 to 2026)
        future_years = np.array([[2022], [2023], [2024], [2025], [2026]])
        predictions_y = model.predict(future_years)

        predictions = []
        for yr, pred in zip(future_years.flatten(), predictions_y):
            predictions.append({
                "year": int(yr),
                "predicted_releases": max(0, int(round(pred)))
            })

        return {
            "model_type": "Linear Regression (OLS Time-Series)",
            "metrics": {
                "r2_score": round(r2_score, 4),
                "annual_slope_rate": round(slope, 2),
                "intercept": round(intercept, 2),
                "training_years": f"{int(X.min())}-{int(X.max())}"
            },
            "predictions": predictions
        }

    def detect_anomalies(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Applies Isolation Forest & Z-Score analysis on multi-dimensional catalog metrics to spot anomalous countries."""
        # Require feature columns
        feature_cols = ["titles_count", "movie_count", "tv_count", "avg_movie_duration", "avg_content_age"]
        valid_rows = [r for r in data if all(c in r and r[c] is not None for c in feature_cols)]

        if len(valid_rows) < 5:
            # Fallback to Z-score on titles_count if multi-dim not available
            titles = np.array([r.get("titles_count", 0) for r in data], dtype=float)
            mean = np.mean(titles)
            std = np.std(titles) + 1e-6
            z_scores = np.abs((titles - mean) / std)

            anomalies = []
            for r, z in zip(data, z_scores):
                if z > 1.8:
                    anomalies.append({
                        "name": r.get("country_name", "Unknown"),
                        "titles_count": r.get("titles_count"),
                        "z_score": round(float(z), 2),
                        "reason": f"Significantly higher title volume ({r.get('titles_count')} vs avg {round(mean)})"
                    })

            return {
                "model_type": "Z-Score Statistical Anomaly Detector",
                "metrics": {"total_analyzed": len(data), "anomalies_detected": len(anomalies)},
                "anomalies": anomalies
            }

        # Multi-dimensional Isolation Forest
        X = np.array([[float(r[c]) for c in feature_cols] for r in valid_rows])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        clf = IsolationForest(contamination=0.15, random_state=42)
        preds = clf.fit_predict(X_scaled) # -1 for anomaly, 1 for normal

        anomalies = []
        for r, pred in zip(valid_rows, preds):
            if pred == -1:
                # Compute specific outlier reason
                movie_ratio = round(r["movie_count"] / (r["titles_count"] + 1e-5) * 100, 1)
                anomalies.append({
                    "name": r["country_name"],
                    "titles_count": r["titles_count"],
                    "movie_ratio_pct": movie_ratio,
                    "avg_duration": r["avg_movie_duration"],
                    "reason": f"Unusual content profile ({movie_ratio}% movies, avg duration {r['avg_movie_duration']} min)"
                })

        return {
            "model_type": "Isolation Forest (Unsupervised Anomaly Detection)",
            "metrics": {
                "contamination": 0.15,
                "total_analyzed": len(valid_rows),
                "anomalies_detected": len(anomalies)
            },
            "anomalies": anomalies
        }

    def cluster_countries(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Applies K-Means clustering to group countries into distinct content production clusters."""
        feature_cols = ["titles_count", "movie_count", "tv_count"]
        valid_rows = [r for r in data if all(c in r and r[c] is not None for c in feature_cols)]

        if len(valid_rows) < 4:
            return {"model_type": "K-Means Clustering", "error": "Insufficient rows for clustering"}

        X = np.array([[float(r[c]) for c in feature_cols] for r in valid_rows])
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        n_clusters = min(3, len(valid_rows))
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)

        cluster_summary = {}
        for r, label in zip(valid_rows, labels):
            lbl = f"Cluster {int(label) + 1}"
            if lbl not in cluster_summary:
                cluster_summary[lbl] = []
            cluster_summary[lbl].append(r["country_name"])

        return {
            "model_type": "K-Means Clustering (n=3)",
            "metrics": {"n_clusters": n_clusters, "total_countries": len(valid_rows)},
            "clusters": cluster_summary
        }
