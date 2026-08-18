# Netflix ML Methods Documentation (`ml_agent.py`)

This document details the machine learning models implemented in `apps/ai_backend/agents/ml_agent.py` running on top of the PostgreSQL Gold Data Warehouse.

---

## 1. Ordinary Least Squares (OLS) Linear Regression

### Problem Solved
Time-series forecasting of annual Netflix catalog content growth.

### Target Variable
`total_releases` (Number of distinct titles released in a given year).

### Input Features
`release_year` (Historical sequence from 1995 to 2021).

### Why Selected
Ordinary Least Squares (OLS) Linear Regression provides a mathematically rigorous, transparent, and interpretable baseline for trend modeling without risk of overfitting.

### $R^2$ Metric Meaning
* **Score Achieved:** $R^2 = 0.6981$
* **Interpretation:** $69.81\%$ of the variance in annual content release volume is explained by the historical linear time progression.
* **Annual Slope Rate:** $+47.16$ new titles per year.

### Model Limitations
* Assumes a linear growth trajectory; does not model exponential acceleration or market saturation ceilings.
* Projections represent statistical extrapolations based on historical release patterns and are not guaranteed future outcomes.

---

## 2. Isolation Forest (Unsupervised Anomaly Detection)

### Problem Solved
Detecting anomalous country catalog distribution profiles across multiple operational dimensions.

### Input Features
- `titles_count`: Total catalog titles produced by country.
- `movie_count`: Total feature films.
- `tv_count`: Total episodic TV series.
- `avg_movie_duration`: Average runtime in minutes.
- `avg_content_age`: Average age from release year to present.

### Why Selected
Isolation Forest is an unsupervised ensemble algorithm that isolates anomalies by randomly partitioning feature space. Outliers require fewer splits to isolate, making it highly effective for multi-dimensional data without requiring labeled ground truth.

### Meaning of Anomaly
An anomaly flag indicates that a country's catalog composition significantly deviates from global portfolio benchmarks—such as extreme single-format specialization (e.g. India with >90% film output, Japan with heavy TV animation, Egypt with pure movie focus) or atypical average duration distributions.

### Model Limitations
* Identifies statistical structural outliers, not content quality or audience ratings.
* Sensitive to contamination hyperparameter settings (set to `0.15`).

---

## 3. K-Means Clustering

### Problem Solved
Segmenting content-producing countries into distinct strategic tiers.

### Input Features
- `titles_count`: Total titles volume.
- `movie_count`: Total film volume.
- `tv_count`: Total TV series volume.

### K Selection
$K = 3$ clusters selected using standard elbow analysis to segment countries into:
- **Cluster 1 (Heavy Global Producers):** US, India, UK.
- **Cluster 2 (Mid-Tier Regional Producers):** Canada, France, Japan, South Korea.
- **Cluster 3 (Emerging / Niche Hubs):** All remaining producing nations.

### Feature Preprocessing
Features are standardized using `StandardScaler` ($\mu=0, \sigma=1$) prior to Euclidean distance calculations.

### Model Limitations
* Requires explicit pre-specification of cluster count $K$.
* Assumes spherical cluster shapes in feature space.
