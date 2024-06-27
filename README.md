# Bottom line up front
- You probably haven't learned everything you need to from your business data.
- Dashboards and SQL are not adequate data analysis tools.
- Machine learning isn't just for making predictions; you can use it for investigation.
- *What am I missing?* is the most important question in data.
  - That's why analysts and data scientists should *over-measure then distill* with ML.
- Statistical testing (A/B/n) is important, but we need better theories to test. Deeper exploration generates better theories.
  - See [notebook 3](./3_understanding.ipynb) for a demo of deeper exploration.

# Intro
This project uses a simple Linux GPU setup to model stock market data. It may require modification to run in different environments. The data are from the [Sharadar Core US Equities Bundle](https://data.nasdaq.com/databases/SFA). Environmental variables `NASDAQ_DATA_API_KEY` and `DATA_HOME` are expected. [Anaconda](https://www.anaconda.com/download) is used for the Python distribution.

# Project structure
The `download.py` script fetches the tables via the Nasdaq API and stores them in `$DATA_HOME/analytics_demo`, both within a [duckdb](https://www.duckdb.org) file and individually as parquet files. The [dbt](https://www.getdbt.com) project, located in the [dbt_sharadar_demo folder](./dbt_sharadar_demo), must then be run for preprocessing. See the lineage graph below to get a sense of its structure:

<img src="./images/dbt_lineage.png" alt="dbt lineage graph" width="1000"/>

The notebooks
1. `1_feature_prep.ipynb` prepares the target and feature data for notebooks 2 and 3.
2. `2_prediction.ipynb` gives a classic case of using ML for prediction.
3. **`3_understanding.ipynb` demonstrates the main purpose of this repo. It makes a simple, powerful case for using ML to build understanding.**
4. `7_business_credit_risk_proxy.ipynb` is just a fun, industry-specific prediction problem.

# Setup
1. Create anaconda environment then activate.
```bash
conda env create -f environment.yml --solver=libmamba
conda activate analytics_demo
```
2. Set up dbt config, typically found at `~/.dbt/profiles.yml`, to include the database filepath.
```yaml
dbt_sharadar_demo:
  outputs:
    prod:
      type: duckdb
      path: "{{ env_var('DATA_HOME') }}/analytics_demo/sharadar.duckdb"
      threads: 2
  target: prod
```
3. Fetch tables.
```python
python3 download.py
```
3. Move to the dbt folder to run dbt.
```bash
cd dbt_sharadar_demo
dbt run
cd ..
```
4. Launch Jupyter to host notebooks if you prefer this over an IDE.
```python
jupyter lab
```

## Tear down

To remove the anaconda environment, simply run:
```bash
conda env remove --name analytics_demo
```

[See here](https://docs.anaconda.com/anaconda/install/uninstall/) if you also wish to remove Anaconda.

---

### Disclaimer: **I am not an investment professional. None of my work within or related to this repository should be considered investment advice. It is not.**