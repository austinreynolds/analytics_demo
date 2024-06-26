# Bottom line up front
- You probably haven't learned everything you need to from your business data.
- Dashboards and SQL display results; they are not adequate analysis tools.
- Machine learning isn't just for making predictions; you can use it for investigation.
- *What am I missing?* is the most important question in data.
  - Investigators must *over-measure then distill with ML* in order to miss less.
- Statistical testing (A/B/n) is important to assess an idea, but *we need to generate better ideas*.
  - ML-powered exploration, by taking more into account, can lead to deeper, more complete theories about what is happening.
  - See notebook 3 for an example: [Latent dimensions: The most powerful analysis nobody does](./3_understanding.ipynb).

# Intro
This project uses a simple Linux GPU setup to model stock market data. It uses the [Anaconda](https://www.anaconda.com/download) Python distribution and data from the [Sharadar Core US Equities Bundle](https://data.nasdaq.com/databases/SFA) on the Nasdaq data link. Environmental variables `NASDAQ_DATA_API_KEY` and `DATA_HOME` are expected.

# Project structure
The `download.py` script fetches the tables, stores them in parquet files, then loads them into a [duckdb](https://www.duckdb.org) file, all within `$DATA_HOME/analytics_demo`. The [dbt](https://www.getdbt.com) project, located in [dbt_sharadar_demo](./dbt_sharadar_demo), must then be run for preprocessing. Lineage graph:

<img src="./images/dbt_lineage.png" alt="dbt lineage graph" width="1000"/>

The notebooks:
- [1_prep.ipynb](./1_prep.ipynb) prepares the target and feature data for notebooks 2 and 3.
- [2_prediction.ipynb](./2_prediction.ipynb) gives a classic ML use case for prediction.
- **[3_understanding.ipynb](./3_understanding.ipynb) demonstrates the main purpose of this repo. It gives a simple, powerful example of how to use ML to build understanding.**
- [7_business_credit_risk_proxy.ipynb](7_business_credit_risk_proxy.ipynb) is a more stand-alone notebook about a fun industry-specific problem.

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
