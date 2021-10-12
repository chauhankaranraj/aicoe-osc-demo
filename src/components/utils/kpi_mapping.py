import pandas as pd
from src.components.config import config

df = pd.read_csv('https://raw.githubusercontent.com/chauhankaranraj/aicoe-osc-demo/839d7db5fed7f4e3aa4edf2da199b418f1f29355/data/kpi_mapping/ESG/kpi_mapping.csv', header=0)
_KPI_MAPPING = {str(i[0]): i[1] for i in df[["kpi_id", "question"]].values}
KPI_MAPPING = {(float(key)): value for key, value in _KPI_MAPPING.items()}

# Which questions should be added the year
ADD_YEAR = df[df["add_year"]].kpi_id.tolist()

# Category where the answer to the question should originate from
KPI_CATEGORY = {
    i[0]: [j.strip() for j in i[1].split(", ")]
    for i in df[["kpi_id", "kpi_category"]].values
}
