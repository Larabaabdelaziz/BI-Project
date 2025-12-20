
# 1. Clone & Setup
git clone https://github.com/Larabaabdelaziz/BI-Project.git
cd BI-Project
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Mac/Linux

# 2. Install Dependencies
pip install -r requirements.txt

# 3. Install Requirements
bash
pip install pandas streamlit plotly sqlalchemy pyodbc jupyter

# 4. Run ETL (Creates database)
python etl/etl.py

# 5. Launch Dashboard
streamlit run etl/dashboard.py
