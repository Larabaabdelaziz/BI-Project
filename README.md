
# 1. Clone & Setup
git clone https://github.com/Larabaabdelaziz/BI-Project.git
cd BI-Project
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Mac/Linux

Requirements

Python 3.8+

SQL Server (or SQL Express)

ODBC Driver 17 for SQL Server

# 2. Install Requirements
bash
pip install pandas streamlit plotly sqlalchemy pyodbc jupyter

# 3. Run ETL (Creates database)
python etl/etl.py

# 4. Launch Dashboard
streamlit run etl/dashboard.py






Choix Techniques


Bibliothèques Python


Streamlit : Dashboard rapide sans frontend complexe


Pandas : Standard pour manipulation de données


Plotly : Graphiques interactifs simples


SQLAlchemy : Connexion base de données simplifiée


Architecture


Schéma en étoile : Compréhension facile, requêtes rapides

ETL en Python : Un seul langage, coût nul

SQL Server : Standard entreprise, performances OLAP
