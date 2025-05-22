import pyodbc
import requests


SERVER = "DESKTOP-4LC4SRM\\MSSQLSERVER01"

def connect_db():
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )
    return conn