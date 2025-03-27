import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up Snowflake connection using credentials from environment variables
db_user = os.getenv("SNOWFLAKE_USER")
db_password = os.getenv("SNOWFLAKE_PASSWORD")
db_account = os.getenv("SNOWFLAKE_ACCOUNT")
db_name = os.getenv("SNOWFLAKE_DB")
db_schema = os.getenv("SNOWFLAKE_SCHEMA")

# Create SQLAlchemy engine for Snowflake
engine = create_engine(f'snowflake://{db_user}:{db_password}@{db_account}/{db_name}/{db_schema}')

# SQL query to count unique customers per country
query = """
    SELECT Country, COUNT(DISTINCT CustomerID) AS TotalCustomers
    FROM ALL_ECOMMERCE_DATA
    WHERE CustomerID IS NOT NULL
    GROUP BY Country
    ORDER BY TotalCustomers DESC;
"""

# Fetch data from Snowflake
df = pd.read_sql(query, engine)

# Standardize column names to lowercase for easier access
df.columns = df.columns.str.lower()

# Set Seaborn style for better visuals
sns.set_theme(style="whitegrid")

# Initialize the figure
plt.figure(figsize=(14, 7))

# Create a horizontal bar plot
ax = sns.barplot(
    x=df["totalcustomers"],
    y=df["country"],
    palette="Blues_r"
)

# Add numeric labels on each bar
for index, value in enumerate(df["totalcustomers"]):
    ax.text(value + 5, index, f"{value:,}", va="center", fontsize=10)

# Set labels and title
plt.xlabel("Number of Customers", fontsize=12)
plt.ylabel("Country", fontsize=12)
plt.title("Total Customers per Country", fontsize=14)

# Add a light grid for better readability
plt.grid(axis="x", linestyle="--", alpha=0.7)

# Display the plot
plt.show()
