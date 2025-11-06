import sqlite3
import pandas as pd

def display_question_header(question_number: str) -> None:
    """Displays a header for the question"""
    HEADER_WIDTH = 40
    print()
    print("*" * HEADER_WIDTH)
    middle = ((HEADER_WIDTH - len(question_number)) // 2) - 2
    print("*", " " * middle, end='')
    print(question_number, end='')
    print(" " * (middle if len(question_number) % 2 == 0 else middle+1), "*")
    print("*" * HEADER_WIDTH)
    
    
def print_rows(df: pd.DataFrame, n: int) -> None:
    """Prints the first 5 rows of the df"""
    print(df.head(n=n).to_string(index=False))


# Create connection to database, create file if needed
conn = sqlite3.connect("performance.db")
cur = conn.cursor()

print("SQLITE3 Connection Created.\n")

# load csv data into df
df = pd.read_csv("Product_Performance.csv")

# Create the table
cur.execute("""
    CREATE TABLE IF NOT EXISTS product_data (
        productid     INTEGER  PRIMARY KEY,
        product_name  TEXT     NOT NULL,
        category      TEXT     NOT NULL,
        price         REAL     NOT NULL,
        quantity_sold INTEGER  NOT NULL,
        rating        REAL     NOT NULL,
        brand         TEXT     NOT NULL
    )
""")

print("Created Table.\n")

# load the df into the table
df.to_sql("product_data", conn, if_exists="replace", index=False)
# getting the row count for output
count = pd.read_sql_query("SELECT COUNT(*) AS count FROM product_data", conn).iloc[0, 0]
print(f"Added {count} rows.\n")

# commit the changes (not automatic like in DuckDB)
conn.commit()
print("Changes Committed.\n")

# Run statements
ROW_DISPLAY_LIMIT = 5

# Q1
display_question_header("Q1")
query = "SELECT COUNT(*) FROM product_data"
df = pd.read_sql_query(query, conn)
print_rows(df, ROW_DISPLAY_LIMIT)

# Q2
display_question_header("Q2")
query = "SELECT AVG(price), MAX(price) FROM product_data"
df = pd.read_sql_query(query, conn)
print_rows(df, ROW_DISPLAY_LIMIT)

# Q3
display_question_header("Q3")
query = """
    SELECT category, COUNT(*) AS ProductCount
    FROM product_data
    GROUP BY category
    ORDER BY ProductCount DESC
"""
df = pd.read_sql_query(query, conn)
print_rows(df, ROW_DISPLAY_LIMIT)

# Q4
display_question_header("Q4")
query = """
    SELECT Product_Name, SUM(Quantity_Sold) AS TotalSold
    FROM product_data
    GROUP BY Product_Name
    ORDER BY TotalSold DESC
    LIMIT 1;
"""
df = pd.read_sql_query(query, conn)
print_rows(df, ROW_DISPLAY_LIMIT)

#Q5
display_question_header("Q5")
query = """
    SELECT Category, ROUND(AVG(Rating), 2) AS AvgRating
    FROM product_data
    GROUP BY Category
"""
df = pd.read_sql_query(query, conn)
print_rows(df, ROW_DISPLAY_LIMIT)

# close connection to db
conn.close()