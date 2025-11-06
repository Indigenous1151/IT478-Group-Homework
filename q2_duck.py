import duckdb
import pandas as pd # Only needed for type hinting in function

def display_question_header(question_number= "") -> None:
    """Displays a header for the question"""
    HEADER_WIDTH = 40
    print()
    print("*" * HEADER_WIDTH)
    middle = ((HEADER_WIDTH - len(question_number)) // 2) - 2
    print("*", " " * middle, end='')
    print(question_number, end='')
    print(" " * (middle if len(question_number) % 2 == 0 else middle+1), "*")
    print("*" * HEADER_WIDTH)
    
    
def print_rows(df: pd.DataFrame, n = 5) -> None:
    """Prints the first 5 rows of the df"""
    print(df.head(n=n).to_string(index=False))


# Create connection to database, create file if needed
conn = duckdb.connect("performance.duckdb")
cur = conn.cursor()

print("DUCKDB Connection Created.\n")

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

# Load rows into table from the df
conn.execute("DELETE FROM product_data") # no TRUNCATE, DELETE FROM instead
conn.register("temp_view", df)
conn.execute("INSERT INTO product_data SELECT * FROM temp_view")
conn.unregister("temp_view")

# Get row count
count = conn.execute("SELECT COUNT(*) AS count FROM product_data").fetchone()[0] # type: ignore
print(f"Added {count} rows.\n")


print("Changes Committed.\n")

# Run statements
ROW_DISPLAY_LIMIT = 5

# Q1
display_question_header("Q1")
df = cur.execute("SELECT COUNT(*) FROM product_data").df()
print_rows(df, ROW_DISPLAY_LIMIT)

# Q2
display_question_header("Q2")
df = cur.execute("SELECT AVG(price), MAX(price) FROM product_data").df()
print_rows(df, ROW_DISPLAY_LIMIT)

# Q3
display_question_header("Q3")
df = cur.execute("""
    SELECT category, COUNT(*) AS ProductCount
    FROM product_data
    GROUP BY category
    ORDER BY ProductCount DESC
""").df()
print_rows(df, ROW_DISPLAY_LIMIT)

# Q4
display_question_header("Q4")
df = cur.execute("""
    SELECT Product_Name, SUM(Quantity_Sold) AS TotalSold
    FROM product_data
    GROUP BY Product_Name
    ORDER BY TotalSold DESC
    LIMIT 1;
""").df()
print_rows(df, ROW_DISPLAY_LIMIT)

#Q5
display_question_header("Q5")
df = cur.execute("""
    SELECT Category, ROUND(AVG(Rating), 2) AS AvgRating
    FROM product_data
    GROUP BY Category
""").df()
print_rows(df, ROW_DISPLAY_LIMIT)

# close connection to db
conn.close()
