import sqlite3

def connect_to_database():
    """Connect to the bookstore database"""
    try:
        conn = sqlite3.connect('bookstore.db')
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_revenue_query(conn):
    """Execute SQL query to calculate total revenue by author"""
    cursor = conn.cursor()
    
    query = """
    SELECT 
        a.author_name AS "Author Name",
        b.title AS "Book Title",
        ROUND(SUM(b.price * od.quantity), 2) AS "Total Sales Amount"
    FROM 
        authors a
    INNER JOIN 
        books b ON a.author_id = b.author_id
    INNER JOIN 
        order_details od ON b.book_id = od.book_id
    INNER JOIN 
        orders o ON od.order_id = o.order_id
    WHERE 
        o.status != 'Cancelled'
    GROUP BY 
        a.author_name, b.title
    ORDER BY 
        "Total Sales Amount" DESC
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error executing query: {e}")
        return None

def display_results(results):
    """Display query results in a formatted table"""
    if not results:
        print("\nNo results found.")
        return
    
    # Calculate column widths
    headers = ["Author Name", "Book Title", "Total Sales Amount"]
    max_author_len = max(len(str(row[0])) for row in results) if results else 0
    max_title_len = max(len(str(row[1])) for row in results) if results else 0
    max_sales_len = 15  # For formatted currency
    
    # Adjust column widths to fit headers and data
    col1_width = max(len(headers[0]), max_author_len, 15)
    col2_width = max(len(headers[1]), max_title_len, 20)
    col3_width = max(len(headers[2]), max_sales_len, 18)
    
    # Create format strings
    header_format = f"| {{:<{col1_width}}} | {{:<{col2_width}}} | {{:>{col3_width}}} |"
    row_format = f"| {{:<{col1_width}}} | {{:<{col2_width}}} | {{:>{col3_width}}} |"
    separator = "+" + "-" * (col1_width + 2) + "+" + "-" * (col2_width + 2) + "+" + "-" * (col3_width + 2) + "+"
    
    # Display the table
    print("\n" + "=" * (col1_width + col2_width + col3_width + 10))
    print("AUTHOR REVENUE REPORT".center(col1_width + col2_width + col3_width + 10))
    print("=" * (col1_width + col2_width + col3_width + 10))
    print(separator)
    print(header_format.format(headers[0], headers[1], headers[2]))
    print(separator)
    
    for row in results:
        author_name = str(row[0])
        book_title = str(row[1])
        sales_amount = f"${row[2]:.2f}"
        print(row_format.format(author_name, book_title, sales_amount))
    
    print(separator)
    print(f"\nTotal records: {len(results)}")
    print("=" * (col1_width + col2_width + col3_width + 10) + "\n")

def show_menu():
    """Display the main menu"""
    print("\n" + "=" * 50)
    print("BOOKSTORE QUERY MENU")
    print("=" * 50)
    print("1. Run Author Revenue Query")
    print("2. Exit")
    print("=" * 50)

def main():
    """Main function to run the query application"""
    conn = connect_to_database()
    if not conn:
        print("Failed to connect to database. Exiting...")
        return
    
    while True:
        show_menu()
        choice = input("Enter your choice (1-2): ").strip()
        
        if choice == '1':
            print("\nExecuting query...")
            results = execute_revenue_query(conn)
            if results is not None:
                display_results(results)
        elif choice == '2':
            print("\nThank you for using the Bookstore Query System. Goodbye!")
            break
        else:
            print("\nInvalid choice. Please enter 1 or 2.")
    
    conn.close()

if __name__ == "__main__":
    main()

