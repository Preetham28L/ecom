import sqlite3
import csv
import os

# Connect to the database
conn = sqlite3.connect('bookstore.db')
cursor = conn.cursor()

# List of CSV files to process
csv_files = [
    'members.csv',
    'authors.csv',
    'books.csv',
    'orders.csv',
    'order_details.csv'
]

def get_table_name(filename):
    """Extract table name from filename by removing .csv extension"""
    return filename.replace('.csv', '')

def infer_column_type(value):
    """Try to infer the SQLite column type from a sample value"""
    if not value or value.strip() == '':
        return 'TEXT'
    
    # Try integer
    try:
        int(value)
        return 'INTEGER'
    except ValueError:
        pass
    
    # Try real/float
    try:
        float(value)
        return 'REAL'
    except ValueError:
        pass
    
    # Default to TEXT
    return 'TEXT'

def create_table(cursor, table_name, columns):
    """Dynamically create a table with the given columns"""
    # Infer types from first data row if available
    # For now, we'll use a simple approach: check column names and values
    column_defs = []
    
    # Read a sample row to infer types (we'll do this during insertion)
    # For now, create all as TEXT initially, we can optimize later
    for col in columns:
        # Check if column name suggests a type
        if col.endswith('_id') or col == 'id':
            column_defs.append(f"{col} INTEGER")
        elif 'price' in col.lower() or 'quantity' in col.lower():
            column_defs.append(f"{col} REAL")
        elif 'date' in col.lower():
            column_defs.append(f"{col} TEXT")
        else:
            column_defs.append(f"{col} TEXT")
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
    cursor.execute(create_sql)

def is_empty_row(row):
    """Check if a row is empty (all values are empty or whitespace)"""
    return not row or all(not cell or not cell.strip() for cell in row)

# Process each CSV file
for csv_file in csv_files:
    try:
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"Warning: {csv_file} not found. Skipping...")
            continue
        
        # Get table name from filename
        table_name = get_table_name(csv_file)
        
        # Open and read the CSV file
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            # Read header row
            header = next(csv_reader, None)
            if not header:
                print(f"Warning: {csv_file} is empty. Skipping...")
                continue
            
            # Clean header (remove whitespace)
            header = [col.strip() for col in header]
            
            # Create table
            create_table(cursor, table_name, header)
            
            # Prepare for batch insert
            rows_inserted = 0
            
            # Read and insert data rows
            for row in csv_reader:
                # Filter out empty rows
                if is_empty_row(row):
                    continue
                
                # Ensure row has same length as header (pad with empty strings if needed)
                while len(row) < len(header):
                    row.append('')
                
                # Truncate row if it's longer than header
                row = row[:len(header)]
                
                # Clean row values
                row = [cell.strip() if cell else '' for cell in row]
                
                # Create placeholders for SQL INSERT
                placeholders = ','.join(['?' for _ in header])
                columns_str = ','.join(header)
                
                # Insert row
                insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                cursor.execute(insert_sql, row)
                rows_inserted += 1
            
            # Commit after each file
            conn.commit()
            print(f"Successfully ingested {csv_file} ({rows_inserted} rows inserted)")
    
    except Exception as e:
        print(f"Error processing {csv_file}: {str(e)}")
        # Continue with next file even if one fails
        continue

# Close the connection
conn.close()
print("\nData ingestion completed!")

