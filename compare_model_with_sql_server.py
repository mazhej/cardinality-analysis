from sqlalchemy import inspect, create_engine
from ir_modernized.models import Base  # Import your existing models
from generated_models import Base as GeneratedBase  # Import the generated models

# Define the connection string for SQL Server
connection_string = "mssql+pyodbc://FRCDBP20Dev/P20W_IR?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Use the inspector to get database schema information
inspector = inspect(engine)

# Compare tables
existing_tables = set(Base.metadata.tables.keys())
generated_tables = set(GeneratedBase.metadata.tables.keys())

if existing_tables - generated_tables:
    print("Tables in existing models but not in the database:")
    print(existing_tables - generated_tables)

if generated_tables - existing_tables:
    print("Tables in the database but not in existing models:")
    print(generated_tables - existing_tables)

# Compare columns for each table
for table_name in existing_tables.intersection(generated_tables):
    print(f"\nComparing table: {table_name}")
    existing_columns = Base.metadata.tables[table_name].columns
    generated_columns = GeneratedBase.metadata.tables[table_name].columns

    existing_column_names = set(existing_columns.keys())
    generated_column_names = set(generated_columns.keys())

    if existing_column_names - generated_column_names:
        print(f"Columns in existing models but not in the database: {existing_column_names - generated_column_names}")

    if generated_column_names - existing_column_names:
        print(f"Columns in the database but not in existing models: {generated_column_names - existing_column_names}")

    # Compare column details
    for column_name in existing_column_names.intersection(generated_column_names):
        existing_col = existing_columns[column_name]
        generated_col = generated_columns[column_name]

        # Ignore collation differences when comparing types
        existing_type = str(existing_col.type).split(" COLLATE")[0]
        generated_type = str(generated_col.type).split(" COLLATE")[0]

        if existing_type != generated_type:
            print(f"Type mismatch for column {column_name}: Existing({existing_type}) vs Generated({generated_type})")

        if existing_col.nullable != generated_col.nullable:
            print(f"Nullable mismatch for column {column_name}: Existing({existing_col.nullable}) vs Generated({generated_col.nullable})")