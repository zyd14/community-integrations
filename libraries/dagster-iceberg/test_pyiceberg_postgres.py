import tempfile
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Use same config as tests
POSTGRES_URI = "postgresql+psycopg2://test:test@localhost:5432/test"

# Create a temporary warehouse
with tempfile.TemporaryDirectory() as temp_dir:
    catalog = load_catalog(
        name="postgres",
        uri=POSTGRES_URI,
        warehouse=f"file://{temp_dir}/warehouse2",
    )
    
    # Create namespace
    catalog.create_namespace_if_not_exists("test_ns")
    
    # Create a simple table
    catalog.drop_table("test_ns.test_table2")
    data = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    table = catalog.create_table(
        "test_ns.test_table2",
        schema=data.schema,
    )
    
    print(f"Table created. Current snapshot: {table.current_snapshot()}")
    
    # Try overwrite
    print("\nCalling table.overwrite()...")
    table.overwrite(df=data, overwrite_filter="TRUE")
    
    print(f"After overwrite (no refresh). Current snapshot: {table.current_snapshot()}")
    
    # Refresh table
    table = table.refresh()
    print(f"After refresh. Current snapshot: {table.current_snapshot()}")
    
    # Try reloading the table
    table2 = catalog.load_table("test_ns.test_table2")
    print(f"After reload. Current snapshot: {table2.current_snapshot()}")
    
    # Scan the table
    result = table2.scan().to_arrow()
    print(f"\nTable contents: {result.to_pydict()}")


