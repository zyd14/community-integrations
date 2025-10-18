import tempfile
import pyarrow as pa
from pyiceberg.catalog import load_catalog

def main():
# Create a temporary warehouse
    with tempfile.TemporaryDirectory() as temp_dir:
        catalog = load_catalog(
            name="test",
            uri=f"sqlite:///{temp_dir}/catalog.db",
            warehouse=f"file://{temp_dir}/warehouse",
        )
        
        # Create namespace
        catalog.create_namespace_if_not_exists("test_ns")
        
        # Create a simple table
        data = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        table = catalog.create_table(
            "test_ns.test_table",
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
        table2 = catalog.load_table("test_ns.test_table")
        print(f"After reload. Current snapshot: {table2.current_snapshot()}")
        
        # Scan the table
        result = table2.scan().to_arrow()
        print(f"\nTable contents: {result.to_pydict()}")

if __name__ == "__main__":
    main()