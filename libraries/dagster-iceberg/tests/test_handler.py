from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
from dagster import build_output_context
from dagster._core.storage.db_io_manager import TableSlice
from pyiceberg.catalog import Catalog

from dagster_iceberg.handler import IcebergBaseTypeHandler, WriteMode


class MockTypeHandler(IcebergBaseTypeHandler[pa.Table]):
    """Mock type handler for testing."""

    def to_data_frame(self, table, table_slice, target_type):
        return table.scan().to_arrow()

    def to_arrow(self, obj):
        return obj


@pytest.fixture
def mock_catalog():
    """Mock catalog for testing."""
    catalog = Mock(spec=Catalog)
    mock_table = Mock()
    mock_table.schema.return_value.model_dump.return_value = {
        "fields": [
            {"name": "col1", "type": "string"},
            {"name": "col2", "type": "int32"},
        ]
    }
    mock_snapshot = Mock()
    mock_snapshot.model_dump.return_value = {"snapshot_id": "test_snapshot"}
    mock_table.current_snapshot.return_value = mock_snapshot
    catalog.load_table.return_value = mock_table
    return catalog


@pytest.fixture
def table_slice():
    """Mock table slice for testing."""
    return TableSlice(
        table="test_table",
        schema="test_schema",
        partition_dimensions=[],
    )


@pytest.fixture
def mock_table_writer():
    with patch("dagster_iceberg._utils.io.table_writer") as mock_table_writer:
        mock_table_writer.return_value = None
        yield mock_table_writer


@pytest.fixture
def sample_data():
    """Sample PyArrow table for testing."""
    return pa.table({"col1": ["a", "b"], "col2": [1, 2]})


@pytest.mark.parametrize(
    [
        # Test case 1: Definition metadata only
        (
            {
                "write_mode": "append",
                "partition_spec_update_mode": "update",
                "schema_update_mode": "update",
                "table_properties": {"prop1": "value1"},
            },
            {},
            WriteMode.append,
            "update",
            "update",
            {"prop1": "value1"},
        ),
        # Test case 2: Output metadata overrides definition metadata
        (
            {
                "write_mode": "overwrite",
                "partition_spec_update_mode": "error",
                "schema_update_mode": "error",
                "table_properties": {"prop1": "value1"},
            },
            {"write_mode": "append"},
            WriteMode.append,
            "error",
            "error",
            {"prop1": "value1"},
        ),
    ],
)
def test_handle_output_metadata_passing(
    definition_metadata,
    output_metadata,
    expected_write_mode,
    expected_partition_spec_mode,
    expected_schema_mode,
    expected_table_properties,
    mock_catalog,
    table_slice,
    sample_data,
    mock_table_writer,
):
    """Test that metadata from definition and output contexts is passed correctly to table_writer."""

    # Create output context with metadata
    context = build_output_context(
        metadata=output_metadata,
        definition_metadata=definition_metadata,
        run_id="test_run_id",
    )

    # Create mock type handler
    handler = MockTypeHandler()

    # Call handle_output
    handler.handle_output(
        context=context,
        table_slice=table_slice,
        obj=sample_data,
        connection=mock_catalog,
    )

    # Verify table_writer was called
    mock_table_writer.assert_called_once()

    # Get the call arguments
    call_args = mock_table_writer.call_args

    # Verify all expected parameters were passed
    assert call_args.kwargs["dagster_run_id"] == "test_run_id"
    assert (
        call_args.kwargs["dagster_partition_key"] is None
    )  # No partitions in this test

    # Verify metadata-derived parameters
    assert call_args.kwargs["write_mode"] == expected_write_mode
    assert (
        call_args.kwargs["partition_spec_update_mode"] == expected_partition_spec_mode
    )
    assert call_args.kwargs["schema_update_mode"] == expected_schema_mode
    assert call_args.kwargs["table_properties"] == expected_table_properties


class TestHandleOutput:
    def test_append_output_metadata_overrides_definition_metadata(
        self, mock_catalog, table_slice, sample_data, mock_table_writer
    ):
        """Test that output metadata takes priority over definition metadata."""

        definition_metadata = {
            "write_mode": "overwrite",
            "partition_spec_update_mode": "error",
            "schema_update_mode": "error",
            "table_properties": {"def_prop": "def_value"},
        }

        output_metadata = {
            "write_mode": "append",
            "partition_spec_update_mode": "update",
            "schema_update_mode": "update",
            "table_properties": {"output_prop": "output_value"},
        }

        context = build_output_context(
            metadata=output_metadata,
            definition_metadata=definition_metadata,
            run_id="test_run_id",
        )

        handler = MockTypeHandler()

        with patch("dagster_iceberg._utils.io.table_writer") as mock_table_writer:
            mock_table_writer.return_value = None

            handler.handle_output(
                context=context,
                table_slice=table_slice,
                obj=sample_data,
                connection=mock_catalog,
            )

            # Verify output metadata takes priority
            call_args = mock_table_writer.call_args
            assert call_args.kwargs["write_mode"] == WriteMode.append
            assert call_args.kwargs["partition_spec_update_mode"] == "update"
            assert call_args.kwargs["schema_update_mode"] == "update"
            assert call_args.kwargs["table_properties"] == {
                "output_prop": "output_value"
            }



def test_handle_output_with_partitions(
    mock_catalog,
    sample_data,
):
    """Test handle_output with partitioned assets."""

    # Create table slice with partitions
    table_slice = TableSlice(
        table="test_table",
        schema="test_schema",
        partition_dimensions=[],
    )

    # Create context with partition key
    context = build_output_context(
        metadata={"write_mode": "append"},
        definition_metadata={"partition_spec_update_mode": "update"},
        run_id="test_run_id",
        partition_key="2022-01-01",
    )

    # Mock has_asset_partitions to return True
    context.has_asset_partitions = True

    handler = MockTypeHandler()

    with patch("dagster_iceberg._utils.io.table_writer") as mock_table_writer:
        mock_table_writer.return_value = None

        handler.handle_output(
            context=context,
            table_slice=table_slice,
            obj=sample_data,
            connection=mock_catalog,
        )

        # Verify partition key was passed
        call_args = mock_table_writer.call_args
        assert call_args.kwargs["dagster_partition_key"] == "2022-01-01"


def test_handle_output_metadata_priority(
    mock_catalog,
    table_slice,
    sample_data,
):
    """Test that output metadata takes priority over definition metadata."""

    definition_metadata = {
        "write_mode": "overwrite",
        "partition_spec_update_mode": "error",
        "schema_update_mode": "error",
        "table_properties": {"def_prop": "def_value"},
    }

    output_metadata = {
        "write_mode": "append",
        "partition_spec_update_mode": "update",
        "schema_update_mode": "update",
        "table_properties": {"output_prop": "output_value"},
    }

    context = build_output_context(
        metadata=output_metadata,
        definition_metadata=definition_metadata,
        run_id="test_run_id",
    )

    handler = MockTypeHandler()

    with patch("dagster_iceberg._utils.io.table_writer") as mock_table_writer:
        mock_table_writer.return_value = None

        handler.handle_output(
            context=context,
            table_slice=table_slice,
            obj=sample_data,
            connection=mock_catalog,
        )

        # Verify output metadata takes priority
        call_args = mock_table_writer.call_args
        assert call_args.kwargs["write_mode"] == WriteMode.append
        assert call_args.kwargs["partition_spec_update_mode"] == "update"
        assert call_args.kwargs["schema_update_mode"] == "update"
        assert call_args.kwargs["table_properties"] == {"output_prop": "output_value"}


def test_handle_output_adds_metadata_to_context(
    mock_catalog,
    table_slice,
    sample_data,
):
    """Test that handle_output adds metadata to the context."""

    context = build_output_context(
        metadata={},
        definition_metadata={},
        run_id="test_run_id",
    )

    handler = MockTypeHandler()

    with patch("dagster_iceberg._utils.io.table_writer"):
        handler.handle_output(
            context=context,
            table_slice=table_slice,
            obj=sample_data,
            connection=mock_catalog,
        )

        # Verify that metadata was added to context
        # This tests the second part of handle_output that adds metadata
        assert "table_columns" in context.output_metadata
        assert "snapshot_id" in context.output_metadata
