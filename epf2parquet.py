#!/usr/bin/env python3

# simple util to transform an uncompressed EPF file on stdin into a parquet file
#
# Setup:
# python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt
#
# Usage: ./epf2parquet.py <output_directory>
# pbzip2 -cd application.tbz | python ./epf2parquet.py out/application


import sys
import re
import pandas as pd
import dask.dataframe as dd
import numpy as np
from tqdm import tqdm

pat = r"^([a-z]+)([0-9]{4})([0-9]{2})([0-9]{2})\/([a-z_]+)"

def read_header_info(file=sys.stdin):
    """
    Extracts header information from the EPF file before processing the data rows.
    Returns a dictionary with header metadata and the position to resume reading.
    """
    buffer = ""
    delimiter = '\x02\n'
    column_names = []
    types = []
    primary_keys = []
    date = None
    name = None
    group = None
    export_mode = None

    # Read first line for column names
    while True:
        chunk = file.read(1)
        if not chunk:  # EOF
            return None  # No header found
        buffer += chunk
        if buffer.endswith(delimiter):
            line = buffer.replace('\x02\n', '')
            if '#' in line:
                [tarjunk, header_text] = line.split('#', 1)
                column_names = header_text.split('\x01')
                m = re.match(pat, tarjunk)
                if m:
                    group = m.group(1)
                    name = m.group(5)
                    date = [m.group(2), m.group(3), m.group(4)]
                buffer = ""
                break
            else:
                buffer = ""
                continue  # Skip lines without header marker

    # Read metadata lines
    header_complete = False
    line_count = 1

    while not header_complete:
        buffer = ""
        while True:
            chunk = file.read(1)
            if not chunk:  # EOF
                return None  # Incomplete header
            buffer += chunk
            if buffer.endswith(delimiter):
                break

        line = buffer.replace('\x02\n', '')
        line_count += 1

        if line.startswith('#'):
            v = line[1:].split(':', 1)
            if len(v) == 2:
                if v[0] == 'primaryKey':
                    primary_keys = v[1].split('\x01')
                elif v[0] == 'dbTypes':
                    types = v[1].split('\x01')
                elif v[0] == 'exportMode':
                    export_mode = v[1]
        else:
            # First data line reached - return to this position to start data reading
            # We'll put the first data line back so it can be processed with the rest
            header_complete = True

            # Store the first data line for later processing
            first_data_line = line

    return {
        'column_names': column_names,
        'types': types,
        'primary_keys': primary_keys,
        'date': date,
        'name': name,
        'group': group,
        'export_mode': export_mode,
        'first_data_line': first_data_line
    }

def process_epf_line(line, column_names):
    """
    Process a single EPF line and return a dictionary with the data.
    This function is pure and doesn't modify any external state.
    """
    line = line.replace('\x02\n', '')

    fields = line.split('\x01')
    if len(fields) == len(column_names):
        row = {column_names[index]: fields[index] for index in range(len(column_names))}
        return row
    else:
        print(f"Skipped line due to bad formatting - expected {len(column_names)} fields, got {len(fields)}")
        return None

def parse_epf(header_info, file=sys.stdin):
    """
    Generator to parse EPF file with the header info already extracted.
    """
    column_names = header_info['column_names']

    # Process the first data line that we already read during header parsing
    if 'first_data_line' in header_info and header_info['first_data_line']:
        row = process_epf_line(header_info['first_data_line'] + '\x02\n', column_names)
        if row:
            yield row

    # Process the rest of the file
    buffer = ""
    delimiter = '\x02\n'
    while True:
        chunk = file.read(1)
        if not chunk:  # EOF
            if buffer:
                row = process_epf_line(buffer, column_names)
                if row:
                    yield row
            break
        buffer += chunk
        if buffer.endswith(delimiter):
            row = process_epf_line(buffer, column_names)
            if row:
                yield row
            buffer = ""

def get_safe_type_converter(db_type):
    """
    Returns a safe type conversion function based on the EPF database type
    that can handle empty strings and invalid data.
    """
    if db_type in ('INTEGER', 'BIGINT'):
        def convert_int(value):
            try:
                return pd.NA if value == '' else int(value)
            except (ValueError, TypeError):
                return pd.NA
        return convert_int

    elif db_type in ('REAL', 'DOUBLE'):
        def convert_float(value):
            try:
                return pd.NA if value == '' else float(value)
            except (ValueError, TypeError):
                return pd.NA
        return convert_float

    elif db_type == 'BOOLEAN':
        def convert_bool(value):
            if value == '':
                return pd.NA
            try:
                return bool(int(value))
            except (ValueError, TypeError):
                return pd.NA
        return convert_bool

    else:  # Default to string
        return lambda x: x  # No conversion needed

def stream_to_parquet_with_dask(header_info, output_file, file=sys.stdin, batch_size=10000):
    """
    Convert stream to parquet using Dask for better performance
    """
    # Create type converters based on header info
    converters = {}
    if header_info['types'] and len(header_info['types']) == len(header_info['column_names']):
        for i, col in enumerate(header_info['column_names']):
            converters[col] = get_safe_type_converter(header_info['types'][i])

    # Collect batches first
    all_batches = []
    total_rows = 0

    # Create iterator with progress bar
    stream = parse_epf(header_info, file)
    with tqdm(desc=f"Processing {header_info['name']}", unit="row", colour="green") as pbar:
        while True:
            # Get a batch of rows
            current_batch = []
            for _ in range(batch_size):
                try:
                    row = next(stream)
                    # Apply type conversion and clean values
                    cleaned_row = {}
                    for key, value in row.items():
                        if key in converters:
                            cleaned_row[key] = converters[key](value)
                        else:
                            if not isinstance(value, (str, int, float, bool, type(None))):
                                cleaned_row[key] = str(value)
                            else:
                                cleaned_row[key] = value
                    current_batch.append(cleaned_row)
                except StopIteration:
                    break

            # If we got some rows, add them
            if current_batch:
                # Convert batch to DataFrame and add to list
                df = pd.DataFrame(current_batch)
                all_batches.append(df)

                total_rows += len(current_batch)
                pbar.update(len(current_batch))
            else:
                # No more data
                break

    # If we collected data
    if all_batches:
        # Combine all batches into one DataFrame first
        combined_df = pd.concat(all_batches, ignore_index=True)

        # Define proper pandas dtypes based on EPF types
        dtype_map = {}
        if header_info['types'] and len(header_info['types']) == len(header_info['column_names']):
            for i, col in enumerate(header_info['column_names']):
                db_type = header_info['types'][i]
                if col in combined_df.columns:  # Make sure column exists
                    if db_type in ('INTEGER', 'BIGINT'):
                        dtype_map[col] = 'Int64'  # Pandas nullable integer
                    elif db_type in ('REAL', 'DOUBLE'):
                        dtype_map[col] = 'float64'
                    elif db_type == 'BOOLEAN':
                        dtype_map[col] = 'boolean'
                    else:
                        dtype_map[col] = 'string'

        # Apply pandas dtypes - using nullable types to handle missing values
        for col, dtype in dtype_map.items():
            try:
                combined_df[col] = combined_df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not convert column '{col}' to {dtype}: {e}")
                # Fall back to object type
                combined_df[col] = combined_df[col].astype('object')

        # Create a Dask DataFrame with a sensible number of partitions
        n_partitions = min(max(1, len(all_batches)), 16)  # Cap at 16 partitions
        ddf = dd.from_pandas(combined_df, npartitions=n_partitions)

        # Write to parquet
        ddf.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            write_index=False
        )

        print(f"Processed {total_rows} rows into {output_file}")

        # Return metadata for informational purposes
        return {
            'total_rows': total_rows,
            'file': output_file,
            'name': header_info['name'],
            'date': header_info['date'],
            'columns': header_info['column_names'],
            'primary_keys': header_info['primary_keys']
        }
    else:
        print("No data processed")
        return None

# Use the function
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./epf2parquet.py <output_file>")
        sys.exit(1)

    # First read header information
    header_info = read_header_info()
    if not header_info:
        print("Error: Could not read header information from the EPF file")
        sys.exit(1)

    print(f"Processing EPF file: {header_info['name']} from {'-'.join(header_info['date'])}")
    print(f"Found {len(header_info['column_names'])} columns, {len(header_info.get('primary_keys', [])) or 0} primary keys")

    # Then process the data
    result = stream_to_parquet_with_dask(
        header_info=header_info,
        output_file=sys.argv[1],
        batch_size=50000
    )

    if result:
        print(f"Successfully converted {result['name']} to Parquet ({result['total_rows']} rows)")
