# Data Conversion Tool

This repository contains a Python script designed to convert data from CSV format to JSON format. It's built with a focus on robustness, logging, and easy configuration.

---

### Features

- **Dynamic Conversion**: Automatically discovers datasets in a source directory or processes a specific list of datasets provided via command-line arguments.
- **Configurable Paths**: Uses environment variables (`.env` file) for source and target directories, ensuring flexibility and portability.
- **Robust Logging**: Includes a custom decorator to set up comprehensive logging, directing output to both a file (`logs/application.log`) and the console.
- **Schema-Based Processing**: Reads a `schemas.json` file to determine column names and their order for each dataset, ensuring data integrity.
- **Error Handling**: Implements thorough `try...except` blocks to handle common issues such as missing files, malformed JSON, and I/O errors.
- **Command-Line Interface**: Provides a simple command-line interface using `argparse` to specify which datasets to process.

---

### Prerequisites

- Python 3.8+
- The following Python libraries:
  - `pandas`
  - `python-dotenv`

You can install the required libraries using pip:

```bash
pip install pandas python-dotenv
```

Setup and Configuration

**1. Clone the repository:**

```Bash
git clone <repository-url>
cd <repository-name>
```
**2. Create a `.env` file:**

The script relies on environment variables for its source and target directories. Create a file named `.env` in the root directory of the project with the following content:

```Code snippet
SRC_BASE_DIR=data/source
TGT_BASE_DIR=data/target
```

- `SRC_BASE_DIR`: The path to the directory containing your dataset folders.

- `TGT_BASE_DIR`: The path where the converted JSON files will be stored.

**3. Prepare your data directory:**

The script expects a `schemas.json` file in your `SRC_BASE_DIR`. This file defines the column names and their order for each dataset.

The structure should look like this:

```
.
├── .env
├── main.py
└── data
    ├── source
    │   ├── schemas.json
    │   ├── dataset_a
    │   │   ├── part-00000.csv
│   │   └── ...
│   └── dataset_b
│       ├── part-00000.csv
│       └── ...
    └── target

```
An example of a `schemas.json` file:

```JSON
{
  "dataset_a": [
    {
      "column_name": "id",
      "column_position": 1,
      ...
    },
    {
      "column_name": "name",
      "column_position": 2,
      ...
    }
  ],
  "dataset_b": [
    {
      "column_name": "product_id",
      "column_position": 1,
      ...
    },
    {
      "column_name": "price",
      "column_position": 2,
      ...
    }
  ]
}
```
---
### Usage

The script can be run with or without command-line arguments.

**Option 1: Process All Datasets (No arguments)**

If you run the script without any arguments, it will automatically discover and process all datasets that match the pattern `SRC_BASE_DIR/*/part-*`.

```Bash
python app.py
```

**Option 2: Process Specific Datasets**

You can specify one or more dataset names using the `-ds_name` flag.

**For a single dataset:**

```Bash
python app.py -ds_name dataset_a
```
**For multiple datasets:**

```Bash
python app.py -ds_name dataset_a dataset_b
```

### Logging

The script will create a `logs` directory in the project's root. All log messages will be written to `logs/application.log` and also displayed in the console. This includes informational messages, warnings, and critical errors with full traceback details.

**Code Overview**

- `main.py`: The core script that handles the logic for data conversion.

- `logging_decorator`: A custom function decorator to manage logging.

- `main()`: The entry point of the application, responsible for argument parsing and environment variable validation.

- `convert_file()`: Orchestrates the conversion process for a single dataset.

- `process_files()`: A generator that yields file paths for a given dataset.

- `read_schema()`: Reads and validates the `schemas.json` file.

- `get_column_names()`: Extracts and sorts column names from the schema.

- `read_csv()`: Uses pandas to read a CSV file.

- `to_json()`: Uses pandas to convert a DataFrame to a JSON file.
