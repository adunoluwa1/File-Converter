import os
import json
import glob
import logging
from argparse import ArgumentParser
from typing import List,Dict,Generator,Optional
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def logging_decorator(orig_func):
    """
    Decorator function to create a logger that logs to a file and the console.

    The logger is configured to save logs to 'logs/application.log' and also
    print them to the standard output. The wrapper function passes the logger
    object as the first argument to the decorated function.

    Args:
        orig_func (function): The function to be decorated.

    Returns:
        function: The wrapper function that adds logging capabilities.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_dir = "logs"
    os.makedirs(log_dir,exist_ok=True)
    log_file_path = os.path.join(log_dir,"application.log")

    file_handler = logging.FileHandler(log_file_path)
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    def wrapper_function(*args,**kwargs):
        """Wrapper function to execute original function and with logging functionalities"""
        return orig_func(logger,*args,**kwargs)
    return wrapper_function

@logging_decorator
def main(logger, ds_name:Optional[str|list]):
    """
    Main function for managing application inputs.

    This function retrieves the source and target base directories from
    environment variables and processes a list of dataset names. If no
    dataset name is provided as a command-line argument, it discovers
    all available datasets in the source directory.

    Args:
        logger (logging.Logger): The logger object provided by the decorator.
        ds_name (Optional[str | list]): The name(s) of the dataset(s) to process,
                                        provided as a command-line argument.
                                        Can be a single string or a list of strings.
    
    Raises:
        NameError: If the environment variables 'SRC_BASE_DIR' or 'TGT_BASE_DIR'
                   are not set.
        FileNotFoundError: If no files are found in the source directory when
                           no dataset name is specified.
        Exception: For any unhandled exceptions that occur during processing.
    """
    try:
        src_base_dir:str = os.environ.get("SRC_BASE_DIR")
        tgt_base_dir:str = os.environ.get("TGT_BASE_DIR")
        if not tgt_base_dir or not src_base_dir:
            raise NameError("missing source or target base directory")
    except NameError as e:
        logger.critical(f"Missing Environment Variables: {e}",exc_info=True)
        raise e

    try:
        if not ds_name:
            logger.warning("No dataset name provided as runtime argument")

            ds_names_dir = glob.glob(f"{src_base_dir}/*/part-*")
            if len(ds_names_dir) == 0:
                raise FileNotFoundError(f"No file exists in {src_base_dir} with glob pattern:\
                                         '{src_base_dir}/*/part-*'")
            ds_names = [os.path.basename(os.path.split(ds_dir)[0]) for ds_dir in ds_names_dir]
        else:
            ds_names = ds_name
            logger.info(f"File converter run with runtime arguments: {ds_names}")

        for ds_name in ds_names:
            convert_file(logger,src_base_dir,tgt_base_dir,ds_name)

    except Exception as e:
        logger.critical(f"An unhandled exception occurred: {e}",exc_info=True)
        raise e

def convert_file(logger:Optional[logging.Logger], src_base_dir:str, tgt_base_dir:str, ds_name:str):
    """
    Reads a CSV file, processes it, and converts it to JSON format.

    This function orchestrates the reading of the schema, retrieving column names,
    reading the CSV files into a pandas DataFrame, and then converting the DataFrame
    to a JSON file in the target directory.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        src_base_dir (str): The base directory where source CSV files are located.
        tgt_base_dir (str): The base directory where the output JSON files will be saved.
        ds_name (str): The name of the dataset to be processed.

    Raises:
        Exception: Catches and logs any unhandled exceptions that occur during
                   the file conversion process.
    """
    try:
        schema:dict = read_schema(logger, src_base_dir)
        col_name:list = get_column_names(logger,schema,ds_name)
        for csv_file in process_files(logger,src_base_dir,ds_name):
            df = read_csv(logger,csv_file,col_name)
            if to_json(logger,df,tgt_base_dir,csv_file,ds_name):
                logger.info(f"Successfully processed {ds_name} at {csv_file} to JSON")
            else:
                logger.error(f"Error processing {ds_name} at {csv_file} to JSON")
    except Exception as e:
        logging.critical(f"An unhandled exception occurred: {e}", exc_info=True)
        raise

def process_files(logger:Optional[logging.Logger], src_base_dir:str, table:str)\
      -> Generator[str,None,None]:
    """
    Generates file paths for CSV files within a specific dataset directory.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        src_base_dir (str): The base directory containing the dataset folders.
        table (str): The name of the dataset (table) to search within.

    Yields:
        str: The full path to a CSV file to be processed.

    Raises:
        ValueError: If no files are found in the specified search directory.
    """
    search_dir = f"{src_base_dir}/{table}/part-*"
    logger.info(f"Search directory created: {search_dir}")
    csv_files = glob.glob(search_dir)
    if len(csv_files) == 0:
        logger.error(f"No file found in the search directory with the pattern: {search_dir}",exc_info=True)
        raise ValueError

    for file in csv_files:
        logger.info(f"Successfully found {file} in '{src_base_dir}/{table}'")
        yield file

def read_schema(logger:Optional[logging.Logger], src_base_dir:str) -> Dict[str,List]:
    """
    Reads a 'schemas.json' file and returns its content as a dictionary.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        src_base_dir (str): The base directory where the schema file is located.

    Returns:
        Dict[str, List]: The content of the 'schemas.json' file.

    Raises:
        FileNotFoundError: If the 'schemas.json' file is not found.
        json.JSONDecodeError: If the 'schemas.json' file is malformed and cannot
                              be parsed.
    """
    schema_dir = os.path.join(src_base_dir,"schemas.json")
    try:
        with open(schema_dir) as schema:
            schemas:dict = json.load(schema)
            logger.info(f"Successfully loaded schema in {schema_dir}")
        return schemas
    except FileNotFoundError as e:
        logger.error(f"No file found in schema directory: {schema_dir}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Unable to parse JSON file in {schema_dir}: {e}")
        raise

def get_column_names(logger:Optional[logging.Logger], schema:Dict[str,List],table:str,sorting_key:str='column_position')\
      -> List[str]:
    """
    Extracts and sorts column names for a given table from a schema dictionary.

    The column names are sorted based on a specified key, with 'column_position'
    as the default.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        schema (Dict[str, List]): The schema dictionary containing table definitions.
        table (str): The name of the table to retrieve column names for.
        sorting_key (str, optional): The key to sort the columns by. Defaults to
                                     'column_position'.

    Returns:
        List[str]: A list of sorted column names for the specified table.

    Raises:
        KeyError: If the table name, sorting key, or 'column_name' is not found
                  in the schema dictionary.
        Exception: For any other unhandled exceptions during schema parsing.
    """
    try:
        sorted_schema = sorted(schema[table],key= lambda col: col[sorting_key])
        col_name = [col_detail['column_name'] for col_detail in sorted_schema]
        logger.info(f"Successfully retrieved column names for '{table}' from schema")
        return col_name
    except KeyError as e:
        logger.error(f"Malformed schema: '{sorting_key}' or 'column_name' key does not exist in schema dictionary: {e}",exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"An unhandled exception occurred while parsing schema: {e}")
        raise

def read_csv(logger:Optional[logging.Logger], file_path:str,column_names:List[str]) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame using the provided column names.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        file_path (str): The full path to the CSV file to be read.
        column_names (List[str]): A list of column names to assign to the DataFrame.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the CSV file.

    Raises:
        pd.errors.ParserError: If pandas encounters an error while parsing the CSV.
        Exception: For any unhandled exceptions that occur during file reading.
    """
    try:
        df = pd.read_csv(file_path,names=column_names)
        logger.info(f"Successfully read csv file from {file_path} to DataFrame")
        return df
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing csv file from {file_path}: {e}")
        raise
    except Exception as e:
        logger.critical(f"An unhandled exception occurred while reading file from {file_path}: {e}")
        raise

def to_json(logger:Optional[logging.Logger], df:pd.DataFrame, tgt_base_dir:str, csv_file_path:str, ds_name:str) -> bool:
    """
    Converts a pandas DataFrame to a JSON file and saves it in the target directory.

    The output JSON file is named after the original CSV file.

    Args:
        logger (Optional[logging.Logger]): The logger object for logging messages.
        df (pd.DataFrame): The DataFrame to be converted to JSON.
        tgt_base_dir (str): The base directory for saving the JSON output.
        csv_file_path (str): The full path of the original CSV file (used for naming).
        ds_name (str): The name of the dataset, used to create a subdirectory.

    Returns:
        bool: True if the conversion was successful, False otherwise.

    Raises:
        IOError: If an error occurs during file I/O operations (e.g., writing the JSON file).
        Exception: For any unhandled exceptions during the conversion process.
    """
    try:
        json_dir = os.path.join(tgt_base_dir,ds_name)
        os.makedirs(json_dir,exist_ok=True)
        json_file_name:str = f"{os.path.splitext(os.path.basename(csv_file_path))[0]}.json"
        logger.info(f"Successfully created directory for json file: {json_dir}")
        df.to_json(json_file_name,orient='records',lines=True)
        logger.info(f"Successfully converted DataFrame to JSON at {json_file_name}")
        return True
    except IOError as e:
        logger.error(f"Error converting DataFrame to JSON: {e}",exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"An unhandled exception occurred while parsing DataFrame to JSON:\
                         {e}",exc_info=True)
        raise

if __name__ == "__main__":
    parser = ArgumentParser(description="")
    parser.add_argument("-ds_name",nargs="+",type=str,help="Dataset name",default=None)
    args = parser.parse_args()
    main(ds_name=args.ds_name)
