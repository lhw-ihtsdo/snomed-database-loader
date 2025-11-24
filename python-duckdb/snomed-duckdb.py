import argparse
import logging
import os
import re
import sys
import tempfile
import zipfile
from enum import Enum
from typing import Any, Callable, Match, Union

import duckdb

# Constants for DuckDB
IN_MEMORY_KEYWORD = ":memory:"
UI_PORT = 4213
UI_INSTALL_COMMAND = "INSTALL ui;"
UI_LOAD_COMMAND = "LOAD ui;"
UI_START_COMMAND = "CALL start_ui();"
COPY_OPTIONS = "HEADER, DELIMITER '\t', DATEFORMAT '%Y%m%d', NULL '\n'"

# Constants for logging messages
DEBUG_CONNECTION_CLOSED = "Connection closed"
DEBUG_FAILED_SQL = "SQL failed: COPY {} FROM '{}' ({})"
DEBUG_UI_EXT_LOADED = "UI extension loaded"
ERROR_IMPORT_FAILURE = "Failed to import '{}': {}"
ERROR_INVALID_PACKAGE = "Invalid package directory"
ERROR_SQL_EXEC_FAILED = "SQL execution failed: {}, {}"
ERROR_UI_INIT_FAILED = "UI initialization failed: {}"
ERROR_UI_START_FAILED = "UI start failed: {}"
ERROR_UNRECOGNISED_FORMAT = "Unrecognised file type: {}"
ERROR_ZIP_NOT_FOUND = "Zip file not found"
INFO_EXTRACTING_PACKAGE = "Extracting package '{}'"
INFO_IMPORT_SUCCESS = "Imported '{}'"
INFO_SQL_EXEC_SUCCESS = "Executed SQL: '{}'"
INFO_UI_RUNNING = "UI running at http://localhost:{}"
WARNING_NO_MATCHING_FILES = "No matching files for release type {}"
PROMPT_CLOSE = "Press <ENTER> to close"


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="""SNOMED-CT DuckDB Loader.
    This script imports SNOMED-CT RF2 files from an Edition package into DuckDB and
    launches a web-based UI for interactive queries.
    """
)
parser.add_argument(
    "--package", type=str, default="", help="Path to SNOMED-CT package location"
)
parser.add_argument(
    "--db",
    type=str,
    default="",
    help="Path to DuckDB database file (omit for in-memory mode)",
)
args = parser.parse_args()

package_location = args.package
DB_FILE = args.db
SQL_RESOURCES_PATH = os.path.join(
    os.path.dirname(__file__),
    "resources",
    "sql",
)


class ReleaseType(Enum):
    FULL = "Full"
    SNAPSHOT = "Snapshot"
    DELTA = "Delta"

    def __init__(self, full_name: str):
        self.short_code: str = full_name[0].lower()


def get_table_details(
    release_dir: str, release_type: ReleaseType
) -> list[tuple[str, str, str]]:
    # define the regex filter to match release file naming convention per RF2 specification:
    # https://confluence.ihtsdotools.org/display/DOCRELFMT/3.3.2+Release+File+Naming+Convention
    #
    # [FileType]_[ContentType]_[ContentSubType]_[CountryNamespace]_[VersionDate].[FileExtension]

    file_type = r"x?(sct|der)2"  # match group 1
    content_type = r"(\w+)"  # match group 2

    refset_id = r"(?:\d{6,18})?"
    summary = r"(\w+)?"  # match group 3
    rt = rf"{release_type.value}"
    language_code = r"(-[a-z-]{2,8})?"
    content_sub_type = rf"{refset_id}{summary}{rt}{language_code}"

    country_namespace = r"(?:(INT|[A-Z]{2})\d{7})"
    version_date = r"\d{8}"
    file_ext = r"txt"

    filter_regex = rf"^{file_type}_{content_type}_{content_sub_type}_{country_namespace}_{version_date}\.{file_ext}$"

    # align with table names used in the RVF:
    # https://github.com/IHTSDO/release-validation-framework/blob/master/src/main/resources/sql/create-tables-mysql.sql

    def extract_strategy(match: Match[str]) -> str:
        return (
            match.group(2)
            + "_"
            + release_type.short_code  # i.e. Concept_f for terminology data files
            if (match.group(1) == "sct" and match.group(3) != "OWLExpression")
            else match.group(3)
            + "refset_"
            + release_type.short_code  # Languagerefset_f for derivative work data files
        )

    extract_content_or_summary = (
        filter_regex,  # only import files that match the naming convention above (e.g. exclude *.json)
        extract_strategy,
    )

    drop_suffix_from_refsetdescriptor = (
        rf"RefsetDescriptorrefset_{release_type.short_code}",
        rf"RefsetDescriptor_{release_type.short_code}",
    )

    drop_suffix_from_simplerefset = (r"(Simple)(Refset)", r"\1")
    drop_suffix_from_associationreference = (r"(Association)(Reference)", r"\1")
    shorten_language_prefix = (r"(Language)", "Lang")
    shorten_referenceset = (r"ReferenceSet", "")
    add_underscore_to_relationship_concrete_values = (
        r"(Relationship)(Concrete)(Values)",
        r"\1_\2_\3",
    )
    add_underscore_to_stated_relationship = (r"(Stated)(Relationship)", r"\1_\2")

    regex_transformations: list[tuple[str, Union[str, Callable[[Match[str]], str]]]] = [
        extract_content_or_summary,
        drop_suffix_from_refsetdescriptor,
        drop_suffix_from_simplerefset,
        drop_suffix_from_associationreference,
        shorten_language_prefix,
        shorten_referenceset,
        add_underscore_to_relationship_concrete_values,
        add_underscore_to_stated_relationship,
    ]

    normalized_table_names: list[tuple[str, str, str]] = []
    for dirname, _, files in os.walk(os.path.join(release_dir, release_type.value)):
        for filename in files:
            if re.match(filter_regex, filename):
                normalized_filename = filename
                for pattern, replacement in regex_transformations:
                    normalized_filename = re.sub(
                        pattern, replacement, normalized_filename
                    )
                normalized_table_names.append(
                    (normalized_filename.lower(), dirname, filename)
                )

    # sort filenames to ensure that terminology data and concept files are loaded first
    def sort_key(x: tuple[str, str, str]) -> tuple[bool, bool, str]:
        return (
            "sct2" not in x[1],  # Prioritize terminology data files
            "concept" not in x[0],  # Prioritize concept files
            x[0],  # Finally, sort alphabetically by normalized name
        )

    normalized_table_names.sort(key=sort_key)
    return normalized_table_names


def validate_package_path(package_path: str) -> None:
    if not (
        package_path or os.path.isdir(package_path) or os.path.isfile(package_path)
    ):
        raise ValueError(ERROR_INVALID_PACKAGE)


class DuckDBClient:
    def __init__(self, db_path: str = IN_MEMORY_KEYWORD):
        self.conn = duckdb.connect(db_path)  # type: ignore
        try:
            self.conn.execute(UI_INSTALL_COMMAND)
            self.conn.execute(UI_LOAD_COMMAND)
            logging.debug(DEBUG_UI_EXT_LOADED)
        except Exception as e:
            logging.error(ERROR_UI_INIT_FAILED.format(e))

    def execute_sql_file(self, dirname: str, sql_filename: str) -> list[Any] | None:
        sql_filepath = os.path.join(dirname, sql_filename)
        try:
            with open(sql_filepath, "r") as file:
                output = self.conn.execute(file.read())
                logging.info(INFO_SQL_EXEC_SUCCESS.format(sql_filename))
                return output.fetchall()
        except Exception as e:
            logging.error(ERROR_SQL_EXEC_FAILED.format(sql_filepath, e))

    def execute_ddl(self, release_type: ReleaseType):
        ddl_filename = f"create_{release_type.value.lower()}_tables.sql"
        self.execute_sql_file(SQL_RESOURCES_PATH, ddl_filename)

    def start_ui(self):
        try:
            self.conn.execute(UI_START_COMMAND)
        except Exception as e:
            logging.error(ERROR_UI_START_FAILED.format(e))

    def import_text_file(
        self, table_name: str, dirname: str, rf2_filename: str
    ) -> None:
        rf2_filepath = os.path.join(dirname, rf2_filename)
        try:
            self.conn.execute(
                f"COPY {table_name} FROM '{rf2_filepath}' ({COPY_OPTIONS});"
            )
            logging.info(INFO_IMPORT_SUCCESS.format(rf2_filename))
        except Exception as e:
            match type(e):
                case duckdb.CatalogException:
                    logging.error(ERROR_UNRECOGNISED_FORMAT.format(rf2_filename, e))
                case _:
                    logging.error(ERROR_IMPORT_FAILURE.format(rf2_filename, e))
            logging.debug(
                DEBUG_FAILED_SQL.format(table_name, rf2_filepath, COPY_OPTIONS)
            )

    def close(self):
        self.conn.close()
        logging.debug(DEBUG_CONNECTION_CLOSED)


def validate_targetcomponentid(client: DuckDBClient, release_type: ReleaseType) -> None:
    sql_filename = f"validate_{release_type.value.lower()}_targetcomponentid.sql"

    result = client.execute_sql_file(SQL_RESOURCES_PATH, sql_filename)

    if result and len(result):
        logging.error(
            f"Found {len(result)} invalid targetComponentIds in the Association Refset {release_type} file"
        )
        quit()


if __name__ == "__main__":
    try:
        validate_package_path(package_location)
    except ValueError as e:
        logging.error(e)
        parser.print_help(sys.stderr)
        quit()

    with tempfile.TemporaryDirectory() as temp_dir:
        if package_location.endswith(".zip"):
            if not os.path.isfile(package_location):
                logging.error(ERROR_ZIP_NOT_FOUND)
                quit()
            logging.info(INFO_EXTRACTING_PACKAGE.format(package_location))
            with zipfile.ZipFile(package_location, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            package_location = os.path.join(temp_dir, os.listdir(temp_dir)[0])

        duckdb_client = DuckDBClient(DB_FILE)
        file_imported = False
        try:
            for release_type in [ReleaseType.FULL, ReleaseType.SNAPSHOT]:
                table_details = get_table_details(package_location, release_type)
                if not table_details:
                    logging.warning(WARNING_NO_MATCHING_FILES.format(release_type.name))
                else:
                    duckdb_client.execute_ddl(release_type)
                    for table_name, dirname, filename in table_details:
                        duckdb_client.import_text_file(table_name, dirname, filename)
                    file_imported = True
                    validate_targetcomponentid(duckdb_client, release_type)

            if file_imported:
                duckdb_client.start_ui()
                logging.info(INFO_UI_RUNNING.format(UI_PORT))
                input(PROMPT_CLOSE)
        finally:
            duckdb_client.close()
