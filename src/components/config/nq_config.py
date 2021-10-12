import pathlib
import os

import src

ROOT = pathlib.Path(src.__file__).resolve().parent
RUN_EXTRACTION = True
RAW_INPUT_FILE_PATH = ROOT / "data/NQ/sample_nq_100.jsonl"  #  sample_nq_100.jsonl | v1.0-simplified_simplified-nq-train.jsonl
EXTRACTED_FILES_OUTPUT_FOLDER = ROOT / "output/NQ/extracted_files"
CURATED_FILES_OUTPUT_FOLDER = ROOT / "output/NQ/curated_files"

if not os.path.exists(EXTRACTED_FILES_OUTPUT_FOLDER):
    os.makedirs(EXTRACTED_FILES_OUTPUT_FOLDER)
if not os.path.exists(CURATED_FILES_OUTPUT_FOLDER):
    os.makedirs(CURATED_FILES_OUTPUT_FOLDER)

NQExtactor_kwargs = {
    "raw_nq_json_file": RAW_INPUT_FILE_PATH,
    "out_dir": EXTRACTED_FILES_OUTPUT_FOLDER,
    "n_rows": -1,  # Number of examples that will be extracted from the json file. Set to -1 fo all.
    "drop_no_long_answer": True,
    "chunk_size": 10000,  # Every `chunk_size` extracted examples are grouped together and saved as a CSV.
}

NQCurator_kwargs = {
    "input_dir": EXTRACTED_FILES_OUTPUT_FOLDER,
    "output_dir": CURATED_FILES_OUTPUT_FOLDER,
    "extract_text": True,
    "extract_tables": True,
}
