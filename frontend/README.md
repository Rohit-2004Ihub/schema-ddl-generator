# README – Bronze → Silver Auto-Mapping + DDL Generator (FastAPI + Gemini LLM)

## Overview

This project automates:

* Parsing Excel/CSV files
* Mapping Bronze → Silver table columns (AI-driven)
* Auto–creating new Silver columns
* Smart inference (geography, name extraction, derived values)
* Data type validation
* Removing invalid rows
* Exporting mapping + validation results to Excel
* Generating DDL + INSERT statements
* Change-log comparison between previous & current tables
* Full DDL generation (Databricks / Snowflake) using Gemini
* File download via FastAPI

The system uses Gemini 2.5 Flash for:

* Column semantic mapping
* Derived column generation
* Data-type inference
* Complete DDL generation

## 1. Installation

A. Install Python 3.10+

Download from: https://www.python.org/downloads/



## 2. Install Required Dependencies

Run:


pip install -r requirements.txt


If you don't have a requirements.txt, use:

pip install fastapi uvicorn python-dotenv pandas numpy openpyxl xlrd xlsxwriter langchain-google-genai


## 3. Environment Variables

Create a .env file:

GOOGLE_API_KEY=your_gemini_api_key_here


## 4. Directory Structure

project/
│── app/
│   ├── agents/
│   │   └── schema_agent.py
│   ├── services/
│   │   └── ddl_generator.py
│   ├── mapping_engine.py
│   └── main.py
│
│── outputs/
│── .env
│── requirements.txt
│── README.md


## 5. Running the FastAPI Server

Start the app:


uvicorn app.main:app --reload


## 6. API Endpoints

A. Generate Mapping + DDL


POST /invoke_mapping


Request body:


{
  "bronze_file": "<file-bytes>",
  "bronze_filename": "bronze.xlsx",
  "silver_file": null,
  "silver_filename": "",
  "bronze_name": "bronze_table",
  "silver_name": "silver_table"
}


Response includes:

* Mapping file name
* DDL scripts
* Column mapping metadata

B. Download Mapping File


GET /download/{file_name}


## 7. What AI Does in This Pipeline?

### Bronze → Silver Mapping

AI identifies best matching Silver columns using:

* Semantic understanding
* Sample values
* Column name similarity

### New Column Generation

If Silver columns include fields like:

* country
* continent
* first_name
* last_name

AI fills values row-by-row using:

* Geography inference
* Name parsing
* Numeric/Category inference

### Data Type Validation

AI predicts “expected types” such as:

* int
* float
* datetime
* str

Invalid rows → removed.

## 8. Outputs

The system saves a mapping Excel file:

outputs/mapping_.xlsx

It includes sheets:

* Mapping
* BronzeData
* SilverData
* RemovedRows (if validation failed)

## 9. Full DDL Generation (Databricks / Snowflake)

Gemini generates a fully structured:

* CREATE TABLE
* One INSERT INTO (…) VALUES (…) containing all rows
* Cleaned + sanitized column names

## 10. Change Log Engine

When the same table is uploaded again:

* INSERT rows → new entries
* DELETE rows → removed entries
* UPDATE rows → changed values

Stored in:

```
TABLE_HISTORY[target]
```

## 11. Troubleshooting

Parsing error:

```
Unsupported or corrupt file format
```

Fix: ensure file is .csv or .xlsx.

LLM JSON parse error

Common when Gemini outputs natural language.

Enable clean_llm_output() to strip backticks.

GOOGLE_API_KEY missing

Ensure .env file exists.

## 12. Tech Stack

| Component       | Tool/Framework          |
|-----------------|-------------------------|
| Framework      | FastAPI                |
| LLM            | Gemini 2.5 Flash       |
| Data           | Pandas, NumPy          |
| Storage        | Auto-created outputs/  |
| File Format    | Excel (XLSX)           |
| Validation     | AI + Pandas            |
| Change Tracking| Custom engine          |
| DDL Generation | AI-driven              |

## 13. Key Features Summary

* Automatic Bronze → Silver Mapping
* AI-driven smart column creation
* AI-driven data-type inference
* Automatic row removal for invalid data
* Export to XLSX
* Full DDL + INSERT generation
* Change log for any table updates
* Supports both Databricks & Snowflake