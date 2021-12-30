# CSV Parser. 

Extracts a list of target columns using regex from a directory of CSV's. 

The directory structure of the raw files is as follows:
```
-- files
    -- CSV_1
    -- CSV_2
    -- CSV_3
```

after running the parser the structure will be as follows

```
-- files
    --- processed
        - timestamp_csv_1
        - timestamp_csv_2
        - timestamp_csv_3
    --- curated
        - timestamp_csv_1
        - timestamp_csv_2
        - timestamp_csv_3
    --- process_log
        - metadata.pq
```

Curated will hold the files with the extracted column names, and processed will be the raw files with a timestamp.

the `metadata.pq` file will hold a list of files that have been processed - and will be used to check for new files on each new ingestion cycle.


## Deployment Instructions


``` python
python3 -m venv .venv # one time
pip install -r requirements.txt # one time.
# activate venv each run.
source .venv/bin/activate # linux
.venv/scripts/activate.bat #windows/powershell.
python3 main.py path-to-files
```

## Assumptions


- column order is irrelevant.
- column names are irrelevant and will be their own name unless they differ from file to file (for example some files have ? marks and some fullstops, some have slightly differing wording) in such a case I've taken to naming the column by a standard name. 
- logs are fine to be overwritten for each run. 






