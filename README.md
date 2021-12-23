# Excel to CSV Parser. 

Create a CSV from an excel file - pulling only the relevant columns by using partial string matching.


## Deployment Instructions


``` python
python3 -m venv .venv # one time.
pip install -r requirements.txt # one time.
python3 main.py
```

## Assumptions

- Sheetname is the 2nd sheet from each. If this is not correct this can be changed inside the `get_relevant_columns` function using the `trg_sheet` argument.

- That column names do not need to be changed.
- the column order is irrelevant. 




