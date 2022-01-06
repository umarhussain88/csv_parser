@echo off
set /p path="Enter path to csv files: "
".\env\Scripts\activate" && python main.py %path%