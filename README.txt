This fle package contains the following items:

* User_Manual.pdf

"ETL Pipeline - User Manual", containing:

- a summary of the requirements (chapter 1),
- a summary of the results of the exploratory data analysis (chapter 2),
- notes about design principles and assumptions (chapter 3),
- a short solution description (chapter 4),
- info about the SW environment (chapter 5),
- instructions about how to run the pipeline (chapter 6),
- a list of test cases (chapter 7),
- issues for future development (chapter 8)
- an annex containing details about the used AWS SSH server and MySQL database

* Exploratory_Data_Analysis.ipynb

A Jupyter Notebook file containing the exploratory data analysis

* Exploratory_Data_Analysis.html

A file in html format containing the exploratory data analysis

* daily_data.py

A Python 3 module which watchs the CSV files directory and dumps
the daily data into a repository

* aggregate_data.py

A Python 3 module which calculates and stores the aggregate data periodically

* config.py

A file containing the configurartion variables used in the two python modules

* provide_csv.sh

A simple shell script which facilitates testing

* requirements.txt

A file containing the required libraries
