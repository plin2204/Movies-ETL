# Movies-ETL
•	Create an ETL pipeline from raw data to a SQL database. <br />
•	Extract data from disparate sources using Python.<br />
•	Clean and transform data using Pandas.<br />
•	Use regular expressions to parse data and to transform text into numbers.<br />
•	Load data with PostgreSQL.

# Challenge 8
1.	Create a function that takes in three arguments: 
o	Wikipedia data
o	Kaggle metadata
o	MovieLens rating data (from Kaggle)
2.	Use the code from your Jupyter Notebook so that the function performs all of the transformation steps. Remove any exploratory data analysis and redundant code.
3.	Add the load steps from the Jupyter Notebook to the function. You’ll need to remove the existing data from SQL, but keep the empty tables.
4.	Check that the function works correctly on the current Wikipedia and Kaggle data.
5.	Document any assumptions that are being made. Use try-except blocks to account for unforeseen problems that may arise with new data.
  •	In mudole 8.3.7 to Remove Duplicate Rows: We made an assumption to keep columns that have less than "90%" null values. If we chose "95%", there'll will be less data to trim down. <br />
  

