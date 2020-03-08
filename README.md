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
5.	Document any assumptions that are being made. Use try-except blocks to account for unforeseen problems that may arise with new data. <br />
  •	In Module 8.3.7 to Remove Duplicate Rows: We made an assumption to keep columns that have less than "90%" null values. If we chose "95%", there'll will be less data to trim down. <br />
  •	In Module 8.3.10 to Parse the Box Office Data: still there're some box office not being extracted. We could also handle citation reference like budget.<br />
  •	In Module 8.4.1 to Merge Wikipedia and Kaggle Metadata: We assumed scatter plot to compare two sets of data, but scatter plot doesn't show null values.<br />
  •	In Module 8.4.2 to Transform and Merge Rating Data: We used rating counts directly, instead of looking into other statistics like the mean and median rating for each movie. <br />
  •	In Module 8.4.2 to Transform and Merge Rating Data: We filled out the missing data from rating_counts with 0 directly.<br />
  
