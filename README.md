# Cryptocurrency Data Pipeline
* Overview: This project is a comprehensive data pipeline designed to fetch, process, and store real-time cryptocurrency data. The pipeline includes steps for data retrieval from the CoinMarketCap API, data preprocessing using Polars DataFrames, and the storage of the processed data in a MySQL database. Additionally, the project features an ETL (Extract, Transform, Load) mechanism, automated updates using APScheduler, and a failsafe mechanism with email notifications.

  
# ETL Process Explanation:


# 1) Data Extraction
The data extraction phase involves fetching data from the CoinMarketCap API using the fetch_data function. The API key is included in the headers for authentication. The retrieved data is then normalized into a Pandas DataFrame and converted to a Polars DataFrame for efficient processing.



# 2) Error Hadnling
In case an exception occurs during the data fetching process, an email notification is sent to the specified email address. This is achieved using the smtplib module to connect to an SMTP server and send an email with details about the error.



# 3) Data Cleaning and Transformation
After data extraction, unnecessary columns are dropped using the df.drop method. The remaining columns are then renamed for better readability. Subsequently, rounding is applied to selected numeric columns for more convinient analysis.



# 4) Conversion to Dictionary and Transposing
The Polars DataFrame is converted to a dictionary, and the values are extracted and stored in a list called values. The transpose_list is then created by transposing the original list, ensuring that each sub-list corresponds to a specific column in the database.



# 5) Data Loading to Database
The insert_values_to_database function iterates over the transposed list and executes an SQL INSERT statement for each row in the list. This process inserts the fetched and processed data into the MySQL database.



# 6) Data Update in Database
The update_database function is designed to update existing records in the database. It utilizes an SQL UPDATE statement, matching rows based on the cryptocurrency symbol and updating the relevant columns with new values.



# 7)  Automated Updates with APScheduler
The APScheduler library is used to schedule periodic updates of the database. The scheduler is configured to run the update_database function at hourly intervals. This ensures that the database stays up-to-date with the latest cryptocurrency data.











