Steps to Set Up and Run the ETL Code:

	1.	Install Required Dependencies:
	•	PySpark: pip install pyspark
	•	Pandas: pip install pandas
 
	2.	Prepare CSV Files:
	•	Place your CSV files for the two sales regions in a directory.
	•	Ensure that the CSV files contain these following columns:
	•	OrderId
	•	QuantityOrdered
	•	ItemPrice
	•	PromotionDiscount
 
	3.	Set Up Your File Paths:
	•	In the main() function, replace the region_a_file and region_b_file paths with the actual paths to your CSV files:

region_a_file = "path_to_order_region_a.csv"
region_b_file = "path_to_order_region_b.csv"


	4.	Run the Code:
	•	Save the code to a Python file .
	•	Run the script from the command line or terminal using:

    python sales_etl.py


	5.	Output:
	•	The ETL process will read the data from the CSV files, transform it, load the cleaned data into a SQLite database (sales_data.db)
	
