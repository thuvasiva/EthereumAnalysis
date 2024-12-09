# EthereumAnalysis
An analysis of a dataset containing Ethereum transactions from August 2015 to January 2019, performed using Apache Spark.
The Spark programs read from an S3 bucket containing the whole dataset with approximately a size of 100GB. 
They then output the results into text files on to the same S3 bucket. These raw results were then used to create charts visualising the results.

Program 1: no_of_transactions_and_average_value_per_month.py

This program computes the number of transactions per month and the average value of transactions per month.

Result:
![image](https://github.com/user-attachments/assets/037c630b-f424-4cf7-ba0c-5638fa5a9ca5)

Program 2: top10_contracts_by_ether_received.py

This program determines the top 10 contracts by Ether Received.

Result:
![image](https://github.com/user-attachments/assets/03b67da4-fca7-4efc-b0e6-92005d99a335)

Program 3: top10_miners_by_size_of_blocks_mined.py

This program determines the top 10 miners by the size of blocks mined.

Result:
![image](https://github.com/user-attachments/assets/dece6d46-264e-4ea5-b38d-c572553fb8fd)

Program 4: average_gas_price_per_month.py

This program determines the average gas price in each month.

Result:
![image](https://github.com/user-attachments/assets/b90e19d8-c664-4617-8257-3ecd123b9d28)

Program 5: average_gas_used_for_contract_transactions.py

This program determines the average gas used for only transactions related to contracts per month.

Result:
![image](https://github.com/user-attachments/assets/84b7e428-7dbb-4973-809e-9e8043dd82cb)

Program 6: total_gas_used_by_top10_contracts_per_month.py

This program determines the total gas used by the top 10 contracts (determined in program 2) per month.

Result:
![image](https://github.com/user-attachments/assets/1944d3b2-0d7b-4b72-9db9-ddca8a49e740)
