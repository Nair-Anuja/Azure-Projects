# FormulaOneProject
This project ingests data from Ergast API ,which hosts formula 1 data, into the Azure DataLake Storage. The transformations were done on  DataBricks using pyspark.

The data from the API is in json format which had to be flattened.
Azure service principal and key vault were used to connect the workspace to the azure dala lake storage.
