# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    timesheet_data = [
    {"employee_id": 1, "date": "2025-02-20", "hours_worked": 8},
    {"employee_id": 2, "date": "2025-02-20", "hours_worked": 7},
    # Add more entries as needed
]

# Create a DataFrame from the timesheet data
timesheet_df = session.create_dataframe(timesheet_data)

# Write the DataFrame to a Snowflake table
timesheet_df.write.save_as_table("timesheet", mode="overwrite")

# Query the timesheet table
result = session.table("timesheet").select(col("employee_id"), col("date"), col("hours_worked"))
result.show()