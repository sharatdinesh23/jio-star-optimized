import pandas as pd 
from sqlalchemy import create_engine, text
import os 
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

#connecting the postgres
engine = create_engine(f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}')


#existing SQL files from local 
sql_file = r"D:\JioStar\Query\JioStar_Query_India Query.sql"

#starttime 
starttime = datetime.now()
print(f"Start Time: {starttime}")

#Reading the query from the file 
with open(sql_file, 'r') as file:
    query  = file.read()


#execute the Query 
with engine.connect() as conn:
    df = pd.read_sql_query(text(query), conn)
    # print(df.head())  # Display the first few rows of the DataFrame
    
#endtime
endtime = datetime.now()
print(f"End Time: {endtime}")
#difference between time 
print(f"Time Taken: {endtime - starttime}")

output_file = r"D:\JioStar\Output\JioStar_Query_India Query.xlsx"
df.to_excel(output_file, index=False)

print("Execution is completed")

engine.dispose()  # Close the connection pool

