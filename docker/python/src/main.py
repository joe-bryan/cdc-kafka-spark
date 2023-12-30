# Import needed libraries
from sqlalchemy import create_engine
import pandas as pd
import time
from database_config import pwd, uid, server, db, port
from sqlalchemy import text

engine = create_engine(f"postgresql://{uid}:{pwd}@{server}:{port}/{db}")

with engine.begin() as conn:
    df = pd.read_sql_query(text("Select * from public.rental"), conn)


for index, row in df.iloc[0:10, :].iterrows():
    mod = pd.DataFrame(row.to_frame().T)
    mod.to_sql(f"rental_streaming", engine, if_exists="append", index=False)
    print(
        "Row Inserted "
        + mod.rental_id.astype(str)
        + " "
        + mod.rental_date.astype(str).astype(str)
    )
    time.sleep(4)
