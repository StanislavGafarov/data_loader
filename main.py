from df_preprocessing import DF_Preprocessing
from utils import get_db_description

if __name__ == "__main__":
    print("Starting")
    db_description = get_db_description()
    df_prep = DF_Preprocessing(db_description)
    # TODO: Saving DataFrames if it necessary
    # TODO: Loading cleaning DataFrames into local db
    # TODO: Adding multiprocessing
    print("Done")

