from df_preprocessing import DF_Preprocessing
from utils import get_db_description, save_df_dict

if __name__ == "__main__":
    print("Starting")
    db_description = get_db_description()
    df_prep = DF_Preprocessing(db_description)
    raw_df_dict = df_prep()
    # print('Saving DataFrames')
    # save_df_dict(raw_df_dict)
    # save_df_dict(df_dict, suffix='_clean')
    # TODO: Adding multiprocessing
    print("Done")

