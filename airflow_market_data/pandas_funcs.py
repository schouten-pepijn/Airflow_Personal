import pandas as pd

def convert_api_call_to_dataframe(api_data, column_names):
    # create dataframe
    dataframe = pd.DataFrame(api_data, columns=column_names)
    
    # convert to numeric
    dataframe = dataframe.apply(pd.to_numeric, errors='coerce')
    
    # convert to datetime
    dataframe[column_names[0]] = pd.to_datetime(dataframe[column_names[0]], unit='s')
    
    return dataframe


def select_columns(dataframe, column_names):
    # select columns
    return dataframe.loc[:, column_names]


def apply_ema(dataframe, column_name, window_size):
    # apply exponential moving average
    return dataframe[column_name].ewm(span=window_size).mean().round(2)