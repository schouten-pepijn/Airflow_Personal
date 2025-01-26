from great_expectations.dataset import PandasDataset
from great_expectations.data_context import DataContext
import json

def ge_validation(dataframe):
    dataframe_ge = PandasDataset(dataframe)
    
    dtypes = ["datetime64[ns]", "float64", "float64", "float64", "float64"]
    cols = ["date", "open", "high", "low", "close"]
    
    for column, dtype in zip(cols, dtypes):
        dataframe_ge.expect_column_values_to_be_of_type(column=column, type_=dtype)
        
        if column != "date":
            dataframe_ge.expect_column_values_to_not_be_null(column=column)
            
    dataframe_ge.expect_column_values_to_be_unique(column="date")
    
    
    validation_results = dataframe_ge.validate()
    
    all_success = all(result["success"] for result in validation_results["results"])


    if all_success:
        pass
    else:
        failed_results = [result for result in validation_results["results"] if not result["success"]]
        with open("btc_data_validation_results.json", "w") as f:
            json.dump(failed_results, f, indent=2)
        raise Exception("Validation failed. Check validation_results.json for details.")
    
    
    