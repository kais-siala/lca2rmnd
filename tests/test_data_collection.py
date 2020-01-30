import pytest
import pandas as pd

from lca2rmnd.data_collection import RemindDataCollection

rdc = RemindDataCollection("BAU")

def test_load_data():
    assert type(rdc.data) is pd.DataFrame
    assert set(["Region", "Variable", "Unit", "Year"]).issubset(rdc.data.columns)
    assert len(rdc.data)
    
