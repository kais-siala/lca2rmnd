from . import DATA_DIR

import os
import pandas as pd
import xarray as xr
import numpy as np
from glob import glob

class RemindDataCollection():
    """Manage access to the REMIND output file."""

    def __init__(self, scenario, filepath_remind_files=None):
        self.scenario = scenario

        filename = self.scenario + ".mif"
        filepath_remind_files = (filepath_remind_files or DATA_DIR / "remind" )

        fname = filepath_remind_files / "remind_{}.mif".format(scenario)

        if os.path.exists(fname):
            self.rmndpath = fname
        else:
            raise FileNotFoundError("No scenario output file found for scenario " + scenario)
        self.data = self.get_remind_data()

    def get_remind_data(self):
        """
        Read the REMIND csv result file and return an `xarray` with dimensions:
        * region
        * variable
        * year

        :return: a multi-dimensional array with Remind data
        :rtype: xarray.core.dataarray.DataArray

        """

        df = pd.read_csv(
            self.rmndpath, sep=";", index_col=["Region", "Variable", "Unit"]
        ).drop(columns=["Model", "Scenario"])
        if(len(df.columns == 20)):
            df.drop(columns=df.columns[-1], inplace=True)
        df.columns = df.columns.astype(int)

        df.reset_index(inplace=True)
        df = df.melt(id_vars=["Region", "Variable", "Unit"], var_name = "Year")

        return df


