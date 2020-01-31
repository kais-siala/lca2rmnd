from . import DATA_DIR
from .data_collection import RemindDataCollection

from rmnd_lca.electricity import Electricity
from rmnd_lca.utils import eidb_label
from rmnd_lca.geomap import Geomap

import brightway2 as bw
import pandas as pd


class ElectricityLCAReporting():
    def __init__(self, years, scenario,
                 indicatorgroup='ReCiPe Midpoint (H)'):
        self.years = years
        self.scenario = scenario

        self.methods = [m for m in bw.methods if m[0] == indicatorgroup]
        rdc = RemindDataCollection(self.scenario)
        self.data = rdc.data[rdc.data.Year.isin(self.years)]

        self.geo = Geomap()

    def reportSectoralLCA(self):
        # low voltage consumers
        low_voltage = [
            "FE|Buildings|Electricity",
            "FE|Transport|Electricity"
        ]

        market = "market group for electricity, low voltage"

        df_lowvolt = self.sum_variables_and_add_scores(market, low_voltage)

        # medium voltage consumers
        medium_voltage = [
            "FE|Industry|Electricity",
            "FE|CDR|Electricity"
        ]

        market = "market group for electricity, medium voltage"
        df_medvolt = self.sum_variables_and_add_scores(market, medium_voltage)

        result = pd.concat([df_lowvolt, df_medvolt])
        result["total_demand"] = result["value"]\
                                 .groupby([result.Region, result.Year])\
                                 .transform("sum")
        result["total_score"] = result["total_score"]\
                                .groupby([result.Region, result.Year, result.method])\
                                .transform("sum")
        result["score_ej"] = result["total_score"] / result["total_demand"]


        return result[["Region", "Year", "method", "total_score", "score_ej"]]

    def sum_variables_and_add_scores(self, market, variables):
        df = self.data[self.data.Variable.isin(variables)]\
                 .groupby(["Region", "Year"])\
                 .sum()
        df.reset_index(inplace=True)
        df["market"] = market

        # add methods dimension & score column
        methods_df = pd.DataFrame({"method": self.methods, "market": market})
        df = df.merge(methods_df)
        df["score"] = 0.

        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.scenario, year))
            for region in df.Region.unique():
                # import ipdb;ipdb.set_trace()
                # find activity
                act = [a for a in db if a["name"] == market and
                       a["location"] == region][0]
                # create first lca object
                lca = bw.LCA({act: 1}, method=df.method[0])
                # build inventories
                lca.lci()

                df_slice = df[(df.Year == year) &
                              (df.Region == region)]

                def get_score(method):
                    lca.switch_method(method)
                    lca.lcia()
                    return lca.score

                df_slice["score"] = df_slice.apply(
                    lambda row: get_score(row["method"]), axis=1)
                df.update(df_slice)

        df["total_score"] = df["score"] * df["value"] * 2.8e11
        return df
