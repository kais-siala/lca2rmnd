from . import DATA_DIR
from .data_collection import RemindDataCollection

from rmnd_lca.electricity import Electricity
from rmnd_lca.utils import eidb_label
from rmnd_lca.geomap import Geomap

import brightway2 as bw
import pandas as pd


class ElectricityLCAReporting():
    """
    Report LCA scores for the REMIND electricity sector.

    The class assumes that the current brightway project contains
    all the relevant databases, including the characterization methods.
    Note that the convention for database names is "ecoinvent_<scenario>_<year>".

    :ivar scenario: name of the REMIND scenario, e.g., 'BAU', 'SCP26'.
    :vartype scenario: str
    :ivar years: years of the REMIND scenario to consider, between 2005 and 2150
        (5 year steps for up to 2060, then 10 year steps to 2110,
        and 20 years for the last two time steps).
        The corresponding brightway databases have to be part of the current project.
    :vartype year: array
    :ivar indicatorgroup: name of the set of indicators to
        calculate the scores for, defaults to ReCiPe Midpoint (H)
    :vartype source_db: str

    """
    def __init__(self, scenario, years,
                 indicatorgroup='ReCiPe Midpoint (H)'):
        self.years = years
        self.scenario = scenario

        self.methods = [m for m in bw.methods if m[0] == indicatorgroup]
        if not self.methods:
            raise ValueError("No methods found in the current brightway2 project for the following group: {}.".format(indicatorgroup))

        # check for brightway2 databases
        dbnames = set(["_".join(["ecoinvent", scenario, str(yr)]) for yr in years])
        missing = dbnames - set(bw.databases)
        if missing:
            raise ValueError("The following brightway2 databases are missing: {}".format(missing))
        rdc = RemindDataCollection(self.scenario)
        self.data = rdc.data[rdc.data.Year.isin(self.years)]
        self.geo = Geomap()

    def report_sectoral_LCA(self):
        """
        Report sectoral averages for the electricity sector based on the (updated)
        ecoinvent electricity market groups.

        :return: a dataframe with impacts for the REMIND electricity supply
            both as regional totals and impacts per kWh.
        :rtype: pandas.DataFrame

        """
        # low voltage consumers
        low_voltage = [
            "FE|Buildings|Electricity",
            "FE|Transport|Electricity"
        ]

        market = "market group for electricity, low voltage"

        df_lowvolt = self._sum_variables_and_add_scores(market, low_voltage)

        # medium voltage consumers
        medium_voltage = [
            "FE|Industry|Electricity",
            "FE|CDR|Electricity"
        ]

        market = "market group for electricity, medium voltage"
        df_medvolt = self._sum_variables_and_add_scores(market, medium_voltage)

        result = pd.concat([df_lowvolt, df_medvolt])
        result["total_demand"] = result["value"]\
                                 .groupby([result.Region, result.Year])\
                                 .transform("sum")
        result["total_score"] = result["total_score"]\
                                .groupby([result.Region, result.Year, result.method])\
                                .transform("sum")
        result["score_kWh"] = result["total_score"] / (result["total_demand"] * 2.8e11)


        return result[["Region", "Year", "method", "total_score", "score_kWh"]]

    def _sum_variables_and_add_scores(self, market, variables):
        """
        Sum the variables that belong to the market
        and calculate the LCA scores for all years,
        regions and methods.
        """
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
