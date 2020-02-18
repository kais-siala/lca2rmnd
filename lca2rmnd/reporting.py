from . import DATA_DIR
from .data_collection import RemindDataCollection
from .activity_select import ActivitySelector

from rmnd_lca import Electricity, Geomap, InventorySet
from rmnd_lca.utils import eidb_label

import brightway2 as bw
import pandas as pd
from bw2data.backends.peewee.proxies import Activity

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
                 indicatorgroup='ReCiPe Midpoint (H) V1.13'):
        self.years = years
        self.scenario = scenario
        self.selector = ActivitySelector()
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

        df["total_score"] = df["score"] * df["value"] * 2.8e11 # EJ -> kWh
        return df

    def report_tech_LCA(self, year):
        """
        For each REMIND technology, find a set of activities in the region.
        Use ecoinvent tech share file to determine the shares of technologies
        within the REMIND proxies.
        """

        tecf = pd.read_csv(DATA_DIR/"powertechs.csv", index_col="tech")
        tecdict = tecf.to_dict()["mif_entry"]

        db = bw.Database("_".join(["ecoinvent", self.scenario, str(year)]))

        regions = self._get_rmnd_regions()
        result = self._cartesian_product({
            "region": regions,
            "tech": list(tecdict.keys()),
            "method": self.methods
        }).sort_index()

        for region in regions:
            # read the ecoinvent techs for the entries
            shares = self.supplier_shares(db, region)

            for tech, acts in shares.items():
                # calc LCA
                lca = bw.LCA(acts, self.methods[0])
                lca.lci()

                for method in self.methods:
                    lca.switch_method(method)
                    lca.lcia()
                    result.at[(region, tech, method), "score"] = lca.score

        return result

    def _cartesian_product(self, idx):
        """
        Create a DataFrame with an Index being the cartesian
        product from a dictionary of lists.

        From: https://stackoverflow.com/questions/58242078/cartesian-product-of-arbitrary-lists-in-pandas/58242079#58242079

        :parm idx: dictionary with keys being the column names and
            values the range of values on that column.
        :return: a dataframe with the given index
        :rtype: `pandas.DataFrame`
        """
        index = pd.MultiIndex.from_product(idx.values(), names=idx.keys())
        return pd.DataFrame(index=index)

    def _get_rmnd_regions(self):
        """Obtain a list of REMIND regions."""
        regionmap = pd.read_csv(
            DATA_DIR/"remind/regionmappingH12.csv",
            sep=";")
        return regionmap.RegionCode.unique()

    def _find_suppliers(self, db, expr, locs):
        """
        Return a list of supplier activites in locations `locs` matching
        the peewee expression `expr` within `db`.
        """
        assert type(locs) == list
        sel = self.selector.select(db, expr, locs)
        if sel.count() == 0:
            locs = ["RER"]
        sel = self.selector.select(db, expr, locs)
        if sel.count() == 0:
            locs = ["RoW"]
        sel = self.selector.select(db, expr, locs)
        if sel.count == 0:
            raise ValueError("No activity found for expression.")

        return [Activity(a) for a in sel]

    def supplier_shares(self, db, region):
        """
        Find the ecoinvent activities for a
        REMIND region and the associated share of production volume.

        :param db: a brightway2 database
        :type db: brightway2.Database
        :param region: region string ident for REMIND region
        :type region: string
        :return: dictionary with the format 
            {<tech>: {
                <activity>: <share>, ...
            },
            ...
            }
        :rtype: dict
        """

        # ecoinvent locations within REMIND region
        locs = self.geo.remind_to_ecoinvent_location(region)
        # ecoinvent activities

        vols = pd.read_csv(DATA_DIR/"electricity_production_volumes_per_tech.csv",
                           sep=";", index_col=["dataset", "location"])

        # the filters come from the rmnd_lca package
        # this package is also used to modify the techs in the first place
        fltrs = InventorySet(db).powerplant_filters
        act_shares = {}
        for tech, tech_fltr in fltrs.items():
            expr = self.selector.create_expr(**tech_fltr)
            acts = self._find_suppliers(db, expr, locs)

            # more than one, check shares
            if len(acts) > 1:
                shares = {}
                for act in acts:
                    lookup = vols.index.isin([(act["name"], act["location"])])
                    if any(lookup):
                        shares[act] = vols.loc[lookup, "Sum of production volume"].iat[0]
                    else:
                        shares[act] = 0.
                tot = sum(shares.values())
                if tot == 0:
                    act_shares[tech] = {act: 1./len(acts) for act in acts}
                else:
                    act_shares[tech] = {act: shares[act]/tot for act in acts}

            else:
                act_shares[tech] = {acts[0]: 1}
        return act_shares
