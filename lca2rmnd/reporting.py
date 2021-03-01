from . import DATA_DIR
from .data_collection import RemindDataCollection
from .activity_select import ActivitySelector
from .utils import project_string

from premise import Geomap, InventorySet
from premise.utils import eidb_label

from bw2data.backends.peewee.proxies import Activity, ActivityDataset as Act
import brightway2 as bw
import pandas as pd
from bw2analyzer import ContributionAnalysis

import time


class LCAReporting():
    """
    The base class for LCA Reports for REMIND output.

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
    def __init__(self, scenario, years, project,
                 remind_output_folder,
                 methods, regions=None):
        self.years = years
        self.scenario = scenario
        self.model = "remind"
        bw.projects.set_current(project)
        self.selector = ActivitySelector()
        self.methods = methods

        if not self.methods:
            raise ValueError(("No methods found in the current brightway2"
                              " project for the following group: {}.")
                             .format(indicatorgroup))

        # # check for brightway2 databases
        # dbnames = set(["_".join(["ecoinvent", scenario, str(yr)])
        #                for yr in years])
        # missing = dbnames - set(bw.databases)
        # if missing:
        #     raise ValueError(
        #         "The following brightway2 databases are missing: {}"
        #         .format(missing))
        rdc = RemindDataCollection(self.scenario, remind_output_folder)
        self.data = rdc.data[rdc.data.Year.isin(self.years) &
                             (rdc.data.Region != "World")]
        if regions is None:
            self.regions = self.data.Region.unique()
        else:
            # all regions there?
            self.regions = regions
            assert self.regions in self.data.Region.unique()
        self.geo = Geomap(self.model)


class TransportLCAReporting(LCAReporting):
    """
    Report LCA scores for the REMIND transport sector.

    The class assumes that the current brightway project contains
    all the relevant databases, including the characterization methods.
    Note that the convention for database names is
    "ecoinvent_<scenario>_<year>".

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

    # available variables
    techs = ["BEV", "FCEV", "Gases", "Hybrid Liquids", "Hybrid Electric", "Liquids"]
    variables = ["ES|Transport|VKM|Pass|Road|LDV|" + tech for tech in techs]

    def _act_from_variable(self, variable, db, year, region, scale=1):
        """
        Find the activity for a given REMIND transport reporting variable.
        """
        techmap = {
            "BEV": "battery electric",
            "FCEV": "fuel cell electric",
            "Gases": "compressed gas",
            "Hybrid Electric": {
                "diesel": "plugin diesel hybrid",
                "petrol": "plugin gasoline hybrid"
            },
            "Hybrid Liquids": {
                "diesel": "diesel hybrid",
                "petrol": "gasoline hybrid"
            },
            "Liquids": {
                "diesel": "diesel",
                "petrol": "gasoline"
            }
        }
        tech = variable.split("|")[-1]
        liq_share = {
            "diesel": 0.4,
            "petrol": 0.6
        }
        if tech in ["Hybrid Electric", "Hybrid Liquids", "Liquids"]:
            if region in ["CHA", "REF", "IND"]:
                demand = {
                    Activity(Act.get(
                        (Act.name == "transport, passenger car, fleet average, {}, {}".format(
                            techmap[tech]["petrol"], year))
                        & (Act.location == region)
                        & (Act.database == db.name))): scale
                }
            else:
                demand = {
                    Activity(Act.get(
                        (Act.name == "transport, passenger car, fleet average, {}, {}".format(
                            techmap[tech][liq], year))
                        & (Act.location == region)
                        & (Act.database == db.name))): scale * liq_share[liq]
                    for liq in ["diesel", "petrol"]
                }
            return demand
        else:
            return  {
                Activity(
                    Act.get(
                        (Act.name == "transport, passenger car, fleet average, {}, {}".format(
                            techmap[tech], year))
                        & (Act.location == region)
                        & (Act.database == db.name))): scale
            }

    def report_LDV_LCA(self):
        """
        Report per-drivetrain impacts along the given dimension.
        Both per-pkm as well as total numbers are given.

        :return: a dataframe with impacts for the REMIND EDGE-T
            transport sector model. Levelized impacts (per pkm) are
            found in the column `score_pkm`, total impacts in `total_score`.
        :rtype: pandas.DataFrame

        """

        df = self.data[self.data.Variable.isin(self.variables)]

        df.loc[:, "score_pkm"] = 0.
        # add methods dimension & score column
        methods_df = pd.DataFrame({"Method": self.methods, "score_pkm": 0.})
        df = df.merge(methods_df, "outer")  # on "score_pkm"

        df.set_index(["Year", "Region", "Variable", "Method"], inplace=True)
        start = time.time()

        # calc score
        for year in self.years:
            # find activities which at the moment do not depend
            # on regions
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                for var in (df.loc[(year, region)]
                            .index.get_level_values(0)
                            .unique()):
                    demand = self._act_from_variable(var, db, year, region)
                    lca = bw.LCA(demand,
                                 method=self.methods[0])
                    # build inventories
                    lca.lci()

                    for method in self.methods:
                        lca.switch_method(method)
                        lca.lcia()
                        fct = 1.
                        if "_LowD" in self.scenario:
                            fct = max(1 - (year - 2020)/15 * 0.15, 0.85)
                        df.loc[(year, region, var, method),
                               "score_pkm"] = lca.score * fct
        print("Calculation took {} seconds.".format(time.time() - start))
        df["total_score"] = df["value"] * df["score_pkm"] * 1e9
        return df[["total_score", "score_pkm"]]

    def _get_material_bioflows_for_bev(self):
        """
        Obtain bioflow ids for *interesting* materials.
        These are the top bioflows in the ILCD materials
        characterization method for an BEV activity.
        """

        method = ('ILCD 2.0 2018 midpoint',
                  'resources', 'minerals and metals')
        year = self.years[0]
        act_str = "transport, passenger car, fleet average, battery electric, {}".format(year)

        # upstream material demands are the same for all regions
        # so we can use GLO here
        act = Activity(
            Act.get((Act.name == act_str)
                    & (Act.database == eidb_label(
                        self.model, self.scenario, year))
                    & (Act.location == "EUR")))
        lca = bw.LCA({act: 1}, method=method)
        lca.lci()
        lca.lcia()

        inv_bio = {value: key for key, value in lca.biosphere_dict.items()}

        ca = ContributionAnalysis()
        ef_contrib = ca.top_emissions(lca.characterized_inventory)
        return [inv_bio[int(el[1])] for el in ef_contrib]

    def report_materials(self):
        """
        Report the material demand of the LDV fleet for all regions and years.

        :return: A `pandas.Series` with index `year`, `region` and `material`.
        """
        # materials
        bioflows = self._get_material_bioflows_for_bev()

        df = self.data[self.data.Variable.isin(self.variables)]

        df.set_index(["Year", "Region", "Variable"], inplace=True)

        start = time.time()
        result = {}
        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                # create large lca demand object
                demand = [
                    self._act_from_variable(
                        var, db, year, region,
                        scale=df.loc[(year, region, var), "value"])
                    for var in (df.loc[(year, region)]
                                .index.get_level_values(0)
                                .unique())]
                # flatten dictionaries
                demand_flat = {}
                for item in demand:
                    for act, val in item.items():
                        demand_flat[act] = val + demand_flat.get(act, 0)

                lca = bw.LCA(demand_flat)
                # build inventories
                lca.lci()
                for code in bioflows:
                    result[(
                        year, region,
                        bw.get_activity(code)["name"].split(",")[0]
                    )] = (
                        lca.inventory.sum(axis=1)[
                            lca.biosphere_dict[code], 0]
                    )
        df_result = pd.Series(result)
        print("Calculation took {} seconds.".format(time.time() - start))
        return df_result * 1e9  # kg

    def report_direct_emissions(self):
        """
        Report the direct (exhaust) emissions of the LDV fleet.
        """

        df = self.data[self.data.Variable.isin(self.variables)]

        df.set_index(["Year", "Region", "Variable"], inplace=True)

        start = time.time()
        result = {}
        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                for var in (df.loc[(year, region)]
                            .index.get_level_values(0)
                            .unique()):
                    for act, share in self._act_from_variable(
                            var, db, year, region).items():
                        for ex in act.biosphere():
                            result[(year, region, ex["name"])] = (
                                result.get((year, region, ex["name"]), 0)
                                + ex["amount"] * share * df.loc[(year, region, var), "value"])

        df_result = pd.Series(result)
        print("Calculation took {} seconds.".format(time.time() - start))
        return df_result * 1e9  # kg

    def report_endpoint(self):
        """
        *DEPRECATED*
        Report the surplus extraction costs for the scenario.

        :return: A `pandas.Series` containing extraction costs
          with index `year` and `region`.
        """
        indicatorgroup = 'ReCiPe Endpoint (H,A) (obsolete)'
        endpoint_methods = [m for m in bw.methods if m[0] == indicatorgroup
                   and m[2] == "total"
                   and not m[1] == "total"]

        df = self.data[self.data.Variable.isin(self.variables)]

        df.set_index(["Year", "Region", "Variable"], inplace=True)
        start = time.time()
        result = {}
        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                # create large lca demand object
                demand = [
                    self._act_from_variable(
                        var, db, year, region,
                        scale=df.loc[(year, region, var), "value"])
                    for var in (df.loc[(year, region)]
                                .index.get_level_values(0)
                                .unique())]
                # flatten dictionaries
                demand = {k: v for item in demand for k, v in item.items()}
                lca = bw.LCA(demand, method=endpoint_methods[0])
                # build inventories
                lca.lci()
                for method in endpoint_methods:
                    lca.switch_method(method)
                    lca.lcia()
                    # 6% discount for monetary endpoint
                    factor = 1e9 * 1.06 ** (year - 2013) \
                             if "resources" == method[1] else 1e9
                    result[(
                        year, region, method
                    )] = lca.score * factor

        df_result = pd.Series(result)
        print("Calculation took {} seconds.".format(time.time() - start))
        return df_result  # billion pkm

    def report_midpoint(self):
        """
        Report midpoint impacts for the full fleet for each scenario.

        :return: A `pandas.Series` containing impacts
          with index `year`,`region` and `method`.
        """

        df = self.data[self.data.Variable.isin(self.variables)]

        df.set_index(["Year", "Region", "Variable"], inplace=True)
        start = time.time()
        result = {}
        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                # create large lca demand object
                demand = [
                    self._act_from_variable(
                        var, db, year, region,
                        scale=df.loc[(year, region, var), "value"])
                    for var in (df.loc[(year, region)]
                                .index.get_level_values(0)
                                .unique())]
                # flatten dictionaries
                demand_flat = {}
                for item in demand:
                    for act, val in item.items():
                        demand_flat[k] = val + demand_flat.get(k, 0)

                lca = bw.LCA(demand_flat, method=self.methods[0])
                # build inventories
                lca.lci()
                for method in self.methods:
                    lca.switch_method(method)
                    lca.lcia()
                    factor = 1e9
                    result[(
                        year, region, method
                    )] = lca.score * factor

        df_result = pd.Series(result)
        print("Calculation took {} seconds.".format(time.time() - start))
        return df_result # billion pkm

    def report_midpoint_to_endpoint(self):
        """
        *DEPRECATED*
        Report midpoint impacts for the full fleet for each scenario.

        :return: A `pandas.Series` containing impacts
          with index `year`,`region` and `method`.
        """
        methods = [m for m in bw.methods
                   if m[0] == "ReCiPe Endpoint (H,A) (obsolete)"
                   and m[2] != "total"]

        df = self.data[self.data.Variable.isin(self.variables)]

        df.set_index(["Year", "Region", "Variable"], inplace=True)
        start = time.time()
        result = {}
        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
                # create large lca demand object
                demand = [
                    self._act_from_variable(
                        var, db, year, region,
                        scale=df.loc[(year, region, var), "value"])
                    for var in (df.loc[(year, region)]
                                .index.get_level_values(0)
                                .unique())]
                # flatten dictionaries
                demand = {k: v for item in demand for k, v in item.items()}
                lca = bw.LCA(demand, method=self.methods[0])
                # build inventories
                lca.lci()
                for method in methods:
                    lca.switch_method(method)
                    lca.lcia()
                    factor = 1e9
                    result[(
                        year, region, method
                    )] = lca.score * factor

        df_result = pd.Series(result)
        print("Calculation took {} seconds.".format(time.time() - start))
        return df_result # billion pkm

class ElectricityLCAReporting(LCAReporting):
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


        return result[["Year", "Region", "method", "total_score", "score_kWh"]].drop_duplicates()

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
        df.loc[:, "score"] = 0.

        # calc score
        for year in self.years:
            db = bw.Database(eidb_label(self.model, self.scenario, year))
            for region in self.regions:
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

                df_slice.loc[:, "score"] = df_slice.apply(
                    lambda row: get_score(row["method"]), axis=1)
                df.update(df_slice)

        df["total_score"] = df["score"] * df["value"] * 2.8e11  # EJ -> kWh
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

        result = self._cartesian_product({
            "region": self.regions,
            "tech": list(tecdict.keys()),
            "method": self.methods
        }).sort_index()

        for region in self.regions:
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

        # the filters come from the premise package
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
