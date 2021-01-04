
import pandas as pd
import random
import brightway2 as bw
import math

from lca2rmnd.reporting import ElectricityLCAReporting, TransportLCAReporting
from premise import InventorySet
from premise.utils import eidb_label

remind_regions = [
    'LAM', 'OAS', 'SSA', 'EUR',
    'NEU', 'MEA', 'REF', 'CAZ',
    'CHA', 'IND', 'JPN', 'USA']

# activate the correct brightway2 project
bw.projects.set_current("transport_lca")

years = [2050]
scenario = "BAU"
model = "remind"

def test_electricity_sectoral_reporting():
    rep = ElectricityLCAReporting(scenario, years)
    res = rep.report_sectoral_LCA()
    assert isinstance(res, pd.DataFrame)
    assert len(res)


def test_electricity_supplier_shares_random():
    rep = ElectricityLCAReporting(scenario, years)
    yr = random.choice(years)
    region = random.choice(remind_regions)

    db = bw.Database(eidb_label(model, scenario, yr))

    shares = rep.supplier_shares(db, region)

    fltrs = InventorySet(db).powerplant_filters
    tech = random.choice(list(fltrs.keys()))

    assert len(shares[tech]) > 0
    assert math.isclose(sum(shares[tech].values()), 1)


def test_electricity_tech_reporting():
    rep = ElectricityLCAReporting(scenario, years)
    yr = random.choice(years)
    region = random.choice(remind_regions)

    db = bw.Database(eidb_label(model, scenario, yr))
    fltrs = InventorySet(db).powerplant_filters
    tech = random.choice(list(fltrs.keys()))

    test = rep.report_tech_LCA(yr)

    assert len(test) > 0
    assert len(test.loc[(region, tech)]) > 0


def test_ldv_tech_reporting():
    rep = TransportLCAReporting(scenario, years)
    test = rep.report_LDV_LCA()

    assert len(test) > 0
    yr = random.choice(years)
    region = random.choice(remind_regions)
    assert len(test.loc[(yr, region)]) > 0

    variables = [
        var for var in rep.data.Variable.unique()
        if var.startswith("ES|Transport|Pass|Road|LDV")
        and "Two-Wheelers" not in var]
    # only high detail entries
    variables = [var for var in variables if len(var.split("|")) == 7]
    assert len(variables) > 0

    var = random.choice(variables)
    assert len(test.loc[(yr, region, var)]) > 0

    met = random.choice(rep.methods)
    assert test.at[(yr, region, var, met), "score_pkm"] > 0


def test_ldv_material_reporting():
    rep = TransportLCAReporting(scenario, years)
    test = rep.report_materials()

    assert len(test) > 0
    yr = random.choice(years)
    region = random.choice(remind_regions)
    assert len(test.loc[(yr, region)]) > 0

    assert test.loc[(yr, region, "Gold")] > 0

def test_ldv_endpoint_reporting():
    rep = TransportLCAReporting(scenario, years)
    test = rep.report_endpoint()

    assert len(test) > 0
    yr = random.choice(years)
    region = random.choice(remind_regions)

    assert test.loc[(yr, region)] > 0
