import pytest
import pandas as pd
import random
import brightway2 as bw

from lca2rmnd.reporting import ElectricityLCAReporting
from rmnd_lca import InventorySet
from rmnd_lca.utils import eidb_label

remind_regions = [
    'LAM', 'OAS', 'SSA', 'EUR',
    'NEU', 'MEA', 'REF', 'CAZ',
    'CHA', 'IND', 'JPN', 'USA']

# activate the correct brightway2 project
bw.projects.set_current("transport_lca")

years = [2015]
scenario = "BAU"
rep = ElectricityLCAReporting(scenario, years)

def test_electricity_sectoral_reporting():
    res = rep.report_sectoral_LCA()
    assert isinstance(res, pd.DataFrame)
    assert len(res)

def test_electricity_supplier_shares_random():
    yr = random.choice(years)
    region = random.choice(remind_regions)

    db = bw.Database(eidb_label(scenario, yr))

    shares = rep.supplier_shares(db, region)

    fltrs = InventorySet(db).powerplant_filters
    tech = random.choice(list(fltrs.keys()))

    assert len(shares[tech]) > 0
    assert sum(shares[tech].values()) == 1
