import pytest
import pandas as pd

import brightway2 as bw

from lca2rmnd.reporting import ElectricityLCAReporting

# activate the correct brightway2 project
bw.projects.set_current("transport_lca")

years = [2015]
scenario = "BAU"
rep = ElectricityLCAReporting(scenario, years)

def test_electricity_sectoral_reporting():
    res = rep.report_sectoral_LCA()
    assert isinstance(res, pd.DataFrame)
    assert len(res)
