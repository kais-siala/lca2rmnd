# content of test_activity_maps.py
from lca2rmnd.activity_select import ActivitySelector

from premise import InventorySet, Geomap
from bw2data.database import DatabaseChooser
from bw2data.backends.peewee.proxies import ActivityDataset as Act

# bw.projects.set_current("transport_lca")

# years = [2015]
# scenario = "BAU"

dummy_list = [
    {'name': 'Electricity, at BIGCC power plant 450MW, pre, pipeline 200km, storage 1000m/2025'},
    {'name': 'Electricity, at BIGCC power plant 450MW, no CCS/2025'},
    {'name': 'Electricity, at power plant/lignite, IGCC, no CCS/2025'},
    {'name': 'Electricity, at power plant/hard coal, pre, pipeline 200km, storage 1000m/2025'},
    {'name': 'Electricity, at power plant/hard coal, post, pipeline 200km, storage 1000m/2025'},
    {'name': 'Electricity, at power plant/natural gas, pre, pipeline 200km, storage 1000m/2025'},
    {'name': 'heat and power co-generation, biogas, gas engine, label-certified'},
    {'name': 'electricity production, hard coal'},
    {'name': 'heat and power co-generation, hard coal'},
    {'name': 'electricity production, natural gas, conventional power plant'},
    {'name': 'electricity production, natural gas, combined cycle power plant'},
    {'name': 'heat and power co-generation, natural gas, conventional power plant, 100MW electrical'},
    {'name': 'electricity production, deep geothermal'},
    {'name': 'electricity production, hydro, reservoir, tropical region'},
    {'name': 'electricity production, nuclear, pressure water reactor'},
    {'name': 'electricity production, oil'},
    {'name': 'electricity production, solar thermal parabolic trough, 50 MW'},
    {'name': 'electricity production, photovoltaic, 3kWp facade installation, multi-Si, laminated, integrated'},
    {'name': 'electricity production, wind, 2.3MW turbine, precast concrete tower, onshore'}, {'name': 'steel production'},
    {'name':'market for aluminium, primary'}]

for act in dummy_list:
    act["location"] = "DE"
    act["unit"] = "kilowatt hour"
    act["reference product"] = "electricity"
dummy_db = {("dummy_db", str(n+1111231)): dummy_list[n] for n in range(len(dummy_list))}

db_act = DatabaseChooser('dummy_db')
db_act.write(dummy_db)

def test_filter_powerplants():
    sel = ActivitySelector()
    fltr = InventorySet(db_act).powerplant_filters

    for tech, tech_fltr in fltr.items():
        expr = sel.create_expr(**tech_fltr)
        select = sel.select(db_act, expr, ["DE"])
        assert select.count() > 0, "No activities found for {}".format(tech)
