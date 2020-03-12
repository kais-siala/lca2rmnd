"""Module to prepare the ecoinvent databases for reporting LCA results
for REMIND scenarios using the carculator inventories, see

https://github.com/romainsacchi/carculator

Usage example:
    years = [2015, 2050]
    scenario = "BAU"
    project_name = "transport_lca"

    fpei36 = "ecoinvent/ecoinvent 3.6_cut-off_ecoSpold02/datasets/"
    create_project(project_name, fpei36, years, scenario)
    load_and_merge(scenario, years)

    # test
    act = bw.Database("ecoinvent_BAU_2015").random()
    bw.LCA({act: 1}, bw.methods.random()).lci()

"""

import rmnd_lca
from bw2data.utils import merge_databases

from carculator import CarInputParameters, \
    fill_xarray_from_input_parameters, \
    CarModel, InventoryCalculation

import brightway2 as bw
import numpy as np


def create_project(project_name, ecoinvent_path,
                   years, scenario="BAU"):
    """
    Create and prepare a brightway2 project with updated
    inventories for electricity markets according to
    REMIND data.

    Relies on `rmnd_lca.NewDatabase`. Existing databases are
    deleted.

    :param str project_name: name of the brightway2 project to modify
    :param str ecoinvent_path: path to the ecoinvent db, at present
        this has to be ecoinvent version 3.6
    :param list years: range of years to create inventories for
    :param str scenario: the scenario to create inventories for

    """
    bw.projects.set_current(project_name)

    print("Clean existing databases.")
    dbstr = list(bw.databases)
    for db in dbstr:
        del(bw.databases[db])
    bw.methods.clear()

    bw.bw2setup()

    print("Import Ecoinvent.")
    if 'ecoinvent 3.6 cutoff' in bw.databases:
        print("Database has already been imported")
    else:
        ei36 = bw.SingleOutputEcospold2Importer(
            ecoinvent_path, 'ecoinvent 3.6 cutoff')
        ei36.apply_strategies()
        ei36.statistics()
        ei36.write_database()

    for year in years:
        print("Create modified database for scenario {} and year {}"
              .format(scenario, year))
        ndb = rmnd_lca.NewDatabase(
            scenario=scenario,
            year=year,
            source_db='ecoinvent 3.6 cutoff',
            source_version=3.6)
        ndb.update_electricity_to_remind_data()
        ndb.write_db_to_brightway()


def load_car_activities(year_range):
    """Load `carculator` inventories for a given range of years.

    :param numpy.ndarray year_range: range of years
    :returns: a brightway2 `LCIImporter` object
    :rtype: bw2io.importers.base_lci.LCIImporter

    """
    cip = CarInputParameters()

    cip.static()

    _, array = fill_xarray_from_input_parameters(cip)

    array = array.interp(
        year=year_range, kwargs={'fill_value': 'extrapolate'})

    cm = CarModel(array, cycle='WLTC')

    cm.set_all()

    ic = InventoryCalculation(cm.array)

    return ic.export_lci_to_bw(ecoinvent_compatibility=False)[0]


def relink_electricity_demand(eidb):
    """Create BEV activities for REMIND regions and relink
    existing electricity exchanges for BEVs to REMIND-compatible (regional)
    market groups.

    :param eidb: a brightway2 `Database`. This database is
        modified in place.

    """
    remind_regions = [
        'LAM', 'OAS', 'SSA', 'EUR',
        'NEU', 'MEA', 'REF', 'CAZ',
        'CHA', 'IND', 'JPN', 'USA']
    # find BEVs
    bevs = [a for a in eidb if "BEV" in a["name"]
            or "PHEV" in a["name"]]

    # any non-global activities found?
    non_glo = [act for act in bevs if act["location"] != "GLO"]
    if non_glo:
        print(("Database is most likely already updated."
               "Deleting existing non-GLO activities."))
        for act in non_glo:
            act.delete()

    market_types = ["low", "medium", "high"]

    for region in remind_regions:
        # find markets
        markets = {mtype: [
            a for a in eidb
            if a["name"].startswith(
                    "market group for electricity, {} voltage".format(mtype))
            and a["location"] == region][0]
                   for mtype in market_types}
        for bev in bevs:
            new_bev = bev.copy()
            new_bev["location"] = region
            new_bev.save()
            for mtype in market_types:
                refprod = "electricity, {} voltage".format(mtype)
                exchanges = [
                    ex for ex in new_bev.technosphere()
                    if ex["reference product"] == refprod]
                # delete old exchanges
                demand = sum([ex["amount"] for ex in exchanges])
                if demand > 0:
                    for ex in exchanges:
                        ex.delete()

                    # new exchange
                    exc = new_bev.new_exchange(**{
                        "name": markets[mtype]["name"],
                        "amount": demand,
                        "unit": "kilowatt hour",
                        "type": "technosphere",
                        "location": region,
                        "uncertainty type": 1,
                        "reference product": refprod,
                        "input": markets[mtype].key
                    })
                    exc.save()


def load_and_merge(scenario, years, relink=True):
    """
    Load carculator outputs and merge them with ecoinvent
    databases for all years.

    :param str scenario: REMIND scenario
    :param list years: range of years
    :param bool relink: create BEVs with electricity inputs
        from market groups in REMIND regions
    """
    for year in years:
        eidb = "_".join(["ecoinvent", scenario, str(year)])
        inv = load_car_activities(np.array([year]))
        inv.apply_strategies()

        if 'additional_biosphere' not in bw.databases:
            inv.create_new_biosphere('additional_biosphere')

        inv.match_database(
            eidb,
            fields=('name', 'unit', 'location', 'reference product'))
        inv.match_database("biosphere3",
                           fields=('name', 'unit', 'categories'))
        inv.match_database("additional_biosphere",
                           fields=('name', 'unit', 'categories'))
        inv.match_database(fields=('name', 'unit', 'location'))
        inv.statistics()
        inv.write_database()

        print("Merge carculator results with ecoinvent.")
        merge_databases(eidb, inv.db_name)
        if relink:
            relink_electricity_demand(bw.Database(eidb))
