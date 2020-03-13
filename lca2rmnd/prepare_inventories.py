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

from bw2data.backends.peewee.proxies import Activity, ActivityDataset as Act
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


def relink_electricity_demand(scenario, year):
    """Create LDV activities for REMIND regions and relink
    existing electricity exchanges for BEVs, FCEVs and PHEVs
    to REMIND-compatible (regional) market groups.

    :param eidb: REMIND scenario.
    :param year: REMIND year.

    """
    eidb = bw.Database(rmnd_lca.utils.eidb_label(scenario, year))
    remind_regions = [
        'LAM', 'OAS', 'SSA', 'EUR',
        'NEU', 'MEA', 'REF', 'CAZ',
        'CHA', 'IND', 'JPN', 'USA']

    def find_evs():
        # find EVs (rexexp function in peewee seems to be broken)
        return [Activity(sel) for sel in Act.select().where(
            (Act.name.contains("EV,")
             | Act.name.contains("PHEV-"))  # PHEV-d and PHEV-p
            & (Act.database == eidb.name))]

    bevs = find_evs()
    # any non-global activities found?
    non_glo = [act for act in bevs if act["location"] != "GLO"]
    if non_glo:
        print(("Found non-global EV activities: {}"
               "DB is most likely already updated.").format(non_glo))
        ans = input("Delete existing non-GLO activities? (y/n)")
        if ans == "y":
            for act in non_glo:
                act.delete()
            bevs = find_evs()
        else:
            return

    for region in remind_regions:
        print("Relinking markets for {}".format(region))
        # find markets
        new_market = Activity(Act.get(
            Act.name.startswith("market group for electricity, low voltage")
            & (Act.location == region)
            & (Act.database == eidb.name)))
        old_market_name = ("electricity market for fuel preparation, {}"
                           .format(year))
        for bev in bevs:
            new_bev = bev.copy()
            new_bev["location"] = region
            new_bev.save()

            exchanges = [
                ex for ex in new_bev.technosphere()
                if ex["name"] == old_market_name]
            # should only be one
            if len(exchanges) > 1:
                raise ValueError("More than one electricity market for "
                                 "fuel production found for {}"
                                 .format(new_bev))
            elif len(exchanges) == 1:
                # new exchange
                new_bev.new_exchange(**{
                    "name": new_market["name"],
                    "amount": exchanges[0]["amount"],
                    "unit": "kilowatt hour",
                    "type": "technosphere",
                    "location": region,
                    "uncertainty type": 1,
                    "reference product": "electricity, low voltage",
                    "input": new_market.key
                }).save()

                exchanges[0].delete()


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
        eidb = rmnd_lca.utils.eidb_label(scenario, year)
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
            relink_electricity_demand(scenario, year)
