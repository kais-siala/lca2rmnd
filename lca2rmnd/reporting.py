from . import DATA_DIR

from rmnd_lca.electricity import Electricity
from rmnd_lca.utils import eidb_label

import brightway2 as bw


class RemindLCAReporting():
    def __init__(self, year, region, scenario, sector):
        self.year = year
        self.region = region
        self.scenario = scenario
        self.sector = sector

        self.db = bw.Database(ecoinvent_inventory_label(self.scenario, self.year))
        self.rdc = RemindDataCollection(self.scenario)

    def reportElectricity(self):
        
        elec = Electricity(self.db, )
        locations = Electricity.remind_to_ecoinvent_location(self.region)
