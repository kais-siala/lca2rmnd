"""Module to prepare the ecoinvent databases for reporting LCA results
for REMIND scenarios using the carculator inventories, see

https://github.com/romainsacchi/carculator

Usage example:
    years = [2015, 2050]
    scenario = "BAU"
    project_name = "transport_lca"

    fpei36 = "ecoinvent/ecoinvent 3.6_cut-off_ecoSpold02/datasets/"
    create_project(project_name, fpei36, years, scenario, "data/remind/")
    load_and_merge(scenario, years)

    # test
    act = bw.Database("ecoinvent_BAU_2015").random()
    bw.LCA({act: 1}, bw.methods.random()).lci()

"""

years = [2030]
scenario = "SSP2-Base"
project_name = "H2"
# dummy switch, will be removed
method = "Romain"

if method = "Alois":
    from lca2rmnd.prepare_inventories import *
    fpei36 = "/home/alois/ecoinvent/ecoinvent 3.6_cut-off_ecoSpold02/datasets/"
    model = "remind"

    create_project(project_name, fpei36, years, scenario, "data/remind/")
    load_and_merge(scenario, years)
    
elif method = "Romain":
    import brightway2 as bw
    import numpy as np
    import pandas as pd
    
    bw.projects.set_current(project_name)
    activities2report = ["market group for electricity, low voltage",
                         "market group for electricity, medium voltage",
                        ]
    indicators = [('IPCC 2013', 'climate change', 'GWP 100a'),
                 ]
    
    for year in years:
        db_name = "ecoinvent_"+scenario+"_{}".format(year)
        for act2report in activities2report:
            FU = [{a:1} for a in bw.Database(db_name) if act2report in a["name"]]
            for j, fu in enumerate(FU):
                LCA = bw.LCA(FU[j], gwp) 
                LCA.lci(fu)
                LCA.lcia()
                if j==0:
                    # Obtain the indices of the activities in the LCA matrix
                    rev_act_dict, _, _ = LCA.reverse_dict()
                    my_index = []
                    for v in rev_act_dict.values():
                        ds = bw.get_activity(v)
                        my_index.append((ds["name"], ds["location"], ds["unit"]))
                    # Initialize dataframe
                    df = pd.DataFrame(index=pd.MultiIndex.from_tuples(my_index, names=["name", "loc", "unit"]))
                df[(list(fu.keys())[0]["name"], list(fu.keys())[0]["location"])] = np.squeeze(np.asarray(LCA.characterized_inventory.sum(axis=0)))
    
    # To be continued