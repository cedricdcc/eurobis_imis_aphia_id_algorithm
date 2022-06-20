#imports
import json 
import ast
import pandas as pd
import numpy as np
import time
import os
import requests
import plotly
import plotly.express as px

print(plotly.__version__)

# cache section so when i run code below that is doesn't take forever
refresh_cache = False
all_dasid_data = {}
begin_path = os.path.dirname(os.path.abspath(__file__))
print(begin_path)

#parameters for the algorithm #

#nodes to get from the json files
nodes = 150 
#minimum rank of taxonomy to be included in the graph
min_rank = 'Species' #[Superdomain,Kingdom,Phylum,Class,Order,Family,Genus,Species]
#maximum rank of taxonomy to be included in the graph
max_rank = 'Class' # [Superdomain,Kingdom,Phylum,Class,Order,Family,Genus,Species]
#% covered of the species in the original list of taxamonyms
covered_percentage = 1 #100% = 1 / 50% = 0.5
#qualified ranks of taxonomy to be included from aphia => default == accepted
qualified_ranks = 'accepted'
#nan string
nan_str = "NAN"

# make a dict of all the diff ranks and their corresponding values of specificity
rank_values = {
    "DOMAIN": 0,
    "KINGDOM": 1,
    "PHYLUM": 2,
    "CLASS": 3,
    "ORDER": 4,
    "FAMILY": 5,
    "GENUS": 6,
    "SPECIES": 7
}

# dict of all the diff prefixes and their corresponding values of specificity
rank_prefixes = {
    "MEGA": -0.4,
    "GIGA": -0.3,
    "SUPER": -0.2,
    "SUB":0.2,
    "INFRA": 0.3,
    "PARV": 0.4
}








#read in the data from the csv file with pandas
df = pd.read_csv(os.path.join(begin_path, 'aphia_ids_to_imis.csv'))
#print the dimentions of the dataframe
print(df.shape)

# make child dataframes based on the parent dataframe 
children = {}
#get the different values that are present in the first column of the dataframe
diff_dasids = df['IMIS_DasID'].unique()

#loop over the different dasids and make a list of all the aphia_ids that are associated with that dasid
for dasid in diff_dasids:
    children[dasid] = df[df['IMIS_DasID'] == dasid]['aphia_id'].tolist()

#print the children dataframess
print(children)

#make requests to the aphia api to get the taxonomy of the aphia_ids
if refresh_cache:
    for dasid, aphia_ids in children.items():
        print(f"beginning search on dasid: {dasid}")
        length_aphia_ids = len(aphia_ids)
        i = 1
        covered_percentage = 0
        for aphia_id in aphia_ids:
            #get percentge of the aphia_ids that are covered (i/length_aphia_ids)*100, if the percentage is higher than the covered_percentage, then print message
            percentage_covered = (i/length_aphia_ids)*100
            i=i+1
            if percentage_covered > covered_percentage:
                print(f"{dasid} | {percentage_covered}'%' done | {i}/{length_aphia_ids}")
                covered_percentage = covered_percentage + 10
            if i % 50 == 0:
                print(f"{dasid} | {percentage_covered}'%' done | {i}/{length_aphia_ids}")
            #check if the aphia_id is in the cache
            try:
                if aphia_id in all_dasid_data[dasid]:
                    pass
                else:
                    url_to_request = 'https://www.marinespecies.org/rest/AphiaClassificationByAphiaID/' + str(aphia_id)
                    aphia_id_data = requests.get(url_to_request).json()
                    all_dasid_data[dasid][aphia_id] = aphia_id_data
                    time.sleep(1.5)
            except:
                all_dasid_data[dasid] = {}
                url_to_request = 'https://www.marinespecies.org/rest/AphiaClassificationByAphiaID/' + str(aphia_id)
                aphia_id_data = requests.get(url_to_request).json()
                all_dasid_data[dasid][aphia_id] = aphia_id_data
                time.sleep(1.5)
        
        #write the data to a json file
        # Directly from dictionary
        utf_8_encoded_dict_encoded= {str(k).encode("utf-8"): str(v).encode("utf-8") for k,v in all_dasid_data.items()}
        utf_8_encoded_dict = {k.decode("utf-8"): v.decode("utf-8") for k,v in utf_8_encoded_dict_encoded.items()}
        new_dict = {}
        for key, value in utf_8_encoded_dict.items():
            new_dict[key] = ast.literal_eval(value.replace("'", "\""))
        with open(os.path.join(begin_path, 'json_data_aphia_ids.json'), 'w') as outfile:
            json.dump(new_dict, outfile)
            
#read in the data from the json file
f = open(os.path.join(begin_path, 'json_data_aphia_ids.json'), 'r')
data = json.load(f)

dasids = list(data.keys())
print(dasids)

def populate_rank_dict(rank_dict, checkvalue):
    if isinstance(checkvalue, dict):
        for key, value in checkvalue.items():
            if key == 'AphiaID':
                aphia_id = str(value)
            if key == 'rank':
                rank = value
            if key == 'child':
                next_value_check = value
        if rank == "Species":
            #check if the aphia_id is in the rank_dict[rank] => if not, add rank_dict.append(1) else add 1 to the rank_dict[rank] index
            if aphia_id in rank_dict[rank]:
                index = rank_dict[rank].index(aphia_id)
                rank_dict["value_species"][index] = rank_dict["value_species"][index] + 1
            else:
                rank_dict["value_species"].append(1)       
        rank_dict[rank].append(aphia_id)
        populate_rank_dict(rank_dict, next_value_check)
    else:
        #go over each key in rank_dict and get the max length of all the lists
        max_length = 0
        for key in rank_dict.keys():
            length_current_rank = len(rank_dict[key])
            if length_current_rank > max_length:
                max_length = length_current_rank
        
        #go over each array in rank_dict and add the missing elements to the array with the max length
        for key in rank_dict.keys():
            length_current_rank = len(rank_dict[key])
            if length_current_rank < max_length:
                if key != 'value_species':
                    rank_dict[key].append(nan_str)
                else:
                    rank_dict[key].append(1)
        return
        
        

def check_dict_aphia_id(checkvalue, array_aphia_ids, parent_aphia_id, character, value_dict, parent_aphia_ids, scientific_names, rank_dict, ranks):
    #check if checkvalue is of type dict
    if isinstance(checkvalue, dict):
        #check if there is a key named 'AphiaID'
        for key, value in checkvalue.items():
            if key == 'AphiaID':
                array_aphia_ids.append(value)
                #check if the value is already in charcter array and if not, add it and append 1 to value array | if yes find the index of the value in the array and add 1 to the value array
                if value in character:
                    index = character.index(value)
                    value_dict[index] = value_dict[index] + 1
                else:
                    character.append(value)
                    value_dict.append(1)
                    ranks.append(checkvalue['rank'])
                    parent_aphia_ids.append(parent_aphia_id)
                    scientific_names.append(checkvalue['scientificname'])
                    if checkvalue['rank'] not in rank_dict:
                        rank_dict[checkvalue['rank']] = []
            if key == 'child':
                check_dict_aphia_id(value, array_aphia_ids, parent_aphia_id=checkvalue["AphiaID"], character= character, value_dict = value_dict, parent_aphia_ids = parent_aphia_ids, scientific_names=scientific_names, rank_dict = rank_dict, ranks=ranks)
    return array_aphia_ids

#get all aphia_ids from each dasid
dasids_unique_aphia_ids = {}
complete_data_dasids = {}

for dasid in dasids:
    all_aphia_ids = []
    scientific_names = []
    parent_aphia_ids = []
    ranks= []
    character = []
    value_dict = []
    rank_dict = {}
    data_dasid = data[dasid]
    for aphia_id , value_aphia_id in data_dasid.items():
        all_aphia_ids.append(aphia_id)
        toappend_later_aphia_ids = []
        check_dict_aphia_id(value_aphia_id, toappend_later_aphia_ids, "", character=character, value_dict=value_dict, parent_aphia_ids=parent_aphia_ids, scientific_names=scientific_names, rank_dict=rank_dict, ranks=ranks)
        for id_aphia in toappend_later_aphia_ids:
            all_aphia_ids.append(id_aphia)
    #get unique values of all_aphia_ids
    unique_aphia_ids = list(set(all_aphia_ids))
    print(f" dasid: {dasid} | unique aphia_ids: {len(unique_aphia_ids)}")
    dasids_unique_aphia_ids[dasid] = unique_aphia_ids
    complete_data_dasids[dasid] = {'character': character, 'values': value_dict, 'parent_aphia_ids': parent_aphia_ids, 'scientific_names': scientific_names, 'rank': ranks}
    
    rank_dict["value_species"] = []
    print(rank_dict)
    for aphia_id , value_aphia_id in data_dasid.items():
        populate_rank_dict(rank_dict, value_aphia_id)
    #print(rank_dict)
    
    path_list = []
    for key in rank_dict.keys():
        #print(len(rank_dict[key]))
        # get length of the array where value is not "Not given"
        length_non_not_given = len(rank_dict[key])-(rank_dict[key].count(nan_str))
        #print(length_non_not_given)
        '''
        print(len(rank_dict[key]))
        if key == "value_species":
            print(rank_dict[key])
        '''
        if key != "value_species":
            path_list.append(key)
    '''
    fig = px.sunburst(
        rank_dict,
        path=path_list,
        values='value_species',
        color=max_rank
    )
    fig.show()
    '''
#print(complete_data_dasids)

#calculate the weight of each species for each dasid
weights_dasids = {}
max_rank_value = rank_values[max_rank.upper()]
for dasid, dicts_complete_values in complete_data_dasids.items():
    length_array = len(dicts_complete_values['character'])
    i = 0
    while i < length_array:
        aphia_id = dicts_complete_values['character'][i]
        scientific_name = dicts_complete_values['scientific_names'][i]
        value = dicts_complete_values['values'][i]
        rank_string_id = dicts_complete_values['rank'][i]
        rank_value = 0
        for str_rank, value_rank in rank_values.items():
            if str_rank in rank_string_id.upper():
                rank_value = value_rank
        prefix_rank = 0
        for prefix_rank, value_prefix in rank_prefixes.items():
            if prefix_rank in rank_string_id.upper():
                prefix_value = value_prefix
        display_value = rank_value + prefix_value
        
        todisplay = 1
        if max_rank_value > display_value:
            todisplay = 0
        
        weight_formula = display_value*todisplay*value
        if dasid not in weights_dasids:
            weights_dasids[dasid] = []
        weights_dasids[dasid].append({'aphia_id': aphia_id, 'scientific_name': scientific_name, 'weight': weight_formula, 'rank': rank_string_id})
        i= i+1

for dasid, values_weights in weights_dasids.items():
    for weight in values_weights:
        print(f"dasid: {dasid} | aphia_id: {weight['aphia_id']} | scientific_name: {weight['scientific_name']} | weight: {weight['weight']} | rank: {weight['rank']}")
        
# for each dasid in weights_dasids, get the top {nodes} aphia_ids
for dasid, values_weights in weights_dasids.items():
    #make array of sorted indexes based on weight
    sorted_indexes = sorted(range(len(values_weights)), key=lambda k: values_weights[k]['weight'], reverse=True)
    #get top {nodes}
    i = 0
    while i < nodes:
        try:
            print(f"dasid: {dasid} | aphia_id: {values_weights[sorted_indexes[i]]['aphia_id']} | scientific_name: {values_weights[sorted_indexes[i]]['scientific_name']} | weight: {values_weights[sorted_indexes[i]]['weight']} | rank: {values_weights[sorted_indexes[i]]['rank']}")
            i = i+1
        except:
            i = i+1
        