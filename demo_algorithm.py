#imports
import json 
import ast
import pandas as pd
import numpy as np
import time
import os
import sys
import csv
import requests
import plotly
import plotly.express as px

print(plotly.__version__)

# cache section so when i run code below that is doesn't take forever
refresh_cache = False
begin_path = os.path.dirname(os.path.abspath(__file__))
print(begin_path)

#parameters for the algorithm #

#nodes to get from the json files
nodes = 50

def get_child_if_dict(child, parent_id, parent_list, aphia_id_list, scientific_name_list, rank_list):
    if isinstance(child["child"], dict):
        parent_list.append(parent_id)
        aphia_id_list.append(child["AphiaID"])
        parent_id = child["AphiaID"]
        rank_list.append(child["rank"])
        scientific_name_list.append(child["scientificname"])
        get_child_if_dict(child["child"], parent_id=parent_id, parent_list=parent_list, aphia_id_list=aphia_id_list, scientific_name_list=scientific_name_list, rank_list=rank_list)
    else:
        parent_list.append(parent_id)
        aphia_id_list.append(child["AphiaID"])
        scientific_name_list.append(child["scientificname"])
        return
    

        
#function that will take response of api call and the current cache and will update cache according to info in response
def update_cache(response, cache, parent_id):
    #get the child of the response
    child = response
    parent_list= []
    aphia_id_list = []
    scientific_name_list = []
    rank_list = []
    parent_id = ''
    get_child_if_dict(child, parent_id, parent_list, aphia_id_list, scientific_name_list, rank_list)
    # for i in parent list and aphia id list, check if they are in cache, if not, add them to cache
    length_ids = len(aphia_id_list)
    i = 0
    while i < length_ids:
        aphia_id = aphia_id_list[i]
        parent = parent_list[i]
        scientific_name = scientific_name_list[i]
        rank= rank_list[i]
        # check if aphia id is in cache
        if str(aphia_id) not in cache:
            #add the aphia id to cache
            cache[str(aphia_id)] = {
                "scientificname": scientific_name,
                "aphiaid": aphia_id,
                "rank": rank,
                "parent": parent,
                "children": 0,
                "directchildren": 0
            }
            
            if parent != '' and i != 0:
                cache[str(parent)]["children"] += 1
                cache[str(parent)]["directchildren"] += 1
        else:
            #if parent is not '' and i is not 0, change cache of parent
            if parent != '' and i != 0:
                cache[str(parent)]["children"] += 1
        i+=1


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

def open_cache_file(cache_file):
    f = open(os.path.join(begin_path, cache_file), 'r')
    return json.load(f)

def write_cache_data(cache, cache_file):
    utf_8_encoded_dict_encoded= {str(k).encode("utf-8"): str(v).encode("utf-8") for k,v in cache.items()}
    utf_8_encoded_dict = {k.decode("utf-8"): v.decode("utf-8") for k,v in utf_8_encoded_dict_encoded.items()}
    new_dict = {}
    for key, value in utf_8_encoded_dict.items():
        new_dict[key] = ast.literal_eval(value.replace("'", "\""))
    with open(os.path.join(begin_path, cache_file), 'w') as f:
        json.dump(new_dict, f)
'''
#print the children dataframes
print(children)
try:
    #read in the data from the csv file with pandas
    f = open(os.path.join(begin_path, 'data_object.json'), 'r')
    cached_data = json.load(f)
except:
    with open('data_object.json', 'w') as f:
        print("The json cache file is created")
    refresh_cache = True
#make requests to the aphia api to get the taxonomy of the aphia_ids
if refresh_cache:
    cached_data = {}
for dasid, aphia_ids in children.items():
    print(f"beginning search on dasid: {dasid}")
    length_aphia_ids = len(aphia_ids)
    i = 1
    covered_percentage = 0
    for aphia_id in aphia_ids:
        cached_data = open_cache_file(cache_file='data_object.json')
        #print(cached_data)
        try:
            cached_data[str(dasid)]["test"] = []
            #delete test key from cache
            cached_data[str(dasid)].pop("test", None)
        except Exception as e:
            print(e)
            cached_data[str(dasid)] = {}
        
        try:
            cached_data[str(dasid)]["urls_done"].append("test")
            cached_data[str(dasid)]["urls_done"].pop()
        except Exception as e:
            print(e)
            cached_data[str(dasid)]["urls_done"]= []
            
        try:
            cached_data[str(dasid)]["data"]["test"] = "test"
            #delete the test key
            cached_data[str(dasid)]["data"].pop("test")
        except Exception as e:
            print(e)
            cached_data[str(dasid)]["data"] = {}
            
        
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
            if aphia_id in cached_data[str(dasid)]:
                pass
            else:
                url_to_request = 'https://www.marinespecies.org/rest/AphiaClassificationByAphiaID/' + str(aphia_id)
                #print(cached_data)
                if url_to_request not in cached_data[str(dasid)]["urls_done"]:
                    aphia_id_data = requests.get(url_to_request).json()
                    #put function to propagate the taxonomy into the cache
                    update_cache(aphia_id_data, cached_data[str(dasid)]["data"], parent_id="")
                    #cached_data[dasid][aphia_id] = aphia_id_data
                    cached_data[str(dasid)]["urls_done"].append(url_to_request)
                    write_cache_data(cached_data, cache_file='data_object.json')
                    time.sleep(1.5)
        except:
            url_to_request = 'https://www.marinespecies.org/rest/AphiaClassificationByAphiaID/' + str(aphia_id)
            #print(cached_data)
            if url_to_request not in cached_data[str(dasid)]["urls_done"]:
                aphia_id_data = requests.get(url_to_request).json()
                update_cache(aphia_id_data, cached_data[str(dasid)]["data"], parent_id="")
                #cached_data[dasid][aphia_id] = aphia_id_data
                cached_data[str(dasid)]["urls_done"].append(url_to_request)
                write_cache_data(cached_data, cache_file='data_object.json')
                time.sleep(1.5)
'''
 
 
def add_nodes_via_relevancy(index, final_ids, all_data, sorted_index_highest_relevance):
    #get the first node from the sorted index and add it to the list of nodes
    try:
        aphia_id_highest_relevance = sorted_index_highest_relevance[index]["aphia_id"]
    except Exception as e:
        print(e)
        print(index)
        return
    #go over the all_data and find all the nodes that have the aphia_id_highest_relevance as a parent
    added_ids = []
    #get a list of all the aphia_ids in the final_ids
    all_final_ids = []
    for aphia_id in final_ids:
        all_final_ids.append(aphia_id)
    
    for node, node_value in all_data.items():
        if node_value["parent"] == aphia_id_highest_relevance:
            if node not in all_final_ids:
                added_ids.append(node)
    if len(added_ids) == 0:
        index += 1
        add_nodes_via_relevancy(index, final_ids, all_data, sorted_index_highest_relevance)
    
    #check if the length of the added_ids is bigger than the number of nodes
    if len(added_ids)+len(final_ids) > nodes:
        print("nodes to long , trying another node")
        index = index + 1
        add_nodes_via_relevancy(index, final_ids, all_data, sorted_index_highest_relevance)
    else:
        for to_add_id in added_ids:
            final_ids[to_add_id] = all_data[to_add_id]
 
#read the cache file and print the data
cached_data = open_cache_file(cache_file='data_object.json')
for dasid, data in cached_data.items():
    print(f"dasid: {dasid}")
    current_nodes = 0
    final_ids = {}
    all_data = data["data"]
    #sort out the data by number of children
    sorted_index = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
    print(sorted_index)
    #get the first node from the sorted index and add it to the list of nodes
    final_ids[sorted_index[0]] = all_data[sorted_index[0]]
    while len(final_ids) < nodes:   
        #get the children of all the nodes in the final_ids and get the ammount of direct children they have
        relevancy_list=[]
        for node, node_value in final_ids.items():
            #get child value and direct child value
            children = node_value["children"]
            direct_children = node_value["directchildren"]
            
            #determine relevancy of the node
            try:
                relevancy = children/direct_children
            except:
                relevancy = 0
            relevancy_list.append({"aphia_id":node_value["aphiaid"],"relevancy":relevancy})
        #get sorted list of all the children of the node with the highest relevancy
        sorted_index_highest_relevance = sorted(relevancy_list, key=lambda x: x["relevancy"], reverse=True)
        print(sorted_index_highest_relevance)
        current_relevancy_index = 0
        add_nodes_via_relevancy(current_relevancy_index, final_ids, all_data, sorted_index_highest_relevance)
        #print(final_ids)
        
print(final_ids)    
            
    







































'''
      
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
        
def get_direct_children(aphia_id, parent_aphia_id):
    #get the direct children of the parent_aphia_id
    direct_children = []
    for dasid, aphia_ids in children.items():
        if parent_aphia_id in aphia_ids:
            direct_children.append(dasid)

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
    relevancy_list = []
    direct_children = []
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
    
    #calculate the relevancy of each aphia_id
    for aphia_id in unique_aphia_ids:
        try:
            
            
            #get the index of the aphia_id in the character array
            index_relevancy_calc = character.index(aphia_id)
            #get the parent of the aphia_id
            parent_relevancy_calc = parent_aphia_ids[index_relevancy_calc]
            #get the index of the parent in the character array
            parent_index = character.index(parent_relevancy_calc)
            #get the value of the index in the value_dict array
            value_relevancy_calc = value_dict[parent_index]
            #calculate the relevancy of the aphia_id
            relevancy_calc = 1 / value_relevancy_calc
            #append to the relevancy_list
            relevancy_list.append(relevancy_calc)
        except:
            relevancy_list.append(0)
        
        
        
    
    complete_data_dasids[dasid] = {'character': character, 'values': value_dict, 'parent_aphia_ids': parent_aphia_ids, 'scientific_names': scientific_names, 'rank': ranks, 'relevancy': relevancy_list}
    
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
        
        print(len(rank_dict[key]))
        if key == "value_species":
            print(rank_dict[key])
        
        if key != "value_species":
            path_list.append(key)
    
     fig = px.sunburst(
        rank_dict,
        path=path_list,
        values='value_species',
        color="Order"
    )
    fig.show()
    
   
    
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
        relevancy = dicts_complete_values['relevancy'][i]
        rank_value = 0
        for str_rank, value_rank in rank_values.items():
            if str_rank in rank_string_id.upper():
                rank_value = value_rank
        prefix_rank = 0
        for prefix_rank, value_prefix in rank_prefixes.items():
            if prefix_rank in rank_string_id.upper():
                prefix_value = value_prefix
        display_value = rank_value + prefix_value
        is_species = 0
        if "SPECIES" in rank_string_id.upper():
            is_species = 1
        
        todisplay = 1
        if max_rank_value > display_value:
            todisplay = 0
        
        weight_formula = (display_value*todisplay*value*relevancy)
        if is_species == 0:
            weight_formula = weight_formula*(1-covered_percentage)
        if dasid not in weights_dasids:
            weights_dasids[dasid] = []
        weights_dasids[dasid].append({'aphia_id': aphia_id, 'scientific_name': scientific_name, 'weight': weight_formula, 'rank': rank_string_id, 'relevancy': relevancy})
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
    csv_file = []
    while i < nodes:
        try:
            csv_file.append({"aphia_id": values_weights[sorted_indexes[i]]['aphia_id'], "scientific_name": values_weights[sorted_indexes[i]]['scientific_name'], "weight": values_weights[sorted_indexes[i]]['weight'], "rank": values_weights[sorted_indexes[i]]['rank']})
            print(f"dasid: {dasid} | aphia_id: {values_weights[sorted_indexes[i]]['aphia_id']} | scientific_name: {values_weights[sorted_indexes[i]]['scientific_name']} | weight: {values_weights[sorted_indexes[i]]['weight']} | rank: {values_weights[sorted_indexes[i]]['rank']}")
            i = i+1
        except:
            i = i+1
    #write to csv
    with open(f"{dasid}_chosen_nodes.csv", 'w', newline='') as csvfile:
        fieldnames = ['aphia_id', 'scientific_name', 'weight', 'rank', 'relevancy']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_file)
        
    #write a csv file that contains all the nodes of the dasid
    with open(f"{dasid}_all_nodes.csv", 'w', newline='') as csvfile:
        fieldnames = ['aphia_id', 'scientific_name', 'weight', 'rank', 'relevancy']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(values_weights)
'''