#imports
import json 
import ast
from operator import le
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

amplifier = 10

#constants for the algorithm
rank_score_dict = {
    "DOMAIN": 8*amplifier,
    "KINGDOM": 7*amplifier,
    "PHYLUM": 6*amplifier,
    "CLASS": 5*amplifier,
    "ORDER": 4*amplifier,
    "FAMILY": 3*amplifier,
    "GENUS": 2*amplifier,
    "SPECIES": 1*amplifier
}

subrank_score_dict = {
    "MEGA": +0.4*amplifier,
    "GIGA": +0.3*amplifier,
    "SUPER": +0.2*amplifier,
    "SUB": -0.2*amplifier,
    "INFRA": -0.3*amplifier,
    "PARV": -0.4*amplifier
}

###################################################
############# HELPER FUNCTIONS BEGIN###############
###################################################

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
        rank_list.append(child["rank"])
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
        
###################################################
############# HELPER FUNCTIONS END#################
###################################################

###################################################
############# DASIDS WITH APHIAIDS ################
###################################################

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

#print the children dataframes
print(children)
dataframe_later_use = children

###################################################
############# DASIDS WITH APHIAIDS ################
###################################################

'''
###################################################
############### CACHING CODE BEGIN ################
###################################################
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
###################################################
################# CACHING CODE END ################
###################################################
'''

###################################################
############# ALGORITHM CODE BEGIN ################
###################################################
 
#read the cache file and print the data
cached_data = open_cache_file(cache_file='data_object.json')
for dasid, data in cached_data.items():
    print(f"dasid: {dasid}")
    current_nodes = 0
    final_ids = {}
    all_data = data["data"]
    #sort out the data by number of children
    sorted_index = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
    #print(sorted_index)
    #get the first node from the sorted index and add it to the list of nodes
    index = 0
    while index < 1:
        final_ids[sorted_index[index]] = all_data[sorted_index[index]]
        index += 1
    last_final_id_length = 0
    while len(final_ids) < nodes and len(final_ids) >= last_final_id_length:
        try:
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
                
                #determine the rank_value of the node
                rankupper = node_value["rank"].upper()
                main_rank_value = 0
                prefix_rank_value = 0
                for rank, rank_value in rank_score_dict.items():
                    #get len of rnak
                    len_rank = len(rank)
                    #get last len_rank char of the rankupper
                    spliced_rank = rankupper[-len_rank:]
                    if spliced_rank == rank.upper():
                        main_rank_value = rank_value
                
                for prefix_rank, prefix_rank_value in subrank_score_dict.items():
                    #get len of rnak
                    len_prefix_rank = len(prefix_rank)
                    #get first len_rank char of the rankupper
                    spliced_prefix_rank = rankupper[:len_prefix_rank]
                    if spliced_prefix_rank == prefix_rank.upper():
                        prefix_rank_value = prefix_rank_value
                true_rank_value = main_rank_value + prefix_rank_value
                
                relevancy_list.append({"aphia_id":node_value["aphiaid"],"relevancy":relevancy, "rank_value":true_rank_value})
            #sort the relevancy list by rank_value
            sorted_list_rank = sorted(relevancy_list, key=lambda x: x["rank_value"], reverse=True)
            #get the first node from the sorted list and add it to the list of nodes
            unchanged = True
            sorted_ranked_list_index = 0
            while unchanged:
                    max_ranked_node = sorted_list_rank[sorted_ranked_list_index]
                    #get the children of the node
                    all_childs = []
                    for aphia_id, aphia_id_value in all_data.items():
                        if str(max_ranked_node["aphia_id"]) == str(aphia_id_value["parent"]):
                            all_childs.append(aphia_id_value)
                    #check if the length of the children is greater + current length final ids than the number of nodes
                    if len(all_childs) > 0:
                        if len(all_childs) + len(final_ids) < nodes:
                            for child in all_childs:
                                final_ids[child["aphiaid"]] = child
                                #delete max ranked node from final_ids
                            print(f"{dasid} | {len(final_ids)}/{nodes} nodes found")
                            try:
                                final_ids.pop(str(max_ranked_node["aphia_id"]))
                            except:
                                final_ids.pop(max_ranked_node["aphia_id"])
                            last_final_id_length = len(final_ids)
                            unchanged = False
                        else:
                            sorted_ranked_list_index += 1
                    else:
                        sorted_ranked_list_index += 1
        except IndexError:
            break
    
    #convert the final_ids to a list of dict to then write to a csv file
    csv_list_final_ids = []
    for final_id , final_id_info in final_ids.items():
        csv_list_final_ids.append(final_id_info)
    #write the csv file
    with open(f"{dasid}_chosen_aphia_ids.csv", 'w', newline='') as csvfile:
        fieldnames = ['scientificname', 'aphiaid', 'rank', 'parent', 'children', 'directchildren']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_list_final_ids)
    print(f"{dasid} done")  
    ### print the tree view map of the final_ids ###
    #go over each row in the csv_list_final_ids get the scientific name and the parent and the aphiaid
    names = []
    scientific_names = []
    scientific_names_parents = []
    parents = []
    ranks = []
    childrens = []
    color = []
    for row in csv_list_final_ids:
        names.append(row["aphiaid"])
        scientific_names.append(row["scientificname"])
        parents.append(row["parent"])
        ranks.append(row["rank"])
        childrens.append(row["children"])
        for node, value_node in all_data.items():
            if str(row["parent"]) == node:
                scientific_names_parents.append(value_node["scientificname"])
                break
        if row["aphiaid"] in dataframe_later_use[int(dasid)]:
            color.append("pink")
        else:
            color.append("royalblue")
    
    #sort the keys of all_data by all_data[key]["children"]
    sorted_all_data = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
    #print(sorted_all_data)
    for node in sorted_all_data:
        #check if the node is in the list of names
        if int(node) not in names:
            names.append(all_data[node]["aphiaid"])
            scientific_names.append(all_data[node]["scientificname"])
            parents.append(all_data[node]["parent"])
            ranks.append(all_data[node]["rank"])
            childrens.append("not important")
            #check to color of the parent
            try:
                index_parent = names.index(all_data[node]["parent"])
                color_parent = color[index_parent]
                appended_color = 0
                if all_data[node]["aphiaid"] in dataframe_later_use[int(dasid)]:
                    appended_color = 1
                    color.append("red")
                else:
                    if appended_color == 0:
                        color.append("lightgrey")
            except Exception as e:
                print(e)
                color.append("lightgrey")
            
            for nodeu, value_nodeu in all_data.items():
                if str(all_data[node]["parent"]) == "":
                    scientific_names_parents.append("")
                    break
                if str(all_data[node]["parent"]) == nodeu:
                    scientific_names_parents.append(value_nodeu["scientificname"])
                    break
    #print(len(names))
    #print(len(parents))
    #print(color)
    #make tree fig
    
    fig = px.treemap(
    names = names,
    parents = parents,
    title=f"dasid {dasid} tree view {len(final_ids)}/{len(dataframe_later_use[int(dasid)])} (#final/#begin aphiaIDs)",
    color = color,
    color_discrete_map={
     '(?)': 'lightgrey',
     'lightgrey': 'lightgrey',
     'royalblue': 'royalblue',
     'red': 'red',
     'pink': 'pink'
    },
    hover_name=scientific_names,
    hover_data={"parent":scientific_names_parents, "rank":ranks, "children reduced":childrens}
    )
    fig.update_traces(root_color="lightgrey")
    fig.update_layout(margin = dict(t=25, l=10, r=10, b=10))
    fig.show()
    path_to_write = f"{dasid}_tree_view.html"
    #get the path to the current directory
    path_dir = os.path.dirname(os.path.abspath(__file__))
    fig.write_html(os.path.join(path_dir, path_to_write))
    
    
###################################################
############### ALGORITHM CODE END ################
###################################################    