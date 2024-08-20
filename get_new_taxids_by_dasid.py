import argparse
import pyodbc
import pandas as pd
import time
import json
import csv
import os

### CONSTANTS ###

cnxn_str_aphia = (
    "Driver={SQL Server Native Client 11.0};"
    "Server=SQL17STAGE;"
    "Database=aphia;"
    "UID=Anyone;"
)

cnxn_str_imis = (
    "Driver={SQL Server Native Client 11.0};"
    "Server=SQL17;"
    "Database=IMIS;"
    "UID=Anyone;"
)

cnxn_aphia = pyodbc.connect(cnxn_str_aphia)
cnxn_imis = pyodbc.connect(cnxn_str_imis)
cursor_aphia = cnxn_aphia.cursor()
cursor_imis = cnxn_imis.cursor()

amplifier = 10

# constants for the algorithm
rank_score_dict = {
    "DOMAIN": 8 * amplifier,
    "KINGDOM": 7 * amplifier,
    "PHYLUM": 6 * amplifier,
    "CLASS": 5 * amplifier,
    "ORDER": 4 * amplifier,
    "FAMILY": 3 * amplifier,
    "GENUS": 2 * amplifier,
    "SPECIES": 1 * amplifier,
}

subrank_score_dict = {
    "MEGA": +0.4 * amplifier,
    "GIGA": +0.3 * amplifier,
    "SUPER": +0.2 * amplifier,
    "SUB": -0.2 * amplifier,
    "INFRA": -0.3 * amplifier,
    "PARV": -0.4 * amplifier,
}

### END CONSTANTS ###


def get_arg_parer():
    """
    Get argument parser
    """

    parser = argparse.ArgumentParser(description="Get new taxids by DAS id")

    parser.add_argument("-i", "--input", help="Input dasid", required=True)

    return parser


class TaxonInfoDasid:
    def __init__(self, DasID):
        self.DasID = DasID
        self.cache = {}
        self.taxon_info_cache = {}
        self.N = 20
        self.final_ids = {}
        self.output_file = f"output_{self.DasID}.csv"

    def get_taxids_by_dasid(self):
        """query to execute to get taxids by dasid
        SELECT dt.[DasID]
            ,dt.[TaxtID]
            ,tt.AphiaID

        FROM [IMIS].[dbo].[Das_Taxt] as dt
        LEFT JOIN [IMIS].[dbo].[TaxTerms] as tt on dt.TaxtID = tt.TaxtID
        Where dt.DasID like '4221'

        4221 is the dasid in this example
        """
        query = (
            "SELECT tt.AphiaID FROM [IMIS].[dbo].[Das_Taxt] as dt LEFT JOIN [IMIS].[dbo].[TaxTerms] as tt on dt.TaxtID = tt.TaxtID Where dt.DasID like '"
            + str(self.DasID)
            + "'"
        )

        # read the query data into an array

        data = pd.read_sql(query, cnxn_imis)
        time.sleep(1.5)
        # print the dataframe
        print(data)
        return data

    def get_json_for_taxid(self, taxid):
        """query to execute is:
            WITH rel AS (
        SELECT
            tu.id,
            tu_name,
            tu_displayname,
            rank_name,
            tu_parent,
            tu.tu_rank,
            tu_fossil,
            tu_hidden,
            tu_qualitystatus,
            0 as dlevel
        FROM
            tu WITH (NOLOCK)
            INNER JOIN ranks WITH (NOLOCK) ON (
            tu_rank = rank_id
            AND kingdom_id = 2
            )
        WHERE
            id IN (118852)
        UNION ALL
        SELECT
            tu.id,
            tu.tu_name,
            tu.tu_displayname,
            ranks.rank_name,
            tu.tu_parent,
            tu.tu_rank,
            tu.tu_fossil,
            tu.tu_hidden,
            tu.tu_qualitystatus,
            dlevel + 1
        FROM
            tu WITH (NOLOCK)
            INNER JOIN rel ON rel.tu_parent = tu.id
            INNER JOIN ranks WITH (NOLOCK) ON (
            tu.tu_rank = rank_id
            AND kingdom_id = 2
            )
        WHERE
            rel.tu_parent IS NOT NULL
        )
        SELECT
        id,
        tu_name,
        tu_displayname as text,
        rank_name as rank,
        tu_rank,
        tu_fossil,
        tu_hidden,
        tu_qualitystatus
        FROM
        rel
        ORDER BY
        dlevel DESC

        118852 is the taxid in this example
        """

        # check if the taxid is in the cache
        if taxid in self.cache:
            return self.cache[taxid]

        print(f"Getting taxid {taxid}")

        query = (
            "WITH rel AS ( SELECT tu.id, tu_name, tu_displayname, rank_name, tu_parent, tu.tu_rank, tu_fossil, tu_hidden, tu_qualitystatus, 0 as dlevel FROM tu WITH (NOLOCK) INNER JOIN ranks WITH (NOLOCK) ON ( tu_rank = rank_id AND kingdom_id = 2 ) WHERE id IN ("
            + str(taxid)
            + ") UNION ALL SELECT tu.id, tu.tu_name, tu.tu_displayname, ranks.rank_name, tu.tu_parent, tu.tu_rank, tu.tu_fossil, tu.tu_hidden, tu.tu_qualitystatus, dlevel + 1 FROM tu WITH (NOLOCK) INNER JOIN rel ON rel.tu_parent = tu.id INNER JOIN ranks WITH (NOLOCK) ON ( tu.tu_rank = rank_id AND kingdom_id = 2 ) WHERE rel.tu_parent IS NOT NULL ) SELECT id, tu_name, tu_displayname as text, rank_name as rank, tu_rank, tu_fossil, tu_hidden, tu_qualitystatus FROM rel ORDER BY dlevel DESC"
        )

        # read the query data into an array
        try:
            data = pd.read_sql(query, cnxn_aphia)
        except Exception as e:
            print(e)
            return None

        time.sleep(0.1)  # 10 per second
        # print the dataframe
        print(data)

        json_data = self._df_taxon_info_to_json(data)
        print(json_data)

        final_json = {
            "AphiaID": "1",
            "rank": "Superdomain",
            "scientificname": "Biota",
            "child": json_data,
        }
        # pprint the json
        print(json.dumps(dict(final_json), indent=4))

        # function here to convert the dataframe to json
        self.cache[str(taxid)] = final_json

        return json_data

    def get_child_if_dict(
        self,
        child,
        parent_id,
        parent_list,
        aphia_id_list,
        scientific_name_list,
        rank_list,
    ):
        if isinstance(child["child"], dict):
            parent_list.append(parent_id)
            aphia_id_list.append(child["AphiaID"])
            parent_id = child["AphiaID"]
            rank_list.append(child["rank"])
            scientific_name_list.append(child["scientificname"])
            self.get_child_if_dict(
                child["child"],
                parent_id=parent_id,
                parent_list=parent_list,
                aphia_id_list=aphia_id_list,
                scientific_name_list=scientific_name_list,
                rank_list=rank_list,
            )
        else:
            parent_list.append(parent_id)
            aphia_id_list.append(child["AphiaID"])
            rank_list.append(child["rank"])
            scientific_name_list.append(child["scientificname"])
            return

    def update_cache(self, response, parent_id):
        # get the child of the response
        child = response
        parent_list = []
        aphia_id_list = []
        scientific_name_list = []
        rank_list = []
        parent_id = ""
        self.get_child_if_dict(
            child,
            parent_id,
            parent_list,
            aphia_id_list,
            scientific_name_list,
            rank_list,
        )
        # for i in parent list and aphia id list, check if they are in cache, if not, add them to cache
        length_ids = len(aphia_id_list)
        i = 0
        while i < length_ids:
            aphia_id = aphia_id_list[i]
            parent = parent_list[i]
            scientific_name = scientific_name_list[i]
            rank = rank_list[i]
            # check if aphia id is in cache
            if str(aphia_id) not in self.taxon_info_cache:
                # add the aphia id to cache
                self.taxon_info_cache[str(aphia_id)] = {
                    "scientificname": scientific_name,
                    "aphiaid": aphia_id,
                    "rank": rank,
                    "parent": parent,
                    "children": 0,
                    "directchildren": 0,
                }

                if parent != "" and i != 0:
                    self.taxon_info_cache[str(parent)]["children"] += 1
                    self.taxon_info_cache[str(parent)]["directchildren"] += 1
            else:
                # if parent is not '' and i is not 0, change cache of parent
                if parent != "" and i != 0:
                    self.taxon_info_cache[str(parent)]["children"] += 1
            i += 1

    def _df_taxon_info_to_json(self, df: pd.DataFrame, index: int = 0):
        """
        Convert taxon info dataframe to json
        each row in the dataframe is a taxon.
        each row will look like the following:
        {
            "AphiaID": 118852,
            "rank": "species",
            "scientific_name": "Pseudocorynactis caribbeorum",
            "child": { same info as above }
        }

        example:
           id   tu_name      text     rank  tu_rank  tu_fossil  tu_hidden  tu_qualitystatus
            0   2  Animalia  Animalia  Kingdom       10        NaN          0                 0
            1   51  Mollusca  Mollusca   Phylum       30        3.0          0                 3

        {
            "AphiaID": 1,
            "rank": "Superdomain",
            "scientific_name": "Biota",
            "child": {
                "AphiaID": 2,
                "rank": "Kingdom",
                "scientific_name": "Animalia",
                "child": {
                    "AphiaID": 51,
                    "rank": "Phylum",
                    "scientific_name": "Mollusca",
                    "child": null
                }
            }
        }

        the next row will be the child of the previous row, for the last row
        the child will be null
        this function will be called recursively to build the json
        """
        if index < (len(df) - 1):
            row = df.iloc[index]
            json = {
                "AphiaID": str(row["id"]),
                "rank": str(row["rank"]),
                "scientificname": str(row["text"]),
                "child": self._df_taxon_info_to_json(df, index + 1),
            }
        else:
            row = df.iloc[index]
            json = {
                "AphiaID": str(row["id"]),
                "rank": str(row["rank"]),
                "scientificname": str(row["text"]),
                "child": None,
            }

        return json

    def reduce_taxa_info(self):
        """
        Reduce the taxons to a given N number of taxons
        """
        current_nodes = 0
        final_ids = {}
        sorted_index = sorted(
            self.taxon_info_cache.keys(),
            key=lambda x: self.taxon_info_cache[x]["children"],
            reverse=True,
        )
        # print(sorted_index)
        # get the first node from the sorted index and add it to the list of nodes
        index = 0
        while index < 1:
            final_ids[sorted_index[index]] = self.taxon_info_cache[sorted_index[index]]
            index += 1
        last_final_id_length = 0
        while len(final_ids) < self.N and len(final_ids) >= last_final_id_length:
            try:
                relevancy_list = []
                for node, node_value in final_ids.items():
                    # get child value and direct child value
                    children = node_value["children"]
                    direct_children = node_value["directchildren"]
                    # determine relevancy of the node
                    try:
                        relevancy = children / direct_children
                    except:
                        relevancy = 0

                    # determine the rank_value of the node
                    rankupper = node_value["rank"].upper()
                    main_rank_value = 0
                    prefix_rank_value = 0
                    for rank, rank_value in rank_score_dict.items():
                        # get len of rnak
                        len_rank = len(rank)
                        # get last len_rank char of the rankupper
                        spliced_rank = rankupper[-len_rank:]
                        if spliced_rank == rank.upper():
                            main_rank_value = rank_value

                    for prefix_rank, prefix_rank_value in subrank_score_dict.items():
                        # get len of rnak
                        len_prefix_rank = len(prefix_rank)
                        # get first len_rank char of the rankupper
                        spliced_prefix_rank = rankupper[:len_prefix_rank]
                        if spliced_prefix_rank == prefix_rank.upper():
                            prefix_rank_value = prefix_rank_value
                    true_rank_value = main_rank_value + prefix_rank_value

                    relevancy_list.append(
                        {
                            "aphia_id": node_value["aphiaid"],
                            "relevancy": relevancy,
                            "rank_value": true_rank_value,
                        }
                    )
                # sort the relevancy list by rank_value
                sorted_list_rank = sorted(
                    relevancy_list, key=lambda x: x["rank_value"], reverse=True
                )
                # get the first node from the sorted list and add it to the list of nodes
                unchanged = True
                sorted_ranked_list_index = 0
                while unchanged:
                    max_ranked_node = sorted_list_rank[sorted_ranked_list_index]
                    # get the children of the node
                    all_childs = []
                    for aphia_id, aphia_id_value in self.taxon_info_cache.items():
                        if str(max_ranked_node["aphia_id"]) == str(
                            aphia_id_value["parent"]
                        ):
                            all_childs.append(aphia_id_value)
                    # check if the length of the children is greater + current length final ids than the number of nodes
                    if len(all_childs) > 0:
                        if len(all_childs) + len(final_ids) < self.N:
                            for child in all_childs:
                                final_ids[child["aphiaid"]] = child
                                # delete max ranked node from final_ids
                            print(
                                f"{self.DasID} | {len(final_ids)}/{self.N} nodes found"
                            )
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

        # convert the final_ids to a list of dict to then write to a csv file
        csv_list_final_ids = []
        for final_id, final_id_info in final_ids.items():
            csv_list_final_ids.append(final_id_info)

        self.final_ids = final_ids

        return csv_list_final_ids

    def write_to_csv(self, csv_list_final_ids):
        """
        Write the final ids to a csv file
        """
        with open(self.output_file, mode="w") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "aphiaid",
                    "scientificname",
                    "rank",
                    "parent",
                    "children",
                    "directchildren",
                ]
            )
            for final_id in csv_list_final_ids:
                writer.writerow(
                    [
                        final_id["aphiaid"],
                        final_id["scientificname"],
                        final_id["rank"],
                        final_id["parent"],
                        final_id["children"],
                        final_id["directchildren"],
                    ]
                )


def main():
    """
    Main function
    """

    parser = get_arg_parer()
    args = parser.parse_args()
    cache = {}

    taxoninfo = TaxonInfoDasid(args.input)

    all_taxids = taxoninfo.get_taxids_by_dasid()

    for index, row in all_taxids.iterrows():
        json_info = taxoninfo.get_json_for_taxid(row["AphiaID"])
        if json_info is not None:
            taxoninfo.update_cache(json_info, parent_id="")

    reduced_taxa_info = taxoninfo.reduce_taxa_info()
    taxoninfo.write_to_csv(reduced_taxa_info)


if __name__ == "__main__":
    main()
