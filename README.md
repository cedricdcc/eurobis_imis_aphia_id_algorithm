# eurobis_imis_aphia_id_algorithm
algorithm to determine whioch aphia ids get to be used 

## HOW TO USE ##

first time use => perform the follwoing commands
+ go into to the root of this github repo on your local disk
+ run the following command in the terminal
```
pip install virtualenv
python -m venv venv
source venv/Source/Activate
pip install -r requirements.txt
```
open the python file and let it run 
+ go into to the root of this github repo on your local disk
+ run the following command in the terminal
```
python demo_algorithm.py
```

### How It Works ###

* The script loads in the aphia_ids_to_imis.csv
* Aphia_ids get sorted per dasid
* All Aphia_ids get querried via a rest call
* Output of this call gets put into a json file that is the cache
* The cache gets loaded in by hte algorithm part of the script
* the algorithm works as followed
  * The root of the tree gets found by counting all the children and direct children => gets put into a list called final_ids
  * The max relevance gets calculated per aphia_id in the final_ids
  * The number of children get counted per id in the final_ids
  * children get added and parent gets deleted from the list if requirements are met
  * repeat untill as deep as possible or if max nodes are reached
*
