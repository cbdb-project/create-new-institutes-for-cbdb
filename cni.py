import pandas as pd
# pip install pypinyin
from pypinyin import lazy_pinyin
import requests
from bs4 import BeautifulSoup
import csv

def get_latest_inst_name_id(url):
    output_id = ""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    output_id = soup.find('input', {'name': 'c_inst_name_code'})['value']
    return output_id

def get_latest_inst_id(url):
    output_id = ""
    response = requests.get(url)
    last_page_url = response.json()['last_page_url']
    response = requests.get(last_page_url)
    output_id = response.json()['data'][-1]['c_inst_code']
    return output_id

input_pd = pd.read_csv('input.txt', delimiter='\t', encoding='utf-8', dtype=str, header=None)

input_name_list = [x.strip() for x in input_pd[0].tolist()]
input_type_list = input_pd[1].tolist()
input_dynasty_list = input_pd[2].tolist()
input_addr_id_list = [x.strip() for x in input_pd[4].tolist()]
input_source_list = [x.strip() for x in input_pd[5].tolist()]

latest_inst_name_id = ""
call_latest_inst_name_url = "https://input.cbdb.fas.harvard.edu/socialinstitutioncodes/create"
latest_inst_name_id = get_latest_inst_name_id(call_latest_inst_name_url)

latest_inst_id = ""
call_latest_inst_url = "https://input.cbdb.fas.harvard.edu/api/select/search/socialinstcode"
latest_inst_id = get_latest_inst_id(call_latest_inst_url)

dy_df = pd.read_csv('DYNASTIES.csv', delimiter=',', encoding='utf-8', dtype=str)
dynasty_dict = dict(zip(dy_df['c_dynasty_chn'], dy_df['c_dy']))

inst_type_df = pd.read_csv('SOCIAL_INSTITUTION_TYPES.csv', delimiter=',', encoding='utf-8', dtype=str)
inst_type_dict = dict(zip(inst_type_df['c_inst_type_hz'], inst_type_df['c_inst_type_code']))

print(latest_inst_name_id)
print(latest_inst_id)

inst_name_sql_list = []
inst_code_sql_list = []
inst_addr_sql_list = []

# check whether inst_name is in the database
for i in range(len(input_name_list)):
    inst_name = input_name_list[i]
    url = "https://input.cbdb.fas.harvard.edu/api/select/search/socialinstcode?q=" + inst_name
    response = requests.get(url)
    if response.json()['total'] != 0:
        existing_inst_name_filename = 'existing_inst_name.csv'
        with open(existing_inst_name_filename, 'w', encoding='utf-8') as f:
            f.write('')
        data_list = [response.json()['data']][0]
        column_names = data_list[0].keys()
        # print(column_names)
        with open(existing_inst_name_filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            writer.writeheader()
            for row in data_list:
                writer.writerow(row)
        # raise Exception('Find existing inst_name, please check existing_inst_name.csv')

for i in range(len(input_name_list)):
    inst_name_sql = ""
    inst_code_sql = ""
    inst_addr_sql = ""
    new_inst_name_id = int(latest_inst_name_id) + i + 1
    new_inst_id = int(latest_inst_id) + i + 1
    inst_name = input_name_list[i]
    inst_type_id = inst_type_dict[input_type_list[i]]
    inst_dynasty_id = dynasty_dict[input_dynasty_list[i]]
    inst_addr_id = input_addr_id_list[i]
    inst_source = input_source_list[i]
    inst_name_pinyin = ' '.join(lazy_pinyin(inst_name))
    inst_name_sql = "INSERT INTO `SOCIAL_INSTITUTION_NAME_CODES` (`c_inst_name_code`, `c_inst_name_hz`, `c_inst_name_py`) VALUES ('" + str(new_inst_name_id) + "', '" + inst_name + "', '" + inst_name_pinyin + "');"
    inst_code_sql = "INSERT INTO `SOCIAL_INSTITUTION_CODES` (`c_inst_name_code`, `c_inst_code`, `c_inst_type_code`, `c_inst_begin_dy`, `c_source`) VALUES ('" + str(new_inst_name_id) +  "', '" + str(new_inst_id) + "', '" + inst_type_id + "', '" + inst_dynasty_id + "', '" + inst_source + "');"
    inst_addr_sql = "INSERT INTO `SOCIAL_INSTITUTION_ADDR` (`c_inst_name_code`, `c_inst_code`, `c_inst_addr_type_code`, `c_inst_addr_id`, `inst_xcoord`, `inst_ycoord`, `c_source`) VALUES ('" + str(new_inst_name_id) + "', '" + str(new_inst_id) + "', '1', '" + inst_addr_id + "', '0', '0', '" + inst_source + "');"
    inst_name_sql_list.append(inst_name_sql)
    inst_code_sql_list.append(inst_code_sql)
    inst_addr_sql_list.append(inst_addr_sql)

with open('output_sql.txt', 'w', encoding='utf-8') as f:
    for i in range(len(inst_name_sql_list)):
        f.write(inst_name_sql_list[i] + '\n')
        f.write(inst_code_sql_list[i] + '\n')
        f.write(inst_addr_sql_list[i] + '\n')

print('Finished!')