from data_reader import load_dicts
from collections import defaultdict
import psutil
import pickle
from tqdm import tqdm
import gc
import ray
import random

load = {'Produccion'}

prod = load_dicts(load)['Produccion']

cod_ = ['ID_CUSTOMER', 'DATE', 'TELEPHONE', 'CALIFICACION', 'DESCRIPTION_COD1', 'COD1']
gestion = {key: value['GESTION'][cod_] for key, value in prod.items() if 'GESTION' in value}
cc_g = [set(value['ID_CUSTOMER']) for value in gestion.values()]
tuples1 = [(key, row, 'GESTION')
           for key, value in tqdm(gestion.items(), colour="green")
           for index, row in value.iterrows() if row['ID_CUSTOMER'] != 'nan']

act_ = ['ID_CUSTOMER', 'DATE', 'CUSTOMER_PHONE', 'CUSTOMER_CHARS', 'DESCRIPTION_COD_ACT']
gestion_wp = {key: value['GESTION_WHATSAPP'][act_] for key, value in prod.items() if 'GESTION_WHATSAPP' in
              value}
cc_gw = [set(value['ID_CUSTOMER']) for value in gestion_wp.values()]
ccw = set.union(*cc_gw)
tuples2 = [(key, row, 'GESTION_WHATSAPP')
           for key, value in tqdm(gestion_wp.items(), colour="green")
           for index, row in value.iterrows() if row['ID_CUSTOMER'] != 'nan']

new_dict = {}

tuples = tuples1 + tuples2

cols = {'GESTION': cod_[1:],
        'GESTION_WHATSAPP': act_[1:]}


@ray.remote
def process_customer(t: tuple):
    row = t[1]
    customer_id = row['ID_CUSTOMER']
    key = t[0]
    customer_info = tuple(row[cols[t[2]]])
    return {(customer_id, key, t[2]): {customer_info}}


num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=int(num_cpus), ignore_reinit_error=True, include_dashboard=False)
refs = [process_customer.remote(tu) for tu in tqdm(tuples, colour="yellow")]
pis = ray.get(refs)
pis = [a for a in pis if a if a!= "error"]
print('Wrapping results')
# Create a defaultdict to store the merged values
merged_dict = defaultdict(set)

# Iterate over each dictionary in the list
for d in tqdm(pis,colour= "red"):
    # Iterate over each key-value pair in the dictionary
    for k, v in d.items():
        # Update the merged_dict with the set union of the current value and the existing value for the key
        merged_dict[k] |= v

# Convert the defaultdict back to a regular dictionary
merged_dict = dict(merged_dict)
gc.collect()
while ray.is_initialized():
    ray.shutdown()
print("Ray_Seguro")

for k, v in tqdm(merged_dict.items(), colour="red"):
    new_dict.setdefault(k[0], dict())
    new_dict[k[0]][k[1:]] = v
# Get a random key-value pair
random_key, random_value = random.choice(list(new_dict.items()))

print("Random key:", random_key)
print("Random value:", random_value)
print('writing the results')
with open("data/outputs/Productividad_Nuevo" + ".pickle", 'wb') as handle:
    pickle.dump(new_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
