from data_reader import load_dicts
import psutil
import pickle
import gc
import ray
load = {'Produccion'}

prod = load_dicts(load)['Produccion']

cod_ = ['ID_CUSTOMER', 'DATE', 'TELEPHONE', 'CALIFICACION', 'DESCRIPTION_COD1', 'COD1']
gestion = {key: value['GESTION'][cod_] for key, value in prod.items() if 'GESTION' in value}
cc_g = [set(value['ID_CUSTOMER']) for value in gestion.values()]
cc = set.union(*cc_g)

act_ = ['ID_CUSTOMER', 'DATE', 'CUSTOMER_PHONE', 'CUSTOMER_CHARS', 'DESCRIPTION_COD_ACT']
gestion_wp = {key: value['GESTION_WHATSAPP'][act_] for key, value in prod.items() if 'GESTION_WHATSAPP' in value}
cc_gw = [set(value['ID_CUSTOMER']) for value in gestion_wp.values()]
ccw = set.union(*cc_gw)

new_dict = {}

gestion_ = ray.put(gestion)
gestion_wp_ = ray.put(gestion_wp)
@ray.remote
def process_customer(id, data_dict, name:str):
    new_dict.setdefault(id, dict())
    for key in data_dict:
        key_ = data_dict[key]
        if id in set(key_['ID_CUSTOMER']):
            new_dict[id][(key[0],key[1], name)] = key_[key_['ID_CUSTOMER'] == id]

num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=int(num_cpus), ignore_reinit_error=True, include_dashboard=False)
refs = [process_customer.remote(id, gestion_, 'GESTION') for id in cc]
pis = ray.get(refs)
pis = [a for a in pis if a if a != "error"]
gc.collect()
while ray.is_initialized():
    ray.shutdown()
    print("Ray_Seguro1")
del pis
del refs

num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=int(num_cpus), ignore_reinit_error=True, include_dashboard=False)
refs = [process_customer.remote(id, gestion_wp_, 'GESTION_WHATSAPP') for id in ccw]
pis = ray.get(refs)
pis = [a for a in pis if a if a != "error"]
gc.collect()
while ray.is_initialized():
    ray.shutdown()
    print("Ray_Seguro2")
del pis
del refs
#
# for id in cc:
#     new_dict.setdefault(id, dict())
#     for key in gestion:
#         key_ = gestion[key]
#         if id in set(key_['ID_CUSTOMER']):
#             new_dict[id][(key[0],key[1],'GESTION')] = key_[key_['ID_CUSTOMER'] == id]
#
# for id in ccw:
#     new_dict.setdefault(id, dict())
#     for key in gestion_wp:
#         key_ = gestion_wp[key]
#         if id in set(key_['ID_CUSTOMER']):
#             new_dict[id][(key[0],key[1],'GESTION_WHATSAPP')] = key_[key_['ID_CUSTOMER'] == id]
#

with open("data/outputs/Productividad_Nuevo" +".pickle", 'wb') as handle:
    pickle.dump(new_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

