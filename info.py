import sys
from data_reader import load_dicts

load = {'Pagos', 'Evolucion', 'Asignacion', 'Cartera', 'Productividad'}

dicts = load_dicts(load)


def info_by_id(id):
    """
    Retrieves all available information in the dicts by id
    :param id: personal identification of the client
    :return: dicts of the form d[id] if id in d
    """
    info_dict = {}
    for key in dicts:
        if id in dicts[key]:
            info_dict[key] = dicts[key][id]
        else:
            info_dict[key] = None

    return info_dict



if __name__ == '__main__':
    for key, value in info_by_id(sys.argv[1]).items():
        print(f"*******************------------------EN {key}-----------------********************")
        print(value)
