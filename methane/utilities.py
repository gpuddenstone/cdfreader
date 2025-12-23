import glob
import json

import  netCDF4 as nc

def walktree(top):
    yield top.groups.values()
    for value in top.groups.values():
        yield from walktree(value)

def get_files(name:str)->list[str]:
    file_names = glob.glob(name)
    if len(file_names)==0:
        print(f"no files found in {name}")
    return file_names

def get_file_dataset(filename:str)->nc.Dataset|None:
    try:
        return nc.Dataset(filename)
    except Exception as e:
        print(f"Could not process file {filename}, problem={e}")
        return None


def display_dataset_metadata(ds:nc.Dataset)->None:
    print(f'raw data set: \n{ds}')
    print('raw dict')

    print(f'dict \n{ds.__dict__}')

    print('GROUPS:')
    for children in walktree(ds):
        for child in children:
            print(child)

    print('DIMENSIONS:')
    for dim in ds.dimensions.values():
        print(dim)
    print('VARIABLES:')
    for var in ds.variables.values():
        print(var)


def get_metadata_to_store(dict_to_store:dict[str,dict[str,str]],ds:nc.Dataset,group_name:str)->None:
    for children in walktree(ds):
        for child in children:
            if child.name==group_name:
                for name,value in child.variables.items():
                    tdict={}
                    for attr in value.ncattrs():
                        tdict[attr]=str(value.getncattr(attr))
                    dict_to_store[name] = tdict


def accumulate_get_metadata_to_store(ds:nc.Dataset)->dict[str,dict[str,str]]:
    accumulator = {}
    get_metadata_to_store(accumulator, ds, 'apriori_data')
    get_metadata_to_store(accumulator, ds, 'geolocation')
    return accumulator


def storable_metadata_json(ds:nc.Dataset)->str:
    data = accumulate_get_metadata_to_store(ds)
    return json.dumps(data)

