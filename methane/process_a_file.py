import cftime

import netCDF4

from methane.utilities import get_file_dataset,storable_metadata_json
from core.database import Db

def process(file_names:list[str],args:dict[str,str],nodb:bool,verbose:bool):
    if verbose:
        print(f"Ready to process {len(file_names)} files.")
        if verbose:
            if nodb:
                print("No db updates will be attempted")
            else:
                print("The db will be updated")
    for file_name in file_names:
        if verbose:
            print(f"Processing {file_name}")
        ds=get_file_dataset(file_name)
        json_descr=storable_metadata_json(ds)
        num_lats=ds.dimensions.get('lat').size
        num_lons=ds.dimensions.get('lon').size
        db=Db(args=args)
        times={}
        times["units"]="seconds since 1970-1-1 0:0:0.0"
        times["calendar"] = "gregorian"
        methane_data_file_id=db.insert_and_get_id(f'INSERT INTO methane_data_file (file_name,metadata) values (%s,%s) RETURNING methane_data_file_id',
                  (file_name,json_descr,))
        records = 0
        for lon in range(0,num_lons-1):
            for lat in range(0,num_lats-1):
                if True:
                    records+=1

                    my_cftime=netCDF4.num2date(ds['time'][lat, lon].item(),
                                                       units=times['units'],
                                                       calendar=times['calendar'],
                                                       only_use_cftime_datetimes=False)
                    db.insert(f'INSERT INTO methane_data (methane_data_file_id,recorded_at,latitude,longitude,methane) values (%s,%s,%s,%s,%s)',
                              (methane_data_file_id,
                                      my_cftime,
                                      ds['lat'][lat].item(),
                                      ds['lon'][lon].item(),
                                      ds['xch4'][lat, lon].item(),))
        print(f'inserted {records} records')