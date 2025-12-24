import json
import os.path
from datetime import datetime, timezone
import sys
import psycopg2
import netCDF4
from dataclasses import dataclass
from os import path
import  netCDF4 as nc
import xmltodict
from methane.utilities import get_file_dataset,storable_metadata_json
from core.database import Db



@dataclass
class metadata_for_file:
    file_name: str = ''
    ds: nc.Dataset = None,
    num_lats: int = 0
    num_lons: int = 0
    methane_data_file_id:int=0
    processed: bool=False




def add_file_record(file_name:str,
            args:dict[str,str],
            nodb:bool=False,
            verbose:bool=False)->metadata_for_file:
    """
    Add all the records for one data file
    :param file_name:
    :param args:
    :param nodb:
    :param verbose:
    :return:
    """
    if not path.exists(file_name):
        print(f'could not find file {file_name}')
        return metadata_for_file(file_name=file_name)
    #  first try to open the cdf and get what we can
    ds=get_file_dataset(file_name)
    json_descr=storable_metadata_json(ds)
    num_lats=ds.dimensions.get('lat').size
    num_lons=ds.dimensions.get('lon').size
    db=None if nodb else Db(args=args)
    # see if there is an associated xml file, and if so then process it
    filename,extension=path.splitext(file_name)
    poss_xml_file=filename+'.xml'
    xmljson = None
    insert_str = f'INSERT INTO methane_data_file (file_name,metadata) values (%s,%s) RETURNING methane_data_file_id'
    parms=(file_name, json_descr,)
    if os.path.exists(poss_xml_file):
        with open(poss_xml_file) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
            xmljson = json.dumps(data_dict)
        if len(xmljson):
            insert_str=f'INSERT INTO methane_data_file (file_name,metadata,xmlmetadata) values (%s,%s,%s) RETURNING methane_data_file_id'
            parms = (file_name, json_descr,xmljson,)
    if nodb:
        return metadata_for_file(file_name=file_name,
                                 ds=ds,
                                 num_lats=num_lats,
                                 num_lons=num_lons,
                                 methane_data_file_id=0,
                                 processed=False)
    try:
        methane_data_file_id = db.insert(
            insert_str=insert_str,
            with_get_id=True,
            parms=(parms))
    except psycopg2.errors.UniqueViolation:
        if verbose:
            print(f" skipping {file_name} because we have already loaded it")
        return metadata_for_file(file_name=file_name)
    return metadata_for_file(file_name=file_name,
                             ds=ds,
                            num_lats=num_lats,
                            num_lons=num_lons,
                            methane_data_file_id=methane_data_file_id,
                            processed=True)





def methane_specific(file_name:str,
            args:dict[str,str],
            batch_commits:int=0,
            nodb:bool=False,
            maxrecords:int=0,
            verbose:bool=False):
    """
    After loading the main data file, load all the records
    :param file_name:
    :param args:
    :param batch_commits:
    :param nodb:
    :param maxrecords:
    :param verbose:
    :return:
    """
    metadata_for_the_file=add_file_record(file_name=file_name,
                                            args=args,
                                            nodb=nodb,
                                            verbose=verbose)
    if not metadata_for_the_file.processed:
        return 0
    unix_epoch_start = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime.now()
    records_inserted = 0
    times={}
    times["units"]="seconds since 1970-1-1 0:0:0.0"
    times["calendar"] = "gregorian"
    db=None if nodb else Db(args=args)
    for lon in range(0,metadata_for_the_file.num_lons-1):
        for lat in range(0,metadata_for_the_file.num_lats-1):
            timeofrecord = netCDF4.num2date(metadata_for_the_file.ds['time'][lat, lon].item(),
                                                        units=times['units'],
                                                        calendar=times['calendar'],
                                                        only_use_cftime_datetimes=False)
            standard_datetime = datetime(
                timeofrecord.year,
                timeofrecord.month,
                timeofrecord.day,
                timeofrecord.hour,
                timeofrecord.minute,
                tzinfo=timezone.utc
            )
            if standard_datetime > unix_epoch_start:
                my_parms= {'insert_str':f'INSERT INTO methane_data (methane_data_file_id,recorded_at,latitude,longitude,methane) values (%s,%s,%s,%s,%s)',
                           'with_get_id' :False,
                           'parms':(metadata_for_the_file.methane_data_file_id,
                                    netCDF4.num2date(metadata_for_the_file.ds['time'][lat, lon].item(),
                                                            units=times['units'],
                                                            calendar=times['calendar'],
                                                            only_use_cftime_datetimes=False),
                                  metadata_for_the_file.ds['lat'][lat].item(),
                                  metadata_for_the_file.ds['lon'][lon].item(),
                                  metadata_for_the_file.ds['xch4'][lat, lon].item(),)}
                if nodb:
                    print(my_parms)
                elif batch_commits:
                    db.insert_continuous(**my_parms)
                    if records_inserted and records_inserted%batch_commits == 0:
                        db.commit()
                else:
                    db.insert(**my_parms)
                records_inserted+=1
            if maxrecords and records_inserted>maxrecords:
                end_time = datetime.now()
                duration = end_time - start_time
                print(f"""
                        start time={start_time}            
                        end time={end_time}            
                        mean records per second={records_inserted /duration.seconds}            
                            """)
                sys.exit()
    end_time = datetime.now()
    duration = end_time - start_time
    print(f'inserted {records_inserted} records from file {file_name}.  Time required={duration}')
    if verbose:
        print(f"""
    start time={start_time}            
    end time={end_time}            
    mean records per second={records_inserted/duration.seconds}            
        """)
    return records_inserted


def process(file_names:list[str],
            args:dict[str,str],
            batch_commits:int=0,
            nodb:bool=False,
            verbose:bool=False):
    run_start_time = datetime.now()
    start_time = datetime.now()
    maxrecords = int(args.get('maxrecords',0))
    if verbose:
        print(f"Ready to process {len(file_names)} files.  Start time = {start_time}")
        if not nodb:
            print("db updates will be attempted")
    if nodb:
        print("No db updates will be attempted")
    total_records=0
    files_processed=0
    for file_name in file_names:
        records_inserted=methane_specific(file_name=file_name,
                        args=args,
                        batch_commits=batch_commits,
                        nodb=nodb,
                        maxrecords=maxrecords,
                        verbose=verbose)
        total_records += records_inserted
        files_processed += 1
    run_end_time = datetime.now()
    duration = run_end_time - run_start_time
    print(f'run completed at {run_end_time}.')
    if verbose:
        print(f"""
    number of files processed: {files_processed}    
    run start time={run_start_time}            
    run end time={run_end_time}            
    mean records per second{total_records / duration.seconds}            
         """)


#
# def process(file_names:list[str],
#             args:dict[str,str],
#             batch_commits:int=0,
#             nodb:bool=False,
#             verbose:bool=False):
#     run_start_time = datetime.now()
#     start_time = datetime.now()
#     maxrecords = int(args.get('maxrecords',0))
#     if verbose:
#         print(f"Ready to process {len(file_names)} files.  Start time = {start_time}")
#         if not nodb:
#             print("db updates will be attempted")
#     if nodb:
#         print("No db updates will be attempted")
#     total_records=0
#     files_processed=0
#     for file_name in file_names:
#         start_time = datetime.now()
#         if verbose:
#             print(f"Processing {file_name} at {start_time}")
#         ds=get_file_dataset(file_name)
#         json_descr=storable_metadata_json(ds)
#         num_lats=ds.dimensions.get('lat').size
#         num_lons=ds.dimensions.get('lon').size
#         db=Db(args=args)
#         times={}
#         times["units"]="seconds since 1970-1-1 0:0:0.0"
#         times["calendar"] = "gregorian"
#         try:
#             methane_data_file_id = db.insert(
#                 insert_str=f'INSERT INTO methane_data_file (file_name,metadata) values (%s,%s) RETURNING methane_data_file_id',
#                 with_get_id=True,
#                 parms=(file_name, json_descr,))
#         except psycopg2.errors.UniqueViolation:
#             if verbose:
#                 print(f" skipping {file_name} because we have already loaded it")
#             continue
#         records_inserted = 0
#         for lon in range(0,num_lons-1):
#             for lat in range(0,num_lats-1):
#                 my_parms= {'insert_str':f'INSERT INTO methane_data (methane_data_file_id,recorded_at,latitude,longitude,methane) values (%s,%s,%s,%s,%s)',
#                            'with_get_id' :False,
#                            'parms':(methane_data_file_id,
#                                     netCDF4.num2date(ds['time'][lat, lon].item(),
#                                                      units=times['units'],
#                                                      calendar=times['calendar'],
#                                                      only_use_cftime_datetimes=False),
#                                   ds['lat'][lat].item(),
#                                   ds['lon'][lon].item(),
#                                   ds['xch4'][lat, lon].item(),)}
#                 if batch_commits:
#                     db.insert_continuous(**my_parms)
#                     if records_inserted and records_inserted%batch_commits == 0:
#                         db.commit()
#                         # if verbose:
#                         #     print(f'{datetime.now()}: inserted {records_inserted} records inserted so far')
#                 else:
#                     db.insert(**my_parms)
#                 records_inserted+=1
#                 if maxrecords and records_inserted>maxrecords:
#                     end_time = datetime.now()
#                     duration = end_time - start_time
#                     print(f"""
#                             start time={start_time}
#                             end time={end_time}
#                             mean records per second={records_inserted /duration.seconds}
#                                 """)
#                     sys.exit()
#         end_time = datetime.now()
#         duration = end_time - start_time
#         print(f'inserted {records_inserted} records from file {file_name}.  Time required={duration}')
#         if verbose:
#             print(f"""
#         start time={start_time}
#         end time={end_time}
#         mean records per second={records_inserted/duration.seconds}
#             """)
#         total_records += records_inserted
#         files_processed += 1
#     run_end_time = datetime.now()
#     duration = run_end_time - run_start_time
#     print(f'run completed at {run_end_time}.')
#     if verbose:
#         print(f"""
#     number of files processed: {files_processed}
#     run start time={run_start_time}
#     run end time={run_end_time}
#     mean records per second{total_records / duration.seconds}
#          """)
