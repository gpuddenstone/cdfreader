import os
import glob
import argparse
import sys
from dotenv import load_dotenv
from methane.utilities import get_files,get_file_dataset,display_dataset_metadata
from methane.process_a_file import process
import  netCDF4 as nc

def process_args()->dict[str,str]:
    parser = argparse.ArgumentParser(description="cdfreader utility")

    # 2. Add arguments
    parser.add_argument("-file","-f", required=True, help="Absolute path for file to read.  Use wildcards for multiple files")
    parser.add_argument("-metadata", "-m",action="store_true",help="Read file metadata only")
    parser.add_argument("-dryrun", "-d", action="store_true", help="Don't save to db")
    parser.add_argument("-commit_batching", "-b", required=False, default=0,type=int, help="Number of inserts before committing")
    parser.add_argument("-maxrecords", "-max", required=False, default=0, help="Maximum records to load")
    parser.add_argument("-csv", "-c", action="store_true", help="Read counties by year")
    parser.add_argument("-verbose", "-v", action="store_true", help="Increase output verbosity")
    parser.add_argument("-host", help="postgres host", required=False,  default="localhost")
    parser.add_argument("-port", help="postgres port", required=False, default="5432")
    parser.add_argument("-user", help="postgres port", required=False,default="")
    parser.add_argument("-password", help="postgres port", required=False,default="")
    parser.add_argument("-db", help="postgres port", required=False,default="")
    return vars(parser.parse_args())

def check_for_env(developing_dict:dict[str,str])->None:
    if len(os.getenv("POSTGRES_USER",""))==0 or len(os.getenv("POSTGRES_PASSWORD",""))==0:
        load_dotenv(".env")
    if len(developing_dict.get("host",""))==0:
        developing_dict.update({"host":os.getenv("POSTGRES_HOST","")})
    if len(developing_dict.get("port",""))==0:
        developing_dict.update({"port":os.getenv("POSTGRES_PORT","")})
    if len(developing_dict.get("user",""))==0:
        developing_dict.update({"user":os.getenv("POSTGRES_USER","")})
    if len(developing_dict.get("password",""))==0:
        developing_dict.update({"password":os.getenv("POSTGRES_PASSWORD","")})
    if len(developing_dict.get("db",""))==0:
        developing_dict.update({"db":os.getenv("POSTGRES_DB","")})


def main():
    print(f'cdfreader is ready.  Utilizing CDFlib Version\n{nc.__version__}')
    args=process_args()
    file_names = get_files(args.get("file",""))
    if len(file_names)==0:
        sys.exit(0)
    # we might need db values.  double check for them
    check_for_env(args)
    nodb = (args.get("dryrun",False) or
            len(args["host"])==0 or
            len(args["port"]) == 0 or
            len(args["user"]) == 0 or
            len(args["password"]) == 0 or
            len(args["db"]) == 0)
    if (args.get("metadata","")):
        display_dataset_metadata(get_file_dataset(file_names[0]))
    process(file_names=file_names,
            args=args,
            nodb=nodb,
            batch_commits=args.get('commit_batching',0),
            verbose=args.get('verbose',False))

if __name__ == "__main__":
    main()






