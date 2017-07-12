from __future__ import print_function
from __future__ import division
import argparse
import collections
import os
import string
from azure.datalake.store import core, lib, multithread

import sys, subprocess, time
import numpy
import pandas as pd
from datetime import datetime
from pyomo.environ import *
import scipy
import logging
from numpy import arange

n_download_retries = 5 # number of times to try to download a CSV file from ADLS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def download_from_adls(adl, short_filename, filename):
    download_succeeded = False
    for i in arange(n_download_retries):
        try:
            multithread.ADLDownloader(adl, lpath = short_filename, rpath = filename, overwrite=True)
        except BaseException as e:
            logger.error('Failed to download the file ' + short_filename + ': ' + str(e), exc_info=True)
            time.sleep(30)
            continue
        download_succeeded = True
        break

    return download_succeeded

def load_data_from_csv(adl, model, root_dir, partition_string, directory_name):

    # create a directory name
    dirname = root_dir + '/' + directory_name + '/'
    if partition_string != 'none':
        partition = partition_string.split(',')
        n_levels = len(partition)
        for level in partition:
            dirname += level + '/'
    else:
        n_levels = 0

    # get a list of all files in the directory
    files = adl.ls(dirname)
    len_directory_name = len(directory_name)
    files = sorted(files, key = lambda x: int(x.split('/')[-1][len_directory_name:].split('_')[1]))

    # split file names into files with parameters and files with sets
    set_files = []
    param_files = []
    for file in files:
        file_short = file.split('/')[-1][len_directory_name:]
        fields = file_short.split('_')
        if fields[2] == 'P':
            param_files.append(file)
        else:
            set_files.append(file)

    data = DataPortal(model=model)

    # load sets
    for filename in set_files:
        
        print("Downloading " + filename)

        # read first line of the file to get a name of the variable
        with adl.open(filename) as f:
            varname = f.readline().decode().strip()

        # download the file from ADLS to local storage
        short_filename = filename.split('/')[-1]
        
        if download_from_adls(adl, short_filename, filename) == False:
            print("Cannot download file " + filename, file=sys.stderr)
            return 

        # create Python command 
        command = 'data.load(filename = \'' + short_filename + '\', set = model.' + varname + ')'

        # load the data
        print(command)
        exec(command)

    # load parameters
    for filename in param_files:
         
        print("Downloading " + filename)

        # read first line of the file to get a name of the variable
        with adl.open(filename) as f:
            params = f.readline().decode().strip().split(',')

        # download the file from ADLS to local storage
        short_filename = filename.split('/')[-1]

        if download_from_adls(adl, short_filename, filename) == False:
            print("Cannot download file " + filename, file=sys.stderr)
            return 
        
        # create Python command
        suffix = short_filename[len(directory_name):].split('_')[3]
        if n_levels > 0:
            n_indices = int(suffix)
        else:
            n_indices = int(suffix.split(".")[0])

        if n_indices == 0:
            model_params = ['model.' + x for x in params]
            params_str = ','.join(model_params)
            command = 'data.load(filename = \'' + short_filename + '\', param = (' + params_str + '))'
        else:
            model_indices = ['model.' + x for x in params[:n_indices]]
            indices_str = ','.join(model_indices)
            model_params = ['model.' + x for x in params[n_indices:]]
            params_str = ','.join(model_params)
            command = 'data.load(filename = \'' + short_filename + '\', param = (' + params_str + '), index = (' + indices_str + '))'

        # load the data
        print(command) 
        exec(command)
        print("finished execution")

    return data

def OptimizeInventory(adls_client, input_adl_folder, partition_str, inventory_policy_name, 
                      optimization_definition, solver_name, solver_path, file_extension,directory_name,timestamp):
    
    definition_file = optimization_definition + '_' + solver_name + '.' + file_extension
    result_file= os.path.realpath('./'+optimization_definition + '_' + timestamp + '.csv')

    # import definition module
    optdef = __import__(optimization_definition)
    
    start_time = time.time()

    # create problem definition file
    model = optdef.defineOptimization()
    
    # load data from CSV files
    data = load_data_from_csv(adls_client, model, input_adl_folder, partition_str,directory_name)
    instance = model.create_instance(data)
    
    if solver_name == 'MIPCL':
        import mipcl_wrapper as mipcl
        result = mipcl.solve(instance, solver_path, definition_file)
    else:
        instance.write(definition_file)
        #for other solvers
        if solver_path != 'default':
            solver = SolverFactory(solver_name, executable=solver_path, solver_io=file_extension)
        #for Gurobi
        else:
            solver = SolverFactory(solver_name, solver_io=file_extension)

        result = solver.solve(instance)
   
    instance.solutions.load_from(result)
    # Write solution csv file
    with open(result_file, 'w') as f:
        for var in instance.component_data_objects(Var):
            f.write('%s,%s\n' % (var, var.value))

    print("Solving optimization problem took" + " %s seconds." % (time.time() - start_time))

    return result_file


if __name__ == '__main__':
    start_time_all = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_adl_folder', required=True)
    parser.add_argument('--output_adl_folder', required=True)
    parser.add_argument('--inventory_policy_name', required=True)
    parser.add_argument('--optimization_definition', required=True)
    parser.add_argument('--directory_name', required=True)
    parser.add_argument('--solver_name', required=True)
    parser.add_argument('--solver_path', required=True)
    parser.add_argument('--file_extension', required=True)
    parser.add_argument('--partition_str', required=True)
    parser.add_argument('--adl_name', required=True)
    parser.add_argument('--adl_tenant_id', required=True)
    parser.add_argument('--adl_client_id', required=True)
    parser.add_argument('--adl_client_secret', required=True)
    parser.add_argument('--timestamp', required=True)
    args = parser.parse_args()
 
    adl_client_start_time = time.time()

    # create ADLS client    
    adl_token = lib.auth(tenant_id=args.adl_tenant_id, client_id=args.adl_client_id, client_secret=args.adl_client_secret)
    adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=args.adl_name)
    print("Creating ADL client took" + " %s seconds." % (time.time() - adl_client_start_time))

    print("Started solving optimization problem")
    optimization_start_time = time.time()
    output_file = OptimizeInventory(adls_file_system_client, args.input_adl_folder, args.partition_str, args.inventory_policy_name, 
                                    args.optimization_definition, args.solver_name, args.solver_path, args.file_extension,args.directory_name,args.timestamp)
    print("Total time for generating solution: " + " %s seconds." % (time.time() - optimization_start_time))

    upload_result_start_time = time.time()
    output_remote_dir_name = args.output_adl_folder + '/'+args.directory_name
    if args.partition_str != 'none':
        partition = args.partition_str.split(',')
        for level in partition:
            output_remote_dir_name += '/' + level

    print('Uploading file {} to ADL folder [{}]...'.format(output_file, output_remote_dir_name))

    multithread.ADLUploader(adls_file_system_client, lpath = output_file, 
                            rpath = output_remote_dir_name + '/' + os.path.basename(output_file), overwrite=True)
    print("Uploading results took " + " %s seconds." % (time.time() - upload_result_start_time))

    print("Total time:" + " %s seconds." % (time.time() - start_time_all))
