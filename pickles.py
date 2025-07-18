

## welcome to PICKLES --> Probabilistic Identification of Clusters using Kurtosis for Localizing Extraterrestrial Signals.


import subprocess
import sys
import os
# from boundary_checker import *
import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
import statistics as stats
import pandas as pd
from scipy import signal
import sys
import datetime
import h5py
from scipy.stats import pearsonr 
import scipy.stats  
from tqdm import tqdm
import traceback
import hdf5plugin
import argparse
import pickle
import math
import gc
from scipy.signal import find_peaks
import itertools
import threading

# from cappuccino.cappuccino.__init__ import *

def main(batch_number, section_bool,rerun):
    """This is the main function.

    Args:
        candidates_df_name (_type_): _description_
    """

    # check if candidates database is set up, if not then initialize it. This is where the candidates will be stored

    # define the size of the frequency window that the observation file is split into
    block_size = 1024

    # define the significance level of signals we want to be sensitive to
    significance_level = 10

    # define a directory to save file outputs to
    main_dir = '/datax/scratch/andresl/xband/'

    
    # pickles handles a few different ways of inputing targets 

    # most useful ones 
    ## --> A batch number (of the ~120 batches that the large dataset of archival observations was split into. this is useful if you're trying to run it on everything, can just go batch number one after the other)
    ## --> A specific file. Good for checking specific observations. Pass it in as `python pickles.py [Target, MJD, node]` --> Ex: [HIP62207,57543,spliced]  --> (in this case the node is just the string 'spliced', that's how the code recognizes spliced files of multiple nodes. Otherwise just the node number will do in replacement of 'spliced'.)

    if batch_number.isdigit():
        batch_number = int(batch_number)
        # we will store the information of returned signals and properties in a csv file

        ## I had to rerun a few batches that had errors, hence this
        if rerun == "True":
            df_name = f'updated_all_cadences_mason_jar_batch_{batch_number}_block_size_{block_size}_snr_{significance_level}_section_{section_bool}_rerun_{rerun}.csv'

        else:
            df_name = f'updated_all_cadences_mason_jar_batch_{batch_number}_block_size_{block_size}_snr_{significance_level}_section_{section_bool}.csv'

        # make sure this place to store files is OK
        db_exists = os.path.exists(main_dir+df_name)
        if db_exists == False:
            print(main_dir+df_name)
            print("Creating candidates database as ",df_name)
            feature_table = pd.DataFrame(columns=["All Files","Index","Block Size","Freq","obs1 maxes","obs3 maxes","obs5 maxes","ON_freq_int","k1","k2","k3","k4","k5","k6","k_score","min_k","med_k","max_k","drift1","drift2"])
            feature_table.to_csv(main_dir+df_name,index=False)
        else:
            print("feature table database already exists:",main_dir+df_name)


    ## if instead we are passing in a specific target
    else:
        target_line = batch_number
        n = len(target_line)
        print('target',target_line)
        target_info = target_line[1:n-1]
        target_info = target_info.split(',')
        target_name = target_info[0]
        target_date = target_info[1]
        target_node = target_info[2]

        
        df_name = f'updated_target_{target_name}_date_{target_date}_node_{target_node}_blocksize_{block_size}_snr{significance_level}.csv'

        db_exists = os.path.exists(main_dir+df_name)
        if db_exists == False:
            print(main_dir+df_name)
            print("Creating candidates database as ",df_name)
            feature_table = pd.DataFrame(columns=["All Files","Index","Block Size","Freq","obs1 maxes","obs3 maxes","obs5 maxes","ON_freq_int","k1","k2","k3","k4","k5","k6","k_score","min_k","med_k","max_k","drift1","drift2"])
            feature_table.to_csv(main_dir+df_name,index=False)
        else:
            print("feature table database already exists:",main_dir+df_name)

    
    # load all cadences
    with open('/datax/scratch/andresl/xband-batches.pkl', 'rb') as f:
        reloaded_batches = pickle.load(f)

    
    if isinstance(batch_number,int):
        
        print(len(reloaded_batches))
        specific_batch = reloaded_batches[batch_number]
        feature_table = pd.read_csv(main_dir+df_name,dtype={"drift2": 'boolean',"blip or broadband": 'boolean'})

        ## how many observations are in this batch that will be processed (usually ~1000)
        batch_observations_to_run_on = range(0,len(specific_batch))

        ## if pickles is running on a batch/target it already ran on, we load up the latest version of that batch/target's csv file to see what observations were already processed and what ones still need to get analyzed
        batches_already_in_rerun_table = []
        if rerun == "True":
            old_df_name = f'updated_all_cadences_mason_jar_batch_{batch_number}_block_size_{block_size}_snr_{significance_level}_section_{section_bool}.csv'

            old_feature_table = pd.read_csv(main_dir+old_df_name,dtype={"drift2": 'boolean',"blip or broadband": 'boolean'})

            unique_batch_numbers_already_run = set(list(old_feature_table["Batch Info"]))
            
            obs_run = np.sort([eval(x)[1] for x in unique_batch_numbers_already_run])

            # this is code because earlier I had run some batches that missed observations within the batch 
            print("Batches already run:", obs_run)
            missing_obs = find_missing_obs(obs_run)
            print("Following observations are missing and will be re-ran:", missing_obs)
            batch_observations_to_run_on = missing_obs
            
            ## I also ended up adding some more features to the feature table 
            last_mason = pd.read_csv(main_dir+df_name,dtype={"drift2": 'boolean',"blip or broadband": 'boolean'})

            # grab all observations but last one in case that one hadn't finished fully
            try:
                batches_already_in_rerun_table = [eval(x)[1] for x in list(last_mason['Batch Info'])][:-1]
            except:
                print("first run with this csv")
                batches_already_in_rerun_table = []
            print('batches alread in rerun csv:', set(batches_already_in_rerun_table))

            ## define a list of the observationst that still need to be analyzed
            main_list = list(set(batch_observations_to_run_on) - set(batches_already_in_rerun_table))
            
            batch_observations_to_run_on = main_list
            
            print("Final Observations to run", batch_observations_to_run_on)
        

        ## now we actually go through each observation in that list and process it
        ## inside a try/except loop in case there is some error
        try:
            # iterate through each node (cadence)
            for i in batch_observations_to_run_on:
                print(f"Now on file {i} out of {len(specific_batch)}")
                # load current csv of file properties
                try:
                    last_mason = pd.read_csv(main_dir+df_name,dtype={"drift2": 'boolean',"blip or broadband": 'boolean'})

                    # only run if we haven't already looked at it
                    if i not in batches_already_in_rerun_table:
                        # grab the specific cadence to look at
                        h5_files = specific_batch[i]
                        # pass the files into the boundary_checker wrapper function. Returns flagged frequencies and respective scores
                        print("Now running on file ",h5_files[0])

                        ## this is the function that does most of the actual analysis, loading of data, and grabbing of specific properties. 
                        k_score_table= pickler_wrapper((batch_number,i),h5_files,block_size,significance_level, section_bool)
        
                        # append all flagged frequencies to the candidates database
                        updated_mason = pd.concat([last_mason, k_score_table])
                        updated_mason.to_csv(main_dir+df_name,index=False)
        
                        print(updated_mason)
                    else:
                        print('Skipping, already Ran this batch in this csv')
                except Exception:
                    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                    print(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ERROR ON CADENCE {i} XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                    print(traceback.print_exc())
    
    
        except Exception:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ERROR ON TARGET {batch_number} XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(traceback.print_exc())

    # if we are instead running on a specific observation file instead of a batch (collection of files)
    else:
        print("TARGET:",target_name,target_date,target_node)
        all_file_paths = [find_cadence(target_name,target_date,target_node,reloaded_batches)]
        print("Files", all_file_paths)
        try:
            last_mason = pd.read_csv(main_dir+df_name)
            # grab the specific cadence to look at
            h5_files = all_file_paths[0]
            # pass the files into the boundary_checker wrapper function. Returns flagged frequencies and respective scores
            print("Now running on file ",h5_files[0])
            k_score_table= pickler_wrapper((target_name,target_date),h5_files,block_size,significance_level,section_bool)

            # append all flagged frequencies to the candidates database
            updated_mason = pd.concat([last_mason, k_score_table])
            updated_mason.to_csv(main_dir+df_name,index=False)

            print(updated_mason)
        except Exception:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ERROR ON CADENCE {i} XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(traceback.print_exc())


def find_cadence(target,time,node,reloaded_batches):
    for batch in range(0,101):
        for cadence in reloaded_batches[batch]:
            combined_string = " ".join(cadence)
            if combined_string.count(target) >= 3 and time in combined_string and node in combined_string:
                return cadence

def find_missing_obs(obs_run):
    all_numbers = set(range(1, 1000))
    missing_numbers = all_numbers - set(obs_run)
    return sorted(missing_numbers)

    
def pickler_wrapper(batch_info,h5_files,block_size,significance_level,section_bool):

    # load data files, for ON and OFF observations
    hf_ON = h5py.File(h5_files[0], 'r')
    hf_OFF = h5py.File(h5_files[1], 'r')
    hf_ON2 = h5py.File(h5_files[2], 'r')
    hf_OFF2 = h5py.File(h5_files[3], 'r')
    hf_ON3 = h5py.File(h5_files[4], 'r')
    hf_OFF3 = h5py.File(h5_files[5], 'r')



    # grab specific rows which will be used to find hotspots
    # here we take the middle row of each ON observation 

    ## I did this to locate the areas of the observation file to focus on --> I was checking to see what frequency
    # snippets showed some sign of signal somewhere inside them, so the first step was just to load the middle time rows of each ON scan of the entire observation
    ## and see where there were signs of a signal
    obs1_row_8 = np.squeeze(hf_ON['data'][7:8,:,:])
    obs3_row_8 = np.squeeze(hf_ON2['data'][7:8,:,:])
    obs5_row_8 = np.squeeze(hf_ON3['data'][7:8,:,:])


    freq_length_of_data = len(obs1_row_8)

    # find file frequency information
    fch1,foff,nchans = get_file_properties(hf_ON)

    # calculate number of iterations needed (how many frequency windows the observation will get divided up into) and find hotspots 
    number = int(np.round(len(obs1_row_8)/block_size))

    # record interesting freq chunks as 'warmspots'. This is the initial pass. 
    # This is a lower sigma requirement before doing a median absolute deviation test of signal strength, which just takes a bit longer to compute so I wanted to reduce the number of frequency snippets to run on first. 
    hotspot_slices = [obs1_row_8,obs3_row_8, obs5_row_8]

    number = int(np.round(len(obs1_row_8)/block_size))

    all_warmspots = []

    print("Cutting cucumbers...")
    for i in range(0,len(hotspot_slices)):
        # i.e. where there exist big outliers (max >= median + 5std)
        warmspots = find_warmspots(hotspot_slices[i],number,block_size)
        all_warmspots = all_warmspots+warmspots

   # keep only the unique blocks
    warmspots = [*set(all_warmspots)]
    print('warmspots',len(warmspots))

    # next filter out warmspots that fall in bad regions (High RFI)
    print("Throwing out rotten ones...")
    filtered_indexes = filter_hotspots(warmspots,fch1,foff,block_size)
    filtered_warmspots = np.delete(warmspots, filtered_indexes)

    # now sort through these warmspots and find hotspots --> higher signal
    all_hotspots = []
    for i in range(0,len(hotspot_slices)):
        hotspots = find_hotspots(hotspot_slices[i],filtered_warmspots,block_size,significance_level)
        all_hotspots=all_hotspots+hotspots
        
    

    filtered_hotspots = [*set(all_hotspots)]
    # print(filtered_hotspots)
    print(len(filtered_hotspots))

    # now we can grab the hotspots exactly half a block size ahead and behind of the flagged index --> basically we are doing overlapping frequency windows to make sure we catch the signal, similar to Peter Ma's deep learning paper
    # --> to capture potential high drift signals that might not show up in all 3 ON observations
    extra_hostpots_plus = [i+.5 for i in filtered_hotspots[1:-1]]
    extra_hostpots_minus = [i-.5 for i in filtered_hotspots[1:-1]]
    extra_hotspots = extra_hostpots_plus+extra_hostpots_minus

    # only include extra hotspots if there are at least 2 strong signals in each ON scan (we don't care super much about little blips that fade out)
    ## but our frequency window size (1024) should ensure that if the windows are overlapping, signals drifting up to +/- 4 Hz/s will definitely show up in at least 2 of the ON scans
    interesting_extra_hotspots = []
    for spot in extra_hotspots:
        strong_signals_extra = 0 
        for i in range(0,len(hotspot_slices)):    
            row = hotspot_slices[i]
            slice_ON = row[int(spot*block_size):int((spot+1)*block_size):]
            snr,threshold = get_snr(slice_ON,significance_level)
            if snr:
                strong_signals_extra +=1 
        if strong_signals_extra >= 2:
            interesting_extra_hotspots.append(spot)



    # check which hotsplot slice (which ON observation) gave the hotspot signal
    filtered_hotspots_slice_indexes = []
    strong_filtered_hotspots = []
    
    strong_signal_counter = 0

    # in the end let's just store those which actually have a strong signal in at least 2
    for spot in filtered_hotspots:
        for i in range(0,len(hotspot_slices)):    
            row = hotspot_slices[i]
            slice_ON = row[int(spot*block_size):int((spot+1)*block_size):]
            snr,threshold = get_snr(slice_ON,significance_level)
            if snr:
                strong_signal_counter += 1
        if strong_signal_counter >= 2:
            strong_filtered_hotspots.append(spot)

    # comine these with the overlapping by half ones
    final_filtered_hotspots = strong_filtered_hotspots+interesting_extra_hotspots
    final_filtered_hotspots = list(np.sort(final_filtered_hotspots))
    print("Final # of cucumbers:",len(final_filtered_hotspots))

    # this variable is more of a placeholder now, since all of the regions have a signal in at least 2 ON observations. (it used to be used for something in a previous iteration of the code)
    for spot in final_filtered_hotspots:
        for i in range(0,len(hotspot_slices)):    
            row = hotspot_slices[i]
            slice_ON = row[int(spot*block_size):int((spot+1)*block_size):]
            snr,threshold = get_snr(slice_ON,significance_level)
            if snr:
                filtered_hotspots_slice_indexes.append(i)

    # delete these for memory saving
    del hotspot_slices
    del obs5_row_8
    del obs3_row_8
    del obs1_row_8

    dt1 = datetime.datetime.now() ## keeping track of how long things are taking

    sectioning = section_bool ## this is an important thing in terms of how long it takes to process an observation
                              ## it can take a lot of memory to load up an entire observation, but it takes a long time to go back and forth loading small pieces of it (input/output time is killer)
                              ## a good compromise is to load up large section of an observation, and then you don't need to individually load up the small frequency snippets, you can just grab them from the already loaded array
                              ## here I'm dividing up an observation into 8 pieces. This might be too big if the observation is spliced nodes instead of an individual one. 
                              ## but if there are a lot of frequency snippets, it's a lot more time efficient than individually loading them. 


    if sectioning == "True":
        num_sections = 8
        print(f"attempting to divide observation into {num_sections} sections")
        
        ## define the sections that the observationg ets split into
        sections = np.linspace(fch1,fch1+foff*nchans,num_sections)
        section_blocks = np.round(abs((sections-fch1)/(block_size*foff)))
        
        observations = [hf_ON, hf_OFF, hf_ON2, hf_OFF2, hf_ON3, hf_OFF3]
        indexes = final_filtered_hotspots
        
        print('section_blocks',section_blocks)

        rounds_run = 0

        ## define a table to store the information of the frequency windows inside that observations ection
        k_score_table_data_full = []
        for num in range(0,len(section_blocks)-1):
            print(f"Now Running on Section {num} of {len(section_blocks)}")
            print(f"Have completed {rounds_run}/{len(indexes)+rounds_run} indexes")
            section = section_blocks[num+1]
            round = np.array([x for x in indexes if x <= section])
            rounds_run += len(round)
            
            indexes = set(indexes) - set(round)
            round = round - section_blocks[num]
        
            sectioned_observations = []
            lower = int(np.round(section_blocks[num]*block_size))
            upper = int(np.round(section_blocks[num+1]*block_size))
        
            ## make sure there are at least some frequency windows to run on inside that observation section
            if len(round) > 0:
                for obs_data in observations:
                    section =  np.squeeze(obs_data['data'][:,:,lower:upper],axis=1)
                    sectioned_observations.append(section)
                    
                round = list(round)

                # actually go get the properties of those frequency snippets
                k_score_table_data = get_k_scores(batch_info,sectioned_observations[0],sectioned_observations[1],sectioned_observations[2],sectioned_observations[3],sectioned_observations[4],sectioned_observations[5],round,h5_files,fch1,foff,filtered_hotspots_slice_indexes,block_size,sectioning,section_blocks[num])

                # and record those properties
                k_score_table_data_full.append(k_score_table_data)

        print(len(k_score_table_data_full))

        if len(k_score_table_data_full) > 0:
            print(len(k_score_table_data_full[0]))
            k_score_table_data_full = list(itertools.chain.from_iterable(k_score_table_data_full))
            k_score_table = pd.DataFrame(k_score_table_data_full, columns=["Batch Info","All Files","Index","Block Size","Freq","obs1 maxes","obs3 maxes","obs5 maxes","ON_freq_int","k1","k2","k3","k4","k5","k6","k_score","min_k","med_k","max_k","drift1","drift2","blip or broadband"])
        else:
            print("Empty Observation")
            k_score_table = pd.DataFrame([[batch_info,h5_files,0,block_size,0,[],[],[],[],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]], columns=["Batch Info","All Files","Index","Block Size","Freq","obs1 maxes","obs3 maxes","obs5 maxes","ON_freq_int","k1","k2","k3","k4","k5","k6","k_score","min_k","med_k","max_k","drift1","drift2","blip or broadband"])

        


    elif sectioning == "False":
        print("Salting and Seasoning...")
        k_score_table = get_k_scores(batch_info,hf_ON,hf_OFF,hf_ON2,hf_OFF2,hf_ON3,hf_OFF3,final_filtered_hotspots,h5_files,fch1,foff,filtered_hotspots_slice_indexes,block_size,sectioning,0)

    dt2 = datetime.datetime.now()
    print('Elapsed Time',dt2 - dt1)

    return k_score_table


# def read_file(obs_data):
#     return np.squeeze(obs_data['data'][:,:,lower:upper],axis=1)

    
# def multi_threaded_file_reader(observations):
#     threads = []
#     results = []

#     # Define the worker function
#     def read_file_thread(file_path):
#         result = read_file(file_path)
#         results.append(result)

#     # Create and start threads
#     for obs_data in observations:
#         thread = threading.Thread(target=read_file_thread, args=(obs_data,))
#         threads.append(thread)
#         thread.start()

#     # Wait for all threads to finish
#     for thread in threads:
#         thread.join()

#     return results
    
    
    


def get_k_scores(batch_info,hf_obs1,hf_obs2,hf_obs3,hf_obs4,hf_obs5,hf_obs6,filtered_hotspots,file_list,fch1,foff,filtered_hotspots_indexes,block_size,sectioning,section_index):

    # define table of properties of interest
    k_score_table = pd.DataFrame(columns=["Batch Info","All Files","Index","Block Size","Freq","obs1 maxes","obs3 maxes","obs5 maxes","ON_freq_int","k1","k2","k3","k4","k5","k6","k_score","min_k","med_k","max_k","drift1","drift2","blip or broadband"])
    # we iterate through all of the hotspots
    k_score_table_data = []


    for i in filtered_hotspots:

        # define the block region we are looking at
        try:
            hotspot_index = filtered_hotspots.index(i)
            hotspot_slice = filtered_hotspots_indexes[hotspot_index]
            
            lower = int(i * block_size)
            upper = int((i+1) * block_size)
            observations_ON = [hf_obs1,hf_obs3,hf_obs5]
            primary_hf_ON = observations_ON[hotspot_slice]


            # laod the snippet/window of interest, either by actually retrieving it from the observation file (if sectioning = False) or just from the already loaded numpy array (if sectioning = True) 
            if sectioning == "False":
    
                
                # get hit index
                row_ON = np.squeeze(primary_hf_ON['data'][-1:,:,lower:upper],axis=1)[0]
    
                       
                # load data of each hotspot
                # integrate each one for more later statistics
                Obs1 = np.squeeze(hf_obs1['data'][:,:,lower:upper],axis=1)
                obs1_int = Obs1.sum(axis=0)
    
                Obs2 = np.squeeze(hf_obs2['data'][:,:,lower:upper],axis=1)
                obs2_int = Obs2.sum(axis=0)
    
                Obs3 = np.squeeze(hf_obs3['data'][:,:,lower:upper],axis=1)
                obs3_int = Obs3.sum(axis=0)
    
                Obs4 = np.squeeze(hf_obs4['data'][:,:,lower:upper],axis=1)
                obs4_int = Obs4.sum(axis=0)
    
                Obs5 = np.squeeze(hf_obs5['data'][:,:,lower:upper],axis=1)
                obs5_int = Obs5.sum(axis=0)
    
                Obs6 = np.squeeze(hf_obs6['data'][:,:,lower:upper],axis=1)
                obs6_int = Obs6.sum(axis=0)

                frequency = fch1+foff*(i*block_size)

                dt1 = datetime.datetime.now()

            elif sectioning == "True":
                Obs1 = hf_obs1[:,lower:upper]
                Obs2 = hf_obs2[:,lower:upper]
                Obs3 = hf_obs3[:,lower:upper]
                Obs4 = hf_obs4[:,lower:upper]
                Obs5 = hf_obs5[:,lower:upper] 
                Obs6 = hf_obs6[:,lower:upper]

                row_ON = primary_hf_ON[-1,lower:upper]

                obs1_int = Obs1.sum(axis=0)
                obs2_int = Obs2.sum(axis=0)
                obs3_int = Obs3.sum(axis=0)
                obs4_int = Obs4.sum(axis=0)
                obs5_int = Obs5.sum(axis=0)
                obs6_int = Obs6.sum(axis=0)    

                frequency = fch1+foff*(i*block_size + section_index*block_size)


            ## now we go about grabbing some properties from that frequency window/snippet

            # sum the time-integrated data for certain statistics --> like looking for peaks of non-drifting signals
            on_sum = obs1_int+obs3_int+obs5_int
            off_sum = obs2_int+obs4_int+obs6_int

            whole_sum = obs1_int+obs3_int+obs5_int+obs2_int+obs4_int+obs6_int

            on_sum = on_sum/np.max(on_sum)
            off_sum = off_sum/np.max(off_sum)
            whole_sum = whole_sum/np.max(whole_sum)

            # calculate k-score
            cadence_max = np.max([np.max(Obs1),np.max(Obs2),np.max(Obs3),np.max(Obs4),np.max(Obs5),np.max(Obs6)])

            obs1_values = (Obs1/cadence_max).flatten()
            obs2_values = (Obs2/cadence_max).flatten()
            obs3_values = (Obs3/cadence_max).flatten()
            obs4_values = (Obs4/cadence_max).flatten()
            obs5_values = (Obs5/cadence_max).flatten()
            obs6_values = (Obs6/cadence_max).flatten()
        
        
            k1 = scipy.stats.kurtosis(obs1_values)
            k2 = scipy.stats.kurtosis(obs2_values)
            k3 = scipy.stats.kurtosis(obs3_values)
            k4 = scipy.stats.kurtosis(obs4_values)
            k5 = scipy.stats.kurtosis(obs5_values)
            k6 = scipy.stats.kurtosis(obs6_values)
            
            k_score = abs((k1+k3+k5)/(k2+k4+k6))

            off_k_sum = k2+k4+k6

            on_ks = [k1,k3,k5]

            med_k = np.median(on_ks)
            min_k = np.min(on_ks)
            max_k = np.max(on_ks)

            # calculate the ranges

            obs1_freq_int = Obs1.sum(axis=1)
            obs3_freq_int = Obs3.sum(axis=1)
            obs5_freq_int = Obs5.sum(axis=1)
 
            obs1_freq_int = obs1_freq_int/np.max(Obs1)
            obs3_freq_int = obs3_freq_int/np.max(Obs1)
            obs5_freq_int = obs5_freq_int/np.max(Obs1)


            # calculate the changes in maximum
            observations = [Obs1/np.max(Obs1),Obs3/np.max(Obs3),Obs5/np.max(Obs5)]
            # also calculate max value at each time integration point
            dt1 = datetime.datetime.now()
            # print('time start',dt1.microsecond/1000)

            obs_time_maxes = []
            for number in [0,1,2]:
                time_maxes = []
                for time in range(16):
                    time_max = np.max(observations[number][time])
                    time_maxes.append(time_max)
                obs_time_maxes.append(time_maxes)

            
            # get snr of maxes to check for broadband
            snr_1,threshold_1 = get_snr(obs_time_maxes[0],10)
            snr_3,threshold_3 = get_snr(obs_time_maxes[1],10)
            snr_5,threshold_5 = get_snr(obs_time_maxes[2],10)

            blip_or_broadband = False
            if sum([snr_1,snr_3,snr_5]) >= 1:
                blip_or_broadband = True


            drifting = True

            # check drift rate two ways
            constant_peaks,peak_drift_1,peak_drift_2, peak_drift_3 = filter_zero_drift(Obs1,Obs2,Obs3,Obs4,Obs5,Obs6,3)
            peak_drift_1= abs(np.array(peak_drift_1))
            peak_drift_2 = abs(np.array(peak_drift_2))
            peak_drift_3 = abs(np.array(peak_drift_3))

            if np.any(peak_drift_1 < 4) or np.any(peak_drift_2 < 4) or np.any(peak_drift_3 < 8):
                drifting = False

            # check drift rate
            zero_drift = drift_index_checker(on_sum, row_ON,10,10)
            drift = 1
            if zero_drift == True:
                drift = 0

            
            dt1 = datetime.datetime.now()
            # print('time start',dt1.microsecond/1000)

            if sectioning == "False":
                k_score_table.loc[len(k_score_table.index)] = [batch_info,file_list,i,block_size,frequency,obs_time_maxes[0],obs_time_maxes[1],obs_time_maxes[2],[obs1_freq_int,obs3_freq_int,obs5_freq_int],k1,k2,k3,k4,k5,k6,k_score,min_k,med_k,max_k,drift,drifting,blip_or_broadband]

            if sectioning == "True":
                i = i+ section_index
                k_score_table_data.append([batch_info,file_list,i,block_size,frequency,obs_time_maxes[0],obs_time_maxes[1],obs_time_maxes[2],[obs1_freq_int,obs3_freq_int,obs5_freq_int],k1,k2,k3,k4,k5,k6,k_score,min_k,med_k,max_k,drift,drifting,blip_or_broadband])
        
        
        
        except Exception:
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ERROR ON BLOCK {i} XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            print(traceback.print_exc())
            k_score_table.loc[len(k_score_table.index)] = [batch_info,file_list,i,block_size,fch1+foff*(i*block_size),math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan,math.nan]

    if sectioning == "False":
        raise NotImplementedError(
            'This bit of the code references something that was never defined? '
            'Don\'t let sectioning = \'True\''
        )
        if len(filtered_hotspots) == 0:
            print('empty observation')
            k_score_table.loc[len(k_score_table.index)] = [batch_info,h5_files,0,block_size,0,[],[],[],[],np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]

        return k_score_table
    elif sectioning == "True":
        return k_score_table_data
























"""Data Functions"""





def get_all_h5_files(target):
    """Returns a list containaing cadences grouped together as tuples, as well as a list of all unique nodes

    Args:
        target (str): Galaxy/Star (or overarching file folder) you are looking at

    :Returns:
        - h5_list (list): list containaing cadences grouped together as tuples
        - unique_nodes (list): list of all unique nodes
    """


    # initialize list to store h5 files
    h5_list = []

    # first change directory into the target directory
    os.chdir(target)
    data_dir = os.getcwd() + "/"

    # we want to get all the unique nodes
    unique_nodes = get_unique_nodes(data_dir)

    for node in unique_nodes:
    # then loop through and grab all the file names
        node_set = get_node_file_list(data_dir,node)
        h5_list.append(node_set)

    return h5_list, unique_nodes




def get_file_properties(f):
    """Get file properties of given h5 file.

    Args:
        f (h5 object): h5 file corresponding to desired observation

    Returns:
        fch1 (float): start frequency of observation in Mhz
        foff (float): frequency of each bin in Mhz

    """
    tstart=f['data'].attrs['tstart']
    fch1=f['data'].attrs['fch1']
    foff=f['data'].attrs['foff']
    nchans=f['data'].attrs['nchans']
    ra=f['data'].attrs['src_raj']
    decl=f['data'].attrs['src_dej']
    target=f['data'].attrs['source_name']
    # print("tstart %0.6f fch1 %0.10f foff %0.30f nchans %d cfreq %0.10f src_raj %0.10f src_raj_degs %0.10f src_dej %0.10f target %s" % (tstart,fch1,foff,nchans,(fch1+((foff*nchans)/2.0)),ra,ra*15.0,decl,target))
    print("Start Channel: %0.10f Frequency Bin: %0.30f # Channels: %d" % (fch1,foff,nchans))

    return fch1, foff, nchans


def find_hotspots(row,first_round,block_size,significance_level):
    
    hotspots = []
    for i in first_round:
        slice_ON = row[i*block_size:(i+1)*block_size:]
        snr,threshold = get_snr(slice_ON,significance_level)
        if snr:
            hotspots.append(i)

    return hotspots

def find_warmspots(row,number,block_size):
    first_round = []
    first_round_multiplier = 5
    # iterate

    
    for i in range(0,number):
        slice_ON = row[i*block_size:(i+1)*block_size:]
        snr,threshold = get_first_round_snr(slice_ON,first_round_multiplier)
        if snr:
            first_round.append(i)
    
    return first_round

def filter_hotspots(hotspots,fch1,foff,block_size):
    """Filters out hotspots in RFI heavy regions. 

    Args:
        hotspots (list): List of hotspot regions found previously
        fch1 (float): start frequency of observation in Mhz
        foff (float): frequency of each bin in Mhz

    Returns:
        all_indexes: Remaining hotspots after filtering
    """

    # define regions that are RFI heavy:
    bad_regions = [[700,1100],[1160,1340],[1370,1390],[1520,1630],[1670,1705],[1915,2000],[2025,2035],[2100,2120],[2180,2280],[2300,2360],[2485,2500],[2800,4400],[4680,4800],[8150,8350],[9550,9650],[10700,12000]]
    # first convert hotspots indexes to frequency channels
    # same as np.linspace(fch1, fch1 + foff * len(hotspots) * block_size, dtype = int)
    hotspots_frequencies = np.array([int((fch1+foff*(i*block_size))) for i in hotspots])

    hotspots_frequencies = np.sort(hotspots_frequencies)
    print(hotspots_frequencies)
    all_indexes = []
    # iterate through bad regions and remove all hotspots in them
    for i in bad_regions:
        bottom = int(i[0])
        top = int(i[1])
        # same as indexes = np.nonzero((bottom < hotspot_frequencies) & (hotspot_frequencies < top))
        indexes = np.where(np.logical_and(bottom<hotspots_frequencies, hotspots_frequencies<top))
        indexes = indexes[0]
        indexes = [int(i) for i in indexes]
        for i in indexes:
            all_indexes.append(i)

    # return filtered hotspots
    return all_indexes


def get_unique_nodes(data_dir):
    """Grabs the unique blc nodes in a given directory

    Args:
        data_dir (str): Data directory to search through

    Returns:
        unique_nodes (list): List of all unique nodes in the directory, sorted.
    """
    node_list = []
    for dirname, _, filenames in os.walk(data_dir):
        for filename in filenames:
            # we remove the start and end nodes as these have low sensitivity
            if "blc" in filename and (filename[4] != '7') and (filename[4] != '0'):
                node_list.append(filename[:5])

    node_set = set(node_list)
    print("Unique nodes:", node_set)

    unique_nodes = sorted(node_set)
    unique_nodes.sort()
    return unique_nodes


def filter_zero_drift(obs1,obs2,obs3,obs4,obs5,obs6,filtering_level):
    # checking if there are lots of zero drift signals in OFF observations, or ON and OFF observations
        
    # plot the integrated frequency
    obs1_int = obs1.sum(axis=0)
    obs2_int = obs2.sum(axis=0)
    obs3_int = obs3.sum(axis=0)
    obs4_int = obs4.sum(axis=0)
    obs5_int = obs5.sum(axis=0)
    obs6_int = obs6.sum(axis=0)

    obs_sums = [obs1_int,obs2_int,obs3_int,obs4_int,obs5_int,obs6_int]


    whole_sum = obs1_int+obs2_int+obs3_int+obs4_int+obs5_int+obs6_int
    off_sum = [obs2_int,obs4_int,obs6_int]
        
    on_sum_list = [obs1_int,obs3_int,obs5_int]
    on_sum_sum = obs1_int+obs3_int+obs5_int

    if filtering_level < 3:
        whole_sum = off_sum

    all_peaks = []
    all_peak_freqs = []

    for i,obs_int in enumerate(on_sum_list):
        obs_data = obs_int/np.max(obs_int)
        sigma_mult = scipy.stats.median_abs_deviation(obs_data)
        peaks, properties = find_peaks(obs_data, prominence=10*sigma_mult, width=1,distance=10)
        freqs = np.arange(0,len(whole_sum),1)

        # print('peaks',freqs[peaks])
        all_peak_freqs.append(freqs[peaks])

    for i,obs_int in enumerate([on_sum_sum]):
        obs_data = obs_int/np.max(obs_int)
        sigma_mult = scipy.stats.median_abs_deviation(obs_data)
        peaks, properties = find_peaks(obs_data, prominence=10*sigma_mult, width=1,distance=10)
        freqs = np.arange(0,len(whole_sum),1)

        all_peaks.append(len(peaks))


    peak_drift_1 = []
    peak_drift_2 = []
    peak_drift_3 = []

    if len(all_peak_freqs[1]) != 0:
        peak_drift_1 = find_closest_elements(all_peak_freqs[0],all_peak_freqs[1])

    if len(all_peak_freqs[2]) != 0:
        peak_drift_2 = find_closest_elements(all_peak_freqs[1],all_peak_freqs[2])
        
    if len(all_peak_freqs[2]) != 0:
        peak_drift_3 = find_closest_elements(all_peak_freqs[0],all_peak_freqs[2])

    return all_peaks[-1], peak_drift_1,peak_drift_2,peak_drift_3

def find_closest_elements(a, b):
    result = []
    
    for element_a in a:
        closest_element_b = min(b, key=lambda x: abs(x - element_a))
        difference = abs(element_a - closest_element_b)
        result.append(difference)
    
    return result

def get_snr(sliced,sigma_multiplier):
    """Checks for any high SNR bins in the given frequency snippet and flags them.

    Args:
        sliced (numpy array): frequency snippet from observation
        sigma_multiplier (int): SNR threshold for a signal to count as significant

    Returns:
        snr (boolean): True if there is a high SNR signal, False if not
        threshold (int): Threshold that normalized data needs to be above in order to count as signal. 
    """
    # this is just like get_first_round_snr but with MAD instead of std
    # so essentially the same as 
    """
    threshold = "sigma"_multiplier * MAD + median
    return max >= threshold, threshold / max    
    """
    snr = False
    # divide by max to make numbers smaller
    sliced = sliced/np.max(sliced)

    # remove top 30 percent of values to get real baseline (in case there are many high value signals). 
    # lower_quantile = np.quantile(sliced,.85)
    # lower_slice = sliced[sliced < lower_quantile]

    # # get median and standard deviation of baseline
    # i think that's old code, this gets the MAD
    median = np.median(sliced)
    # sigma = np.std(lower_slice)

    sigma = scipy.stats.median_abs_deviation(sliced)

    # calcualate threshold as median of baseline + SNR * standard deviation
    threshold = median+sigma_multiplier*sigma
    if np.max(sliced) > threshold:
        snr = True

    return snr, threshold


def get_first_round_snr(sliced,first_round_multiplier):
    """Preliminary filter to find any regions with a certain SNR that is smaller than the specificed cutoff.
        Calculating the quantile of a lot of regions is time intensive, so better to narrow down search field first. 

    Args:
        sliced (numpy array): frequency snippet from observation
        first_round_multiplier (int): Lower SNR required for regions to get passed on to next round of filtering

    Returns:
        snr (boolean): True if there is a high SNR signal, False if not
        threshold (int): Threshold that normalized data needs to be above in order to count as signal. 
    """
    ## returns True if the max of a snippet is 5 sigma above the median
    ## essentially the same as
    """
    threshold = np.median(sliced) + first_round_multiplier * np.std(sliced)
    max_power = np.max(sliced)
    return max_power >= threshold, threshold / max_power
    """

    snr = False
    sliced = sliced/np.max(sliced)

    median = np.median(sliced)
    sigma = np.std(sliced)
    threshold = median+first_round_multiplier*sigma

    if threshold <= 1:
        snr = True

    return snr, threshold

def drift_index_checker(whole_sum, row_ON,significance_level,min_distance):
    """Checks if drift rate == 0. Compares all signals that set off hotspot to those in the full observation summed

    Args:
        whole_sum (numpy array): 2D array representing entire cadence summed 
        row_ON (numpy array): 1D array representing last time row of first observation
        significance_level (int): Minimum SNR for signal to be considered present

    Returns:
        zero_drift (Boolean): True if signal has zero drift, False if not
    """

    whole_sum = whole_sum/np.max(whole_sum)
    row_ON = row_ON/np.max(row_ON)

    zero_drift = False

    # we check if when we sum the entire observation, we pick up the signal that set off the hotspot. 
    # Will only do this if there are same number of peaks in on ROw and summed, in case there was a genuine signal in the ON row

    # get the peaks in the last row and the summed cadence
    hotspot_snr, hotspot_threshold = get_snr(row_ON,significance_level)
    summed_snr, summed_threshold = get_snr(whole_sum,significance_level)

    hotspot_indices = np.where(np.array(row_ON) > hotspot_threshold)[0].tolist()
    summed_indices = np.where(np.array(whole_sum) > summed_threshold)[0].tolist()

    # average any points very close together
    # print(hotspot_indices,summed_indices)


    if len(hotspot_indices) != 0 and len(summed_indices) != 0:

        filtered_hotspot_indices = [hotspot_indices[0]]
        for i in hotspot_indices[1:]:
            if abs(filtered_hotspot_indices[-1] - i) <10:
                filtered_hotspot_indices.pop()
                filtered_hotspot_indices.append(i-.5)
            else:
                filtered_hotspot_indices.append(i)


        filtered_summed_indices = [summed_indices[0]]
        for i in summed_indices[1:]:
            if abs(filtered_summed_indices[-1] - i) <5:
                filtered_summed_indices.pop()
                filtered_summed_indices.append(i-.5)
            else:
                filtered_summed_indices.append(i)

        # check if all hotspots picked up in ON row are in the summed
        all = 0
        for i in filtered_hotspot_indices:
            for j in filtered_summed_indices:
                if abs(i-j) < min_distance:
                    all +=1 
        if all >= len(filtered_hotspot_indices):
            zero_drift = True

    return zero_drift


def get_node_file_list(data_dir,node_number):
    """Returns the list of h5 files associated with a given node

    Args:
        data_dir (str): Data directory to search through
        node_number (str): Node number to filter on

    Returns:
        data_list (list): List of h5 files that make up the cadence, sorted chronlogically
    """


    ## h5 list
    data_list = []
    for dirname, _, filenames in os.walk(data_dir):
        for filename in filenames:
            if filename[-3:] == '.h5' and node_number in filename:
                data_list.append(data_dir + filename)
                
    data_list = sorted(data_list, key=lambda x: (x,x.split('_')[5]))

    return data_list

if __name__ == '__main__':
    batch_number = sys.argv[1] ## batch number or a specific target (see top)
    section_bool = sys.argv[2] ## whether or not we are loading individual frequency blocks 1 by 1, or large sections of the observation.
    rerun = sys.argv[3] ## whether we have already run on this batch/target
    main(batch_number, section_bool,rerun)


    '''
    Maybe useful vocabulary for understanding this code?
    
    frequency window/snippet --> the small frequency block (in this case 1024 frequency bins or ~ 3 kHz)
    observation section --> a large slice of the observation file (in this case 1/8th of it) that is loaded ahead of time for faster input/output times
    '''