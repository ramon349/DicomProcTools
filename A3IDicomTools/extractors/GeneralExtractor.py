
from typing import Dict,List
from .PngExtractor import ExtractorRegister,fix_mismatch
import pathlib
import logging 
import os 
from glob import  glob 
import time 
import pandas as pd 
from functools import partial 
import  pickle 
from pathlib import Path 
from multiprocessing import Pool 
from tqdm import tqdm
import pydicom as pyd 
from enum import Enum,StrEnum

from ..help_data.uid_categories import _mr_tags,_tomo_tags,_xray_tags

class StorageClass(StrEnum): 
    MRCT = "MRCT"
    TOMO = "TOMO" 
    XRAY = "XRAY"
    OTHER = "OTHER"

@ExtractorRegister.register("General")
class GeneralExtractor():
    def __init__(self,config) -> None:
        self.config= config
        self.dicom_home= str(
            pathlib.PurePath(config["DICOMHome"])
        )  # parse the path and convert it to a string
        self.output_directory = str(pathlib.Path(config["OutputDirectory"]))
        self.print_images = config["SaveImages"]
        self.PublicHeadersOnly = config["PublicHeadersOnly"]
        self.processes = config["NumProcesses"]
        self.SaveBatchSize = config["SaveBatchSize"]
        self.png_destination = os.path.join(self.output_directory, "extracted-images")
        self.failed = os.path.join(self.output_directory, "failed-dicom")
        meta_directory = os.path.join(self.output_directory, "meta")
        self.meta_counter= 0 #used for keeping track of what metadata file we are to write
        LOG_FILENAME = os.path.join(self.output_directory, "ImageExtractor.out")
        #this will be a list of all the dicom files to extract 
        self.pickle_file = os.path.join(
            self.output_directory, "ImageExtractor.pickle"
        ) 
        self.ApplyVOILUT= config['ApplyVOILUT']
        self.ExtractNested = config['ExtractNested']
        self.Debug = config['Debug']
        self.populate_extraction_dirs()
        logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
        logging.info("------- Values Initialization DONE -------")

        
    def populate_extraction_dirs(self):
        """
        Images and metadata will be stored in output_directory. This function defines subfolders such as
        metadata and failed dicom folders
        output_directory: str   absolute path to the folder meant to store output artiacts (logs,images,metadata)
        """
        self.png_destination = os.path.join(self.output_directory, "extracted-images")
        self.failed = os.path.join(self.output_directory, "failed-dicom")
        self.meta_directory = os.path.join(self.output_directory, "meta")
        if not os.path.exists(self.meta_directory):
            os.makedirs(self.meta_directory)
        if not os.path.exists(self.png_destination):
            os.makedirs(self.png_destination)
        if not os.path.exists(self.failed):
            os.makedirs(self.failed)
        for e in range(6):
            fail_dir = os.path.join(self.failed, str(e))
            if not os.path.exists(fail_dir):
                os.makedirs(fail_dir)
        print(f"Done Creating Directories")
    def _get_filelist(self) -> Dict[str,List[str]]: 
        """
            Produces list of dicom files to extract. Saves the list as a pickle file
            In the case of a workloads that are resumed we use exisitng metadata to prune the filelist
            and resume from the remainig list
        """
        if os.path.isfile(self.pickle_file):
            f = open(self.pickle_file, "rb")
            filedict = pickle.load(f) 
            #TODO: Add the pruning of the extraction list again 
        else:
            filedict = self.get_dicom_files()
            if os.path.isdir(self.meta_directory): #means we are resumming without pickle
                print(f"We didn't have a pickle file but resuming using found metadata")
                #TODO we need to bring back prunning based on the metadata 
                pass 
            self._write_filelist(filedict)
        if self.Debug:
            #make the debug shortening occur again 
            pass 
        for k in filedict:
            logging.info(f"For {k} we have {len(filedict[k])}")
        return filedict 
    def execute(self):
        fix_mismatch()  # TODO: hold over from old processing code could be improved? 
        # gets all dicom files. if editing this code, get filelist into the format of a list of strings,
        # with each string as the file path to a different dicom file.
        filedict = self._get_filelist()
        #HEre i need a filtering step for MR or CT #TODO
        filelist = self._make_proc_list(filedict)
        total_len = sum([len(v) for k,v in filedict.items()])
        self.run_extraction(filelist,total_len)

    def _make_proc_list(self,filedict):
        #make it a generator for fun 
        for sop_key,sop_list in filedict.items(): 
            for dcm in  sop_list: 
                yield (sop_key,dcm)

    def run_extraction(self,filelist,total_len):

        with Pool(self.processes) as p: 
            meta_rows = list()
            proc = p.imap_unordered(general_extract,filelist)
            for i,dcm_meta in tqdm(enumerate(proc),total=total_len):
                if dcm_meta is None: 
                    continue 
                meta_rows.append(dcm_meta)
                if len(meta_rows) >= self.SaveBatchSize:
                    meta_df = pd.DataFrame(meta_rows)
                    meta_rows = list() 
                    csv_destination = f"{self.output_directory}/meta/metadata_{self.meta_counter}.csv"
                    self.meta_counter += 1
                    meta_df.to_csv(csv_destination) 
        if meta_rows:
            meta_df = pd.DataFrame(meta_rows)
            meta_rows = list()
            csv_destination = f"{self.output_directory}/meta/metadata_{self.meta_counter}.csv"
            self.meta_counter += 1
            meta_df.to_csv(csv_destination) 


    
    def get_dicom_files(self):
        from collections import defaultdict
        storage_d = defaultdict(list)
        with Pool(self.processes) as P: 
            path_glob = Path(self.dicom_home).rglob("*.dcm")
            proc = P.imap_unordered(read_and_categorize_dcm,path_glob)
            for path,store_class in tqdm(proc,desc="Reading and categorizing DCMS"): 
                storage_d[store_class].append(path)
        return storage_d 
    def _write_filelist(self,filelist): 
        with open(self.pickle_file,'wb') as f: 
            pickle.dump(filelist,f)
    
def read_and_categorize_dcm(dcm_path:pathlib.Path): 
    sop_class_uid_tag = [0x0008,0x0016]
    dcm = pyd.dcmread(dcm_path,stop_before_pixels=True,specific_tags=[sop_class_uid_tag])
    sop_class_uid = dcm[sop_class_uid_tag].value 
    store_class = StorageClass.OTHER
    if  sop_class_uid in _mr_tags: 
        store_class = StorageClass.MR
    if sop_class_uid in  _xray_tags: 
        store_class = StorageClass.XRAY
    if sop_class_uid in  _tomo_tags: 
        store_class = StorageClass.TOMO 
    return dcm_path,store_class 

def general_extract(work_tup,save_path=None,print_images=None):
    sop_tag,dcm_path =  work_tup
    match sop_tag: 
        case StorageClass.MRCT: 
            #call the mr CT processor 
            pass 
        case StorageClass.TOMO: 
            #call the TIMI procesor
            pass 
        case StorageClass.XRAY:
            pass 
    return dcm_tags 

