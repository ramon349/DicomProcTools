
from collections import defaultdict
from typing import Dict,List

from numpy import extract
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
from .functional_extractors import process_ctmri, process_png,process_tomo,process_general
from ..help_data.uid_categories import _mr_tags,_tomo_tags,_xray_tags
from glob import glob 
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
        self.img_destination = os.path.join(self.output_directory, "extracted-images")
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
            filedict= self.prune_extracted(filedict)
            #TODO: Add the pruning of the extraction list again 
        else:
            filedict = self.get_dicom_files()
            if os.path.isdir(self.meta_directory): #means we are resumming without pickle
                print(f"We didn't have a pickle file but resuming using found metadata")
                #TODO we need to bring back prunning based on the metadata 
                filedict= self.prune_extracted(filedict)
            self._write_filelist(filedict)
        if self.Debug:
            #make the debug shortening occur again 
            pass 
        for k in filedict:
            logging.info(f"For {k} we have {len(filedict[k])}")
        return filedict 
    def prune_extracted(self,file_dict):
        """
            Shortens extraction file list using exisitng metadta csvs.
        """
        meta_csvs = [str(e) for e in Path(self.meta_directory).rglob("*.csv")]
        all_files = set() 
        max_batch=0 
        for e in tqdm(meta_csvs,total=len(meta_csvs)): 
            batch_id= int(os.path.split(e)[1].split('.')[0].split('_')[1])
            df = pd.read_csv(e,dtype='str',usecols=['file'])
            all_files.update(df['file'].unique().tolist())
            if batch_id>= max_batch: 
                max_batch= batch_id
        print(f"Number of extracted files was {len(all_files)}") 
        filtered_filed = defaultdict(list) 
        to_proc = 0
        for k in file_dict:
            for e in file_dict[k]: 
                if str(e) not in all_files: 
                    filtered_filed[k].append(e)
                    to_proc +=0
        print(f"Resuming Extraction with {to_proc}")
        self.meta_counter = max_batch+1  # we do +1 because we will start writing the next batch
        return filtered_filed        
    def execute(self):
        fix_mismatch()  # TODO: hold over from old processing code could be improved? 
        # gets all dicom files. if editing this code, get filelist into the format of a list of strings,
        # with each string as the file path to a different dicom file.
        filedict = self._get_filelist()
        #HEre i need a filtering step for MR or CT #TODO
        filelist = self._make_proc_list(filedict)
        total_len = sum([len(v) for k,v in filedict.items()])
        t_start = time.time()
        self.run_extraction(filelist,total_len)
        meta_directory = f"{self.output_directory}/meta/"
        metas = glob(f"{meta_directory}*.csv")
        merged_meta = pd.DataFrame()
        # TODO:  Right now we do not fillter out empty metadata columsn. add it in the future?
        for meta in metas:
            m = pd.read_csv(meta, dtype="str")
            merged_meta = pd.concat([merged_meta, m], ignore_index=True)
        merged_meta.to_csv("{}/metadata.csv".format(self.output_directory), index=False)
        logging.info("Total run time: %s %s", time.time() - t_start, " seconds!")
        logging.shutdown()  # Closing logging file after extraction is done !!

    def _make_proc_list(self,filedict):
        #make it a generator for fun 
        for sop_key,sop_list in filedict.items(): 
            for dcm in  sop_list: 
                yield (sop_key,dcm)

    def run_extraction(self,filelist,total_len):

        with Pool(self.processes) as p: 
            meta_rows = list()
            extract_func = partial(general_extract,print_images=self.print_images,save_dir=self.img_destination)
            proc = p.imap_unordered(extract_func,filelist)
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
            path_glob = self.filter_generator(Path(self.dicom_home).rglob("*.dcm"))
            proc = P.imap_unordered(read_and_categorize_dcm,path_glob)
            for path,store_class in tqdm(proc,desc="Reading and categorizing DCMS"): 
                storage_d[store_class].append(path)
        return storage_d 
    def filter_generator(self,path_generator): 
        seen_dirs = set() 
        for dcm_path in path_generator: 
            parent_path = str(dcm_path.parent) 
            if parent_path in seen_dirs: 
                continue
            else: 
                seen_dirs.add(parent_path) 
                yield dcm_path

    def _write_filelist(self,filelist): 
        with open(self.pickle_file,'wb') as f: 
            pickle.dump(filelist,f)
    
def read_and_categorize_dcm(dcm_path:pathlib.Path): 
    sop_class_uid_tag = [0x0008,0x0016]
    dcm = pyd.dcmread(dcm_path,stop_before_pixels=True,specific_tags=[sop_class_uid_tag])
    sop_class_uid = dcm[sop_class_uid_tag].value 
    store_class = StorageClass.OTHER
    if  sop_class_uid in _mr_tags: 
        store_class = StorageClass.MRCT
    if sop_class_uid in  _xray_tags: 
        store_class = StorageClass.XRAY
    if sop_class_uid in  _tomo_tags: 
        store_class = StorageClass.TOMO 
    return dcm_path,store_class 

def general_extract(work_tup,save_dir=None,print_images=None):
    sop_tag,dcm_path =  work_tup
    meta_row = None
    match sop_tag: 
        case StorageClass.MRCT: 
            #call the mr CT processor 
            meta_row = process_ctmri(dcm_path=dcm_path,save_dir=save_dir,print_images=print_images)
        case StorageClass.TOMO: 
            #call the TIMI procesor
            meta_row =  process_tomo(dcm_path=dcm_path,save_dir=save_dir,print_images=print_images)
        case StorageClass.XRAY:
            meta_row = process_png(dcm_path,save_dir=save_dir,print_images=print_images)
        case StorageClass.OTHER: 
            meta_row = process_general(dcm_path)
    return meta_row

