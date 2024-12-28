#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import glob
from shutil import copyfile
import hashlib
import json
import sys
import subprocess
import logging
from multiprocessing.pool import ThreadPool as Pool
import pdb
import time
import pickle
import argparse
import numpy as np
import pandas as pd
import pydicom as pyd
import png
from pydicom.pixel_data_handlers import apply_voi_lut
# pydicom imports needed to handle data errors
from pydicom import config
from pydicom import values
import sys
import pathlib
from pathlib import Path
from functools import partial
import re 
from tqdm import tqdm 

class ExtractorRegister: 
    __data = {} 
    @classmethod 
    def register(cls,cls_name=None):
        def decorator(cls_obj):
            cls.__data[cls_name] = cls_obj
            return cls_obj
        return decorator
    @classmethod
    def get_extractor(cls,key):
        return cls.__data[key]
    @classmethod
    def get_extractors(cls):
        return list(cls.__data.keys() )
    @classmethod
    def build_extractor(cls,conf):
        key = conf['Extractor']
        extractor = cls.get_extractor(key)
        return extractor(conf)

@ExtractorRegister.register("PNG")
class PngExtractor(): 
    def __init__(self,config) -> None:
        self.config= config
        self.dicom_home= str(
            pathlib.PurePath(config["DICOMHome"])
        )  # parse the path and convert it to a string
        self.output_directory = str(pathlib.Path(config["OutputDirectory"]))
        self.print_images = config["SavePNGs"]
        self.PublicHeadersOnly = config["PublicHeadersOnly"]
        self.processes = config["NumProcesses"]
        self.SaveBatchSize = config["SaveBatchSize"]
        self.png_destination = os.path.join(self.output_directory, "extracted-images")
        self.failed = os.path.join(self.output_directory, "failed-dicom")
        meta_directory = os.path.join(self.output_directory, "meta")
        LOG_FILENAME = os.path.join(self.output_directory, "ImageExtractor.out")
        #this will be a list of all the dicom files to extract 
        self.pickle_file = os.path.join(
            self.output_directory, "ImageExtractor.pickle"
        ) 
        self.ApplyVOILUT= config['ApplyVOILUT']
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
    def get_dicom_files(self): 
        filelist = Path(self.dicom_home).rglob("*.dcm")
        return filelist

    def execute(self):
        err = None
        fix_mismatch()  # TODO: hold over from old processing code could be improved? 
        core_count =self.processes 
        # gets all dicom files. if editing this code, get filelist into the format of a list of strings,
        # with each string as the file path to a different dicom file.
        if os.path.isfile(self.pickle_file):
            f = open(self.pickle_file, "rb")
            filelist = pickle.load(f)
        else:
            print(self.dicom_home)
            print("Getting all the dcms in your project. May take a while :)")
            filelist = self.get_dicom_files()
        # TODO: if there is a more understandable way of using imap where some parameters that are constant let me know
        # some version of python has it so you can define keyword args will look into later
        extractor = partial(
            self.image_proc,
            pngDestination=self.png_destination,
            publicHeadersOnly=self.PublicHeadersOnly,
            failDir=self.failed,
            print_images =self.print_images,
        )
        t_start = time.time()
        self.run_extraction(extractor,filelist,core_count=core_count,SaveBatchSize=self.SaveBatchSize,output_dir=self.output_directory)
        meta_directory = f"{self.output_directory}/meta/"
        metas = glob.glob(f"{meta_directory}*.csv")
        merged_meta = pd.DataFrame()
        # TODO:  Right now we do not fillter out empty metadata columsn. add it in the future?
        for meta in metas:
            m = pd.read_csv(meta, dtype="str")
            merged_meta = pd.concat([merged_meta, m], ignore_index=True)
        merged_meta.to_csv("{}/metadata.csv".format(self.output_directory), index=False)
        logging.info("Total run time: %s %s", time.time() - t_start, " seconds!")
        logging.shutdown()  # Closing logging file after extraction is done !!
        logs = []
        logs.append(err)
        logs.append("The PNG conversion is SUCCESSFUL")
        return logs
    def run_extraction(self,extract_func,file_gen,core_count=1,SaveBatchSize=25,output_dir=None):
        counter =0 
        with Pool(core_count) as p:
            meta_rows= list()
            for i, dcm_meta in tqdm(enumerate(p.imap_unordered(extract_func,file_gen))):
                if  dcm_meta is None:  #SHOULD ONLY BE NONE IF THINGS ARE FILTERED
                    continue 
                meta_rows.append(dcm_meta)
                if len(meta_rows) >= SaveBatchSize:
                    meta_df = pd.DataFrame(meta_rows)
                    meta_rows = list()
                    csv_destination = f"{output_dir}/meta/metadata_{counter}.csv"
                    counter += 1
                    meta_df.to_csv(csv_destination) 
        #save if theres any remaining data 
        if meta_rows: 
            meta_df = pd.DataFrame(meta_rows)
            meta_rows = list()
            csv_destination = f"{output_dir}/meta/metadata_{counter}.csv"
            counter += 1
            meta_df.to_csv(csv_destination) 

    def image_proc(self,
        dcmPath,
        pngDestination: str = None,
        publicHeadersOnly: str = None,
        failDir: str = None,
        print_images=None,
        ApplyVOILUT=False
    ):
        """
        Run the dicom extraction. We first extract the metadata then we save the image informaiton
        dcm_path: absolute path to a dicom file
        pngDestination: Where to store extracted pngs
        publicHeadersOnly: only use public headers
        """
        dcm = pyd.dcmread(dcmPath, force=True)
        dicom_tags = extract_dcm(dcm, dcm_path=dcmPath, PublicHeadersOnly=publicHeadersOnly)
        if print_images and dicom_tags is not None:
            png_path, err_code = self.extract_images(
                dcm, png_destination=pngDestination, failed=failDir,ApplyVOILUT=ApplyVOILUT
            )
        else:
            png_path = None
        dicom_tags["png_path"] = png_path
        dicom_tags["err_code"] = err_code
        return dicom_tags
    # Function to extract pixel array information
    # takes an integer used to index into the global filedata dataframe
    # returns tuple of
    # filemapping: dicom to png paths   (as str)
    # fail_path: dicom to failed folder (as tuple)
    # found_err: error code produced when processing
    def extract_images(ds, png_destination,ApplyVOILUT=False):
        """
        Function that  extracts a dicom pixel arrayinto a png image. Patient metadata is used to create the file name
        Supports extracting either RGB or Monochrome images. No LUT or VOI is applied at the moment
        Returns
        pngFile --> path to extract png file or None if extraction failed
        err_code --> error code experience dduring extraction. None if no failoure occurs
        """
        err_code = None
        found_err = None
        try:
            ID1 = str(ds.PatientID)
            try:
                ID2 = str(ds.StudyInstanceUID)
            except:
                ID2 = "ALL-STUDIES"
            try:
                ID3 = str(ds.SOPInstanceUID)
            except:
                ID3 = "ALL-SERIES"
            folderName = (
                hashlib.sha224(ID1.encode("utf-8")).hexdigest()
                + "/"
                + hashlib.sha224(ID2.encode("utf-8")).hexdigest()
            )
            img_iden = f"{ID3}"
            img_name = hashlib.sha224(img_iden.encode("utf-8")).hexdigest() + ".png"

            # check for existence of the folder tree patient/study/series. Create if it does not exist.

            store_dir = os.path.join(png_destination, folderName)
            os.makedirs(store_dir, exist_ok=True)
            pngfile = os.path.join(store_dir, img_name)
            image_2d_scaled,shape ,isRGB,bit_depth = process_image(ds,is16Bit=True,ApplyVOILUT=ApplyVOILUT)
            with open(pngfile, "wb") as png_file:
                if isRGB:
                    image_2d_scaled = rgb_store_format(image_2d_scaled)
                    greyscale = False
                else: 
                    greyscale =True
                w = png.Writer(shape[1], shape[0], greyscale=greyscale,bitdepth=bit_depth)
                w.write(png_file, image_2d_scaled)
        except AttributeError as error:
            found_err = error
            logging.error(found_err)
            err_code = 1
            pngfile = None
        except ValueError as error:
            found_err = error
            logging.error(found_err)
            err_code = 2
            pngfile = None
        except BaseException as error:
            found_err = error
            logging.error(found_err)
            err_code = 3
            pngfile = None
        return (pngfile, err_code)



# Function for getting tuple for field,val pairs
def get_tuples(plan, PublicHeadersOnly, key=""):
    plans = plan.dir()
    outlist = list()
    for tag in plans: 
        try: 
            hasattr(plan,tag)
        except TypeError as e: 
            logging.warning(f"Type Error Occured parsing tag {tag}") 
        if hasattr(plan, tag) and tag != 'PixelData':
            value = getattr(plan, tag)
            # if dicom sequence extract tags from each element
            if type(value) is pyd.sequence.Sequence:
                for nn, ss in enumerate(list(value)):
                    newkey = "_".join([key, ("%d" % nn), tag]) if len(key) else "_".join([("%d" % nn), tag])
                    candidate = get_tuples(ss, PublicHeadersOnly, key=newkey)
                    if len(candidate)>=25:
                        logging.warning(f"Expansion of tag {tag} has more than 10 items. Check if needed")
                        continue 
                    for n_tag,n_val in candidate: 
                        if re.match("\d{1,}_",n_tag):
                            continue
                        else:
                            outlist.append((newkey+n_tag,value))
            else:
                if type(value) is pyd.valuerep.DSfloat:
                    value = float(value)
                elif type(value) is pyd.valuerep.IS:
                    value = str(value)
                elif type(value) is pyd.valuerep.MultiValue:
                    value = tuple(value)
                elif type(value) is pyd.uid.UID:
                    value = str(value)
                outlist.append((key + tag, value))
    return outlist



def extract_dcm(
    plan: pyd.Dataset,
    dcm_path: str,
    PublicHeadersOnly: bool = False,
    FailDirectory: str = None,
):
    """ "
    Extract dicom tags from dicom file. Public tags are filtered if specified.
    PNG
    """
    # checks all dicom fields to make sure they are valid
    # if an error occurs, will delete it from the data structure
    dcm_dict_copy = list(plan._dict.keys())

    for tag in dcm_dict_copy:
        try:
            plan[tag]
        except:
            logging.warning("dropped fatal DICOM tag {}".format(tag))
            del plan[tag]
    c = True
    try:
        check = plan.pixel_array  # throws error if dicom file has no image
    except:
        c = False
    kv = get_tuples(
        plan, PublicHeadersOnly
    )  # gets tuple for field,val pairs for this file. function defined above
    dicom_tags_limit = (
        1000  # TODO: i should add this as some sort of extra limit in the config
    )
    if (
        len(kv) > dicom_tags_limit
    ):  # TODO this should not fail silently. What can we do  about it
        logging.debug(str(len(kv)) + " dicom tags produced by " + dcm_path)
    kv.append(("file", dcm_path))  # adds my custom field with the original filepath
    kv.append(("has_pix_array", c))  # adds my custom field with if file has image
    if c:
        # adds my custom category field - useful if classifying images before processing
        kv.append(("category", "uncategorized"))
    else:
        kv.append(
            ("category", "no image")
        )  # adds my custom category field, makes note as imageless

    kv = dict(kv)
    if 'Pixel Data' in kv: 
        del kv['Pixel Data']
    return kv  


def rgb_store_format(arr):
    """Create a  list containing pixels  in format expected by pypng
    arr: numpy array to be modified.

    We create an array such that an  nxmx3  matrix becomes a list of n elements.
    Each element contains m*3 items.
    """
    out = list(arr)
    flat_out = list()
    for e in out:
        flat_out.append(list())
        for k in e:
            flat_out[-1].extend(k)
    return flat_out

def process_image(ds,is16Bit,ApplyVOILUT=False):
    try:
        isRGB = ds.PhotometricInterpretation.value == "RGB"
    except:
        isRGB = False
    image_2d = ds.pixel_array 
    if ApplyVOILUT: 
        image_2d = apply_voi_lut(image_2d,ds,prefer_lut=True)
    image_2d = image_2d.astype(float)
    shape = ds.pixel_array.shape
    if is16Bit:
        # write the PNG file as a 16-bit greyscale
        image_2d_scaled = (np.maximum(image_2d, 0) / image_2d.max())  * (2**16 -1)
        # # Convert to uint
        image_2d_scaled = np.uint16(image_2d_scaled)
        bit_depth = 16
    else:
        # Rescaling grey scale between 0-255
        image_2d_scaled = (np.maximum(image_2d, 0) / image_2d.max()) * 255.0
        # onvert to uint
        image_2d_scaled = np.uint8(image_2d_scaled)
        bit_depth = 8 
        # Write the PNG file
    return image_2d_scaled,shape,isRGB,bit_depth


# Function when pydicom fails to read a value attempt to read as other types.
def fix_mismatch_callback(raw_elem, **kwargs):
    """
    Specify alternative variable reprepresentations when trying to parse metadata.
    """
    try:
        if raw_elem.VR:
            values.convert_value(raw_elem.VR, raw_elem)
    except TypeError as err:
        logging.error(err)
    except BaseException as err:
        for vr in kwargs["with_VRs"]:
            try:
                values.convert_value(vr, raw_elem)
            except ValueError:
                pass
            except TypeError:
                continue
            else:
                raw_elem = raw_elem._replace(VR=vr)
    return raw_elem


# taken from pydicom docs
def fix_mismatch(with_VRs=["PN", "DS", "IS", "LO", "OB"]):
    """A callback function to check that RawDataElements are translatable
    with their provided VRs.  If not, re-attempt translation using
    some other translators.
    Parameters
    ----------
    with_VRs : list, [['PN', 'DS', 'IS']]
        A list of VR strings to attempt if the raw data element value cannot
        be translated with the raw data element's VR.
    Returns
    -------
    No return value.  The callback function will return either
    the original RawDataElement instance, or one with a fixed VR.
    """
    pyd.config.data_element_callback = fix_mismatch_callback
    config.data_element_callback_kwargs = {
        "with_VRs": with_VRs,
    }


        




