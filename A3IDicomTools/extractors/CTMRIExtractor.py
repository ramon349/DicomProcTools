import os
import hashlib
import logging
import numpy as np
import pydicom as pyd
import png
from .PngExtractor import PngExtractor,extract_dcm,ExtractorRegister
import nibabel as nib 
from enum import Enum
from pathlib import Path
import dicom2nifti

class CTMREnum(Enum):
    CT="1.2.840.10008.5.1.4.1.1.2"
    CT_ENHANCED ="1.2.840.10008.5.1.4.1.1.2.1"
    CT_LEGACY="1.2.840.10008.5.1.4.1.1.2.2"
    MR="1.2.840.10008.5.1.4.1.1.4"
    MR_ENCHANCED="1.2.840.10008.5.1.4.1.1.4.1"
    MR_ENHANCED_COLOR="1.2.840.10008.5.1.4.1.1.4.3"
    MR_ENHANCED_LEGACY="1.2.840.10008.5.1.4.1.1.4.4"



@ExtractorRegister.register("NIFTI")
class MRICTExractor(PngExtractor): 
    def __init__(self, config):
        super().__init__(config)
    def get_dicom_files(self):
        # we pull all the dicom file names like before
        #this time however we do some extra filtering 
        running_list = set() 
        filelist= super().get_dicom_files()
        #itrate through the generator
        for e in filelist: 
            #get the directory containing volume
            vol_dir = os.path.split(e)[0]
            #only yield if we haven't seen this dir before
            if vol_dir not in running_list: 
                running_list.add(vol_dir )
        return list(running_list)
    def image_proc(self, dcmPath, pngDestination = None, publicHeadersOnly = None, failDir = None, print_images=None):
        sample_dcm = next(Path(dcmPath).rglob("*.dcm"))
        dcm = pyd.dcmread(sample_dcm, force=True)
        dicom_tags = extract_dcm(dcm, dcm_path=sample_dcm, PublicHeadersOnly=publicHeadersOnly)
        if print_images and dicom_tags is not None:
            img_path, err_code = self.extract_images(
                dcmPath,dcm,png_destination=pngDestination
            )
        else:
            img_path = None
        dicom_tags["nifti_path"] = img_path
        dicom_tags["err_code"] = err_code
        return dicom_tags
    def extract_images(self,dcmDir,sample_dcm, png_destination):
        err_code = None
        found_err = None
        ds = sample_dcm
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
            img_name = hashlib.sha224(img_iden.encode("utf-8")).hexdigest() + ".nii.gz"

            # check for existence of the folder tree patient/study/series. Create if it does not exist.

            store_dir = os.path.join(png_destination, folderName)
            os.makedirs(store_dir, exist_ok=True)
            img_file_name = os.path.join(store_dir, img_name)
            dicom2nifti.dicom_series_to_nifti(dcmDir,img_file_name)
        except AttributeError as error:
            found_err = error
            logging.error(found_err)
            err_code = 1
            img_file_name = None
        except ValueError as error:
            found_err = error
            logging.error(found_err)
            err_code = 2
            img_file_name = None
        except BaseException as error:
            found_err = error
            logging.error(found_err)
            err_code = 3
            img_file_name = None
        return (img_file_name, err_code)





