import os
import hashlib
import logging
import numpy as np
import pydicom as pyd
import png
from .PngExtractor import PngExtractor,extract_dcm,ExtractorRegister
import nibabel as nib 
from enum import Enum
from .extractUtils import apply_window,extract_all_tags,apply_window_form_tags
from dicom2nifti.common import multiframe_to_block,multiframe_create_affine

class TomoEnum(Enum):
    MAMMO='1.2.840.10008.5.1.4.1.1.13.1.3'
    OCT="1.2.840.10008.5.1.4.1.1.77.1.5.4" 

def get_mammo_tomo_tags():
    tag_d = dict()
    tag_d['shared_functinal_group_sqeuence_tag'] =[0x5200,0x9229] 
    tag_d['frame_anatomy_sequence']= [0x0020,0x9071]
    tag_d['frame_laterality'] = [0x0020,0x9072]
    tag_d['frame_voi_lut_sequence'] = [0x0028,0x9132]
    tag_d['window_center']= [0x0028,0x1050]
    tag_d['window_width']= [0x0028,0x1050]
    return tag_d

@ExtractorRegister.register("TOMO")
class TomoExtractor(PngExtractor):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.set_valid_ids()
        self.mammo_tags_d = get_mammo_tomo_tags()

    def set_valid_ids(self): 
        self.valid_uids= set([e.value for e in TomoEnum])

    def image_proc(self, dcmPath, pngDestination: str = None, publicHeadersOnly: str = None, failDir: str = None, print_images=None, ApplyVOILUT=False):

        dcm = pyd.dcmread(dcmPath, force=True)
        dicom_tags = extract_all_tags(dcm,extract_nested=self.ExtractNested)
        if 'SOPClassUID' in dicom_tags and dicom_tags['SOPClassUID'] in self.valid_uids: 
            if print_images and dicom_tags is not None:
                dcm_code = dicom_tags['SOPClassUID']
                img_path, err_code = self.extract_images(
                    dcm, png_destination=pngDestination ,tags=dicom_tags,code=dcm_code
                )
            else:
                img_path = None
            dicom_tags["image_path"] =img_path 
            dicom_tags["err_code"] = err_code
            dicom_tags['file'] = dcmPath 
            return dicom_tags
        else: 
            logging.warning(f"{dcmPath} skipped not valid Tomo")
            return None 
    def extract_images(self,ds, png_destination,tags=None,code=None):
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
            img_name = hashlib.sha224(img_iden.encode("utf-8")).hexdigest() + ".nii.gz"
            store_dir = os.path.join(png_destination, folderName)
            os.makedirs(store_dir, exist_ok=True)
            pngfile = os.path.join(store_dir, img_name)
            vol = self.convert_multiframe_dicom(ds,code,tags=tags) 
            nib.save(vol,pngfile)
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
    def convert_multiframe_dicom(self,dcm,code,tags=None):
        if code == TomoEnum.MAMMO.value:
            arr = dcm.pixel_array
            arr =  multiframe_to_block(dcm)
            affine,max_slice_inc = multiframe_create_affine([dcm],arr) 
            arr = apply_window_form_tags(arr,tags)
            my_vol = nib.nifti1.Nifti1Image(arr,affine=affine)
            return my_vol
        if  code==TomoEnum.OCT.value:
            arr = dcm.pixel_array
            arr =  multiframe_to_block(dcm)
            affine,max_slice_inc = multiframe_create_affine([dcm],arr) 
            arr = apply_window_form_tags(arr,tags)
            my_vol = nib.nifti1.Nifti1Image(arr,affine=affine)
            return my_vol
        else: 
            raise Exception("I've never seen this Tomo Class")
    def _make_mammo_img(self,dcm,arr):
        arr = self.apply_window(arr,dcm)

    def apply_window(self,arr,dcm): 
        w_min,w_max = self.get_window_params(dcm) 
        arr[arr<w_min] = w_min
        arr[arr>w_max] = w_max 
        return arr 
    def get_window_params(self,dcm): 
        shared_fun_group_seq_tag = self.mammo_tags_d['shared_functional_group_sequence_tag']
        function_sequence = dcm[shared_fun_group_seq_tag][0]
        frame_voi_lut_seq_tag = self.mammo_tags_d['frame_voi_lut_sequence']
        voi_lut_sequence =  function_sequence[frame_voi_lut_seq_tag][0]
        w_c_tag = self.mammo_tags_d['window_center']
        w_w_tag = self.mammo_tags_d['window_width']
        w_c = float(voi_lut_sequence[w_c_tag].value)
        w_w = float(voi_lut_sequence[w_w_tag].value)
        w_min =  w_c - w_w/2
        w_max =  w_c + w_w/2
        return  w_min,w_max
    def verify_lat(self,dcm,arr): 
        shared_fun_group_seq_tag = self.mammo_tags_d['shared_functional_group_sequence_tag']
        function_sequence = dcm[shared_fun_group_seq_tag][0]
        frame_anatomy_seq_tag = self.mammo_tags_d['frame_anatomy_sequence']
        img_laterality = function_sequence[frame_anatomy_seq_tag][0]['frame_laterality'].value 
        est_laterality =  self.estimate_image_lat(dcm.pixel_array) 
        if img_laterality != est_laterality: 
            arr = np.flip(arr,axis=[-1])
        return arr
    def estimate_image_lat(pixel_array) -> str:
        if len(pixel_array.shape)==3: 
            left_edge = np.sum(pixel_array[:,:, 0])  # sum of left edge pixels
            right_edge = np.sum(pixel_array[:,:, -1])  # sum of right edge pixels
        else: 
            left_edge = np.sum(pixel_array[:, 0])  # sum of left edge pixels
            right_edge = np.sum(pixel_array[:, -1])  # sum of right edge pixels
        return "R" if left_edge < right_edge else "L"
