import os
import hashlib
import logging
import numpy as np
import pydicom as pyd
import png
from .PngExtractor import PngExtractor,extract_dcm,ExtractorRegister
import nibabel as nib 
from enum import Enum
class TomoEnum(Enum):
    MAMMO='1.2.840.10008.5.1.4.1.1.13.1.3'
    OCT="1.2.840.10008.5.1.4.1.1.77.1.5.4" 

@ExtractorRegister.register("TOMO")
class TomoExtractor(PngExtractor):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.set_valid_ids()
    def set_valid_ids(self): 
        self.valid_uids= set([e.value for e in TomoEnum])

    def image_proc(self, dcmPath, pngDestination: str = None, publicHeadersOnly: str = None, failDir: str = None, print_images=None, ApplyVOILUT=False):

        dcm = pyd.dcmread(dcmPath, force=True)
        dicom_tags = extract_dcm(dcm, dcm_path=dcmPath, PublicHeadersOnly=publicHeadersOnly)
        if 'SOPClassUID' in dicom_tags and dicom_tags['SOPClassUID'] in self.valid_uids: 
            if print_images and dicom_tags is not None:
                dcm_code = dicom_tags['SOPClassUID']
                png_path, err_code = self.extract_images(
                    dcm, png_destination=pngDestination ,ApplyVOILUT=ApplyVOILUT,code=dcm_code
                )
            else:
                png_path = None
            dicom_tags["png_path"] = png_path
            dicom_tags["err_code"] = err_code
            return dicom_tags
        else: 
            logging.warning(f"{dcmPath} skipped not valid Tomo")
            return None 
    def extract_images(self,ds, png_destination,ApplyVOILUT=False,code=None):
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

            # check for existence of the folder tree patient/study/series. Create if it does not exist.

            store_dir = os.path.join(png_destination, folderName)
            os.makedirs(store_dir, exist_ok=True)
            pngfile = os.path.join(store_dir, img_name)
            vol = self.convert_multiframe_dicom(ds,code) 
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
    def convert_multiframe_dicom(self,dcm,code):
        if code==TomoEnum.MAMMO.value:
            return self._convert_mammo_multframe(dcm) 
        if code==TomoEnum.OCT.value:
            return self._convert_mammo_multframe
        else: 
            raise Exception("I've never seen this Tomo Class")

    def _convert_mammo_multframe(self,dcm):
        arr = dcm.pixel_array
        frame_info = dcm[0x5200,0x9230][0]
        pixel_info = frame_info[0x0028,0x9110][0]
        slice_thickness= pixel_info[0x0018,0x0050].value 
        pixel_spacing = pixel_info[0x0028,0x0030].value
        z_rez = slice_thickness#note this is microns 
        x_res = pixel_spacing[0]
        y_res = pixel_spacing[1]
        aff = np.zeros((3,3))
        aff[0,0]= x_res 
        aff[1,1]=y_res 
        aff[2,2]=z_rez
        other=  np.array([0,0,1])
        my_aff = nib.affines.from_matvec(aff,vector=other)
        my_vol = nib.nifti1.Nifti1Image(arr,affine=my_aff)
        return my_vol
    def _convert_oct_multiframe(dcm): 
        arr = dcm.pixel_array
        z_rez = dcm[0x0022,0x0035].value/1000#note this is microns 
        x_res = dcm[0x0022,0x0037].value/1000
        y_res = dcm[0x0022,0x0037].value/1000
        aff = np.zeros((3,3))
        aff[0,0]= x_res 
        aff[1,1]=y_res 
        aff[2,2]=z_rez
        other=  np.array([0,0,1])
        my_aff = nib.affines.from_matvec(aff,vector=other)
        my_vol = nib.nifti1.Nifti1Image(arr,affine=my_aff)
        return my_vol
