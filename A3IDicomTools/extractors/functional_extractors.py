import logging
import dicom2nifti
import pydicom as pyd
from pydicom import pixels as pyd_pixels
import numpy as np
import hashlib
import os
import png
from .extractUtils import extract_all_tags
from enum import Enum
from dicom2nifti.common import multiframe_create_affine
import nibabel as nib
from .extractUtils import get_window_param as get_window_fallback
from dicom2nifti.convert_dicom import dicom_array_to_nifti
import dicom2nifti 
dicom2nifti.disable_resampling()

class MammoTomoTags(Enum):
    # TODO make a custum enum class that makes it so i don't have to call .value
    SharedFunctionalGroupSequence = [0x5200, 0x9229]
    FrameAnatomySequence = [0x0020, 0x9071]
    FrameLaterality = [0x0020, 0x9072]
    FrameVoiLutSequence = [0x0028, 0x9132]
    WindowCenter = [0x0028, 0x1050]
    WindowWidth = [0x0028, 0x1051]


def run_hash(s):
    return hashlib.sha224(s.encode("utf-8")).hexdigest()


def make_hashpath(dcm, dcm_path, save_dir, extension):
    pID = run_hash(dcm.PatientID)
    studyID = run_hash(dcm.StudyInstanceUID)
    seriesId = run_hash(str(dcm_path))
    folder_name = os.path.join(save_dir, pID, studyID)
    os.makedirs(folder_name, exist_ok=True)
    save_path = os.path.join(folder_name, f"{seriesId}{extension}")
    return save_path


def process_general(dcm_path):
    dcm = pyd.dcmread(dcm_path, stop_before_pixels=True)
    dcm_tags = extract_all_tags(dcm, extract_nested=True)
    dcm_tags["file"] = dcm_path
    dcm_tags["erro_code"] = 0
    return dcm_tags


def process_png(dcm_path, save_dir, print_images,config=None):
    stop_before_pixels = False if print_images else True
    dcm = pyd.dcmread(dcm_path, stop_before_pixels=stop_before_pixels)
    dcm_tags = extract_all_tags(dcm, extract_nested=True)
    err_code = 0
    if print_images:
        try:
            png_path = make_hashpath(dcm, dcm_path, save_dir, extension=".png")
            image_2d_scaled, arr_shape, isRGB, bit_depth = process_image(dcm)
            with open(png_path, "wb") as png_file:
                if isRGB:
                    image_2d_scaled = rgb_store_format(image_2d_scaled)
                    greyscale = False
                else:
                    greyscale = True
                w = png.Writer(
                    arr_shape[1], arr_shape[0], greyscale=greyscale, bitdepth=bit_depth
                )
                w.write(png_file, image_2d_scaled)
        except BaseException as error:
            error_message = f"img:{dcm_path} produced error {error}"
            logging.error(msg=error_message)
            png_path = None
            err_code = 1
    else:
        png_path = None
        err_code = 0
    dcm_tags["image_path"] = png_path
    dcm_tags["err_code"] = err_code
    dcm_tags["file"] = dcm_path
    if "Pixel Data" in dcm_tags:
        del dcm_tags["Pixel Data"]
    return dcm_tags


def process_tomo(dcm_path, save_dir, print_images,reorient=False,config=None):
    stop_before_pixels = False if print_images else True
    dcm = pyd.dcmread(dcm_path, stop_before_pixels=stop_before_pixels)
    dcm_tags = extract_all_tags(dcm, extract_nested=True)
    nifti_path = make_hashpath(dcm, dcm_path, save_dir, extension=".nii.gz")
    err_code = 0 
    apply_voi= config['ApplyVOILUT'] 
    reorient = config['Reorient'] 
    if print_images:
        try: 
            #use the dicom2nifti processing 
            out_info = _dicomnifti_proc([dcm],output_file=nifti_path,reorient_nifti=reorient)
            # use the dicom2nifti outputs to then apply windowing 
            #the first call alread handles saving the image so no need to reload
            if apply_voi: 
                og_vol = out_info['NII'] 
                arr = og_vol.get_fdata() 
                arr = apply_window(arr,dcm,dcm_tags)  
                #save windowed image using the dicom2nifti modifications 
                new_vol = nib.nifti1.Nifti1Image(arr,affine=og_vol.affine,header=og_vol.header) 
                nib.save(new_vol,nifti_path)
        except BaseException as error:
            error_message = f"img:{dcm_path} produced error {error}"
            logging.error(msg=error_message)
            nifti_path = None
            err_code = 1
    else:
        nifti_path = None
    dcm_tags["image_path"] = nifti_path
    dcm_tags["err_code"] = err_code
    dcm_tags["file"] = dcm_path
    if "Pixel Data" in dcm_tags:
        del dcm_tags["Pixel Data"]
    return dcm_tags



def process_ctmri(dcm_path, save_dir, print_images,config=None):
    stop_before_pixels = False if print_images else True
    dcm = pyd.dcmread(dcm_path, stop_before_pixels=stop_before_pixels)
    dcm_dir = os.path.split(dcm_path)[0]
    dcm_tags = extract_all_tags(dcm, extract_nested=False)
    nifti_path = make_hashpath(dcm, dcm_path, save_dir, extension=".nii.gz")
    err_code = 0
    reorient = config['Reorient'] 
    if print_images:
        try:
            dicom2nifti.dicom_series_to_nifti(
                dcm_dir, nifti_path, reorient_nifti=reorient
            )  # TODO: Make reorientation an option
        except BaseException as error:
            error_message = f"img:{dcm_path} produced error {error}"
            logging.error(msg=error_message)
            nifti_path = None
            err_code = 1
        except AttributeError as error:
            error_message = f"img:{dcm_path} produced error {error}"
            logging.error(msg=error_message)
            nifti_path = None
            err_code = 1
    else:
        nifti_path = None
    dcm_tags["image_path"] = nifti_path
    dcm_tags["err_code"] = err_code
    dcm_tags["file"] = dcm_path
    if "Pixel Data" in dcm_tags:
        del dcm_tags["Pixel Data"]
    return dcm_tags


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


def process_image(ds, is16Bit=True):
    try:
        isRGB = ds.PhotometricInterpretation.value == "RGB"
    except:
        isRGB = False
    image_2d = ds.pixel_array
    image_2d = pyd_pixels.apply_voi_lut(image_2d, ds, prefer_lut=True)
    image_2d = image_2d.astype(float)
    shape = ds.pixel_array.shape
    if is16Bit:
        # write the PNG file as a 16-bit greyscale
        image_2d_scaled = (np.maximum(image_2d, 0) / image_2d.max()) * (2**16 - 1)
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
    return image_2d_scaled, shape, isRGB, bit_depth


def _make_mammo_img(dcm, arr, dcm_dict):
    arr = apply_window(arr, dcm, dcm_dict)
    arr = verify_lat(dcm, arr)
    return arr


def apply_window(arr, dcm, dcm_dict):
    try:
        w_min, w_max = get_window_params(dcm)
    except:
        w_min, w_max = get_window_fallback(dcm_dict)
    arr[arr < w_min] = w_min
    arr[arr > w_max] = w_max
    return arr


def get_window_params(dcm):
    function_sequence = dcm[MammoTomoTags.SharedFunctionalGroupSequence.value][0]
    voi_lut_sequence = function_sequence[MammoTomoTags.FrameVoiLutSequence.value][0]
    w_c = float(voi_lut_sequence[MammoTomoTags.WindowCenter.value].value)
    w_w = float(voi_lut_sequence[MammoTomoTags.WindowWidth.value].value)
    w_min = w_c - w_w / 2
    w_max = w_c + w_w / 2
    return w_min, w_max


def verify_lat(dcm, arr):
    try:
        function_sequence = dcm[MammoTomoTags.SharedFunctionalGroupSequence.value][0]
        img_laterality = function_sequence[MammoTomoTags.FrameAnatomySequence.value][0][
            MammoTomoTags.FrameLaterality.value
        ].value
        est_laterality = estimate_image_lat(dcm.pixel_array)
        if img_laterality != est_laterality:
            arr = np.flip(arr, axis=[-1])
    except:
        arr = arr
    return arr


def estimate_image_lat(pixel_array) -> str:
    if len(pixel_array.shape) == 3:
        left_edge = np.sum(pixel_array[:, :, 0])  # sum of left edge pixels
        right_edge = np.sum(pixel_array[:, :, -1])  # sum of right edge pixels
    else:
        left_edge = np.sum(pixel_array[:, 0])  # sum of left edge pixels
        right_edge = np.sum(pixel_array[:, -1])  # sum of right edge pixels
    return "R" if left_edge < right_edge else "L"


def _dicomnifti_proc(dicom_list,output_file,reorient_nifti=True): 
    results =  dicom_array_to_nifti(dicom_list=dicom_list,output_file=output_file,reorient_nifti=reorient_nifti) 
    return results  