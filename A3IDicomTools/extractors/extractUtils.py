from pathlib import Path
import os
from typing import List
import pydicom as pyd
import numpy as np
import hashlib
import png
from pydicom import valuerep as tagTypes
from pydicom import multival as multivalTypes
from pydicom import uid as UidTypes


def get_dcms(dicom_home: str, group_volumes=False) -> List:
    """ " get the dicom files in a directory. Idea is to get all of the files or
    group them into batches based on  file ending name. That woul be useful for dicoms
    """
    candidate_files = list(Path(dicom_home).rglob("*.dcm"))
    if group_volumes:
        candidate_files = [os.path.split(e)[0] for e in candidate_files]
    return candidate_files


def proc_img(pix_arr: np.array, dcm_path: str, dcm_tags: dict, config: dict):
    im_name = os.path.split(dcm_path)[1]
    im_name = os.path.splitext(im_name)[0]
    png_dest = config["png_destination"]
    ID1 = dcm_tags["PatientID"]
    ID2 = (
        dcm_tags["StudyInstanceUID"]
        if "StudyInstanceUID" in dcm_tags
        else "All-STUDIES"
    )
    ID3 = (
        dcm_tags["SeriesInstanceUID"]
        if "SeriesInstanceUID" in dcm_tags
        else "All-SERIES"
    )
    folderName = (
        hashlib.sha224(ID1.encode("utf-8")).hexdigest()
        + "/"
        + hashlib.sha224(ID2.encode("utf-8")).hexdigest()
        + "/"
        + hashlib.sha224(ID3.encode("utf-8")).hexdigest()
    )
    png_path = os.path.join(png_dest, folderName)
    os.makedirs(png_path, exist_ok=True)
    im_name = hashlib.sha224(im_name.encode("utf-8")).hexdigest()
    png_file = f"{png_path}/{im_name}.png"
    found_err = img_handling(pix_arr, png_file=png_file)
    return (png_file, found_err)


def write_grayscale(arr, png_path):
    shape = arr.shape
    with open(png_path, "wb") as png_file:
        w = png.Writer(shape[1], shape[0], greyscale=True, bitdepth=16)
        w.write(png_file, arr)


def img_handling(arr: np.array, png_file: str):
    img_shape = arr.shape
    # is_RGB = dcm_tags['PhotometricInterpretation']=='RGB'
    if len(img_shape) == 2:
        arr_scaled = (np.maximum(arr, 0) / arr.max()) * (2**16 - 1)
        arr_scaled = np.uint16(arr_scaled)
        write_grayscale(arr_scaled, png_file)
    else:
        pass
    fail_path = None
    found_err = None
    if found_err:
        # copy to fail paths
        pass
    else:
        return found_err


from collections.abc import Sequence


def check_multival(val):
    if isinstance(val, str):
        return float(val)
    if isinstance(val, Sequence):
        return float(val[0])
    # if you make it to the end it's likely a float or None
    return val


def get_window_param(dcm_dict):
    window_center, window_width = None, None
    for k, v in dcm_dict.items():
        if "Explanation" in k:
            continue
        if "WindowCenter" in k:
            window_center = check_multival(v)
        if "WindowWidth" in k:
            window_width = check_multival(v)
        if window_center and window_width:
            break
    w_min = window_center - window_width // 2
    w_max = window_center + window_width // 2
    return w_min, w_max


def extract_all_tags(dcm, tag_prefix="", extract_nested=True):
    dcm_tags = [e for e in dcm.dir() if e != "PixelData"]
    tag_d = {}
    for tag in dcm_tags:
        try:
            # pull a value from the dcm tags
            value = getattr(dcm, tag)
            # parse it into  a usable value
            value = proc_tag(value)
        except:
            continue
        if type(value) is pyd.sequence.Sequence:
            if extract_nested:
                for e in value:
                    tag_d.update(extract_all_tags(e, tag_prefix=tag))
        else:
            key_name = f"{tag_prefix}_{tag}" if tag_prefix else tag
            tag_d[key_name] = value
    return tag_d


def proc_tag(tag):
    match tag:
        case tagTypes.PersonName:
            return tag.decode()
        case tagTypes.IS:
            return int(tag)
        case multivalTypes.MultiValue:
            return tuple(tag)
        case tagTypes.DSfloat:
            return float(tag)
        case UidTypes.UID:
            return str(tag)
        case pyd.sequence.Sequence:
            # we do not parse the sequence tag. other code will handle it
            return tag
        case _:
            return str(tag)
