from typing import List
from pathlib import Path
import os 
from typing import List,Tuple
import pydicom as pyd 
import numpy as np 
import hashlib
import png 
def get_dcms(dicom_home:str,group_volumes=False) -> List: 
    """" get the dicom files in a directory. Idea is to get all of the files or 
    group them into batches based on  file ending name. That woul be useful for dicoms 
    """
    candidate_files =list(Path(dicom_home).rglob('*.dcm') )
    if group_volumes: 
        candidate_files = [os.path.split(e)[0] for e in candidate_files]
    return candidate_files


def get_dcm_tags(dcm:pyd.Dataset,outlist=None,key=""):
    if len(key) >0:
        key = key + "_"
    if outlist is None:
        outlist=[]
    for tag in dcm.dir():
        if tag != 'PixelData':
            value = getattr(dcm,tag)
        if type(value) is pyd.sequence.Sequence:
            # we are skippign these for now 
            pass 
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

def proc_img(pix_arr:np.array,dcm_path:str,dcm_tags:dict,config:dict):
    im_name = os.path.split(dcm_path)[1] 
    im_name = os.path.splitext(im_name)[0]
    png_dest = config['png_destination']
    ID1 = dcm_tags['PatientID'] 
    ID2 = dcm_tags['StudyInstanceUID']  if 'StudyInstanceUID' in dcm_tags else 'All-STUDIES' 
    ID3 = dcm_tags['SeriesInstanceUID']  if 'SeriesInstanceUID' in dcm_tags else 'All-SERIES' 
    folderName = hashlib.sha224(ID1.encode('utf-8')).hexdigest() + "/" + \
                         hashlib.sha224(ID2.encode('utf-8')).hexdigest() + "/" + \
                         hashlib.sha224(ID3.encode('utf-8')).hexdigest()
    png_path = os.path.join(png_dest,folderName)
    os.makedirs(png_path,exist_ok=True)
    im_name =  hashlib.sha224(im_name.encode('utf-8')).hexdigest()
    png_file = f"{png_path}/{im_name}.png"
    found_err = img_handling(pix_arr,png_file=png_file)
    return (png_file,found_err)

def write_grayscale(arr,png_path): 
    shape = arr.shape
    with open(png_path,'wb') as png_file: 
        w = png.Writer(shape[1],shape[0],greyscale=True,bitdepth=16)
        w.write(png_file,arr)
    
def img_handling(arr:np.array,png_file:str):
    img_shape  = arr.shape 
    #is_RGB = dcm_tags['PhotometricInterpretation']=='RGB'
    if len(img_shape)==2:
        arr_scaled = (np.maximum(arr,0)/arr.max()) * (2**16 -1)
        arr_scaled = np.uint16(arr_scaled)
        write_grayscale(arr_scaled,png_file) 
    else: 
        pass 
    fail_path = None
    found_err = None 
    if found_err: 
        #copy to fail paths 
        pass  
    else: 
        return found_err

def get_window_param(dcm_dict): 
    window_center,window_width=None,None
    for k,v in dcm_dict.items():
        if 'WindowCenter' in k:
            window_center = float(v)  
        if 'WindowWidth' in k: 
            window_width  = float(v)
        if window_center and window_width: 
            break 
    w_min = window_center - window_width//2
    w_max = window_center + window_width//2 
    return   w_min,w_max
def extract_all_tags(dcm,tag_prefix="",extract_nested=True):
    dcm_tags = [e for e in dcm.dir() if e !='PixelData']
    tag_d = {} 
    for tag in dcm_tags: 
        value = getattr(dcm,tag) 
        if type(value) is pyd.sequence.Sequence: 
            if extract_nested:
                for e in  value: 
                    tag_d.update(extract_all_tags(e,tag_prefix=tag))
        else:
            key_name = f"{tag_prefix}_{tag}" if tag_prefix else tag
            tag_d[key_name] = value
    return tag_d