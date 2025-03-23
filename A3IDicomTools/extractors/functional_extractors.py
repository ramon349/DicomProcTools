
import logging
import pydicom as pyd 
from pydicom import pixels as pyd_pixels 
import numpy as np
import hashlib 
import os 
import png 
from .extractUtils import extract_all_tags
def run_hash(s): 
    return  hashlib.sha224(s.encode("utf-8")).hexdigest()
def make_hashpath(dcm,save_dir,extension): 
    pID = run_hash(dcm.PatientID )
    studyID = run_hash(dcm.StudyInstanceUID)
    seriesId = run_hash(dcm.SeriesInstanceUID)
    save_path =  os.path.join(save_dir,pID,studyID,f"{seriesId}{extension}")
    return save_path


def process_png(dcm_path,save_dir,print_images):
    dcm = pyd.dcmread(dcm_path)
    dcm_tags =extract_all_tags(dcm,extract_nested=True)
    err_code = 0 
    if print_images: 
        try: 
            png_path = make_hashpath(dcm,save_dir,extension=".png")
            image_2d_scaled,arr_shape,isRGB,bit_depth = process_image(dcm) 
            with open(png_path, "wb") as png_file:
                if isRGB:
                    image_2d_scaled = rgb_store_format(image_2d_scaled)
                    greyscale = False
                else: 
                    greyscale =True
                w = png.Writer(arr_shape[1], arr_shape[0], greyscale=greyscale,bitdepth=bit_depth)
                w.write(png_file, image_2d_scaled)
        except BaseException as error: 
            error_message = f"img:{dcm_path} produced error {error}"
            logging.error(msg=error_message)
            png_path = None 
            err_code = 1
    else: 
        png_path = None 
        err_code = 0 
    dcm_tags['png_path'] = png_path
    dcm_tags['err_code'] = err_code
    dcm_tags['file'] = dcm_path
    if 'Pixel Data' in dcm_tags: 
        del dcm_tags['Pixel Data']
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

def process_image(ds,is16Bit=True):
    try:
        isRGB = ds.PhotometricInterpretation.value == "RGB"
    except:
        isRGB = False
    image_2d = ds.pixel_array 
    image_2d = pyd_pixels.apply_rescale(image_2d,ds)
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