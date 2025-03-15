import pydicom as pyd 

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
def extract_all_tags(dcm,tag_prefix=""):
    dcm_tags = [e for e in dcm.dir() if e !='PixelData']
    tag_d = {} 
    for tag in dcm_tags: 
        value = getattr(dcm,tag) 
        if type(value) is pyd.sequence.Sequence: 
            for e in  value: 
                tag_d.update(extract_all_tags(e,tag_prefix=tag))
        else:
            key_name = f"{tag_prefix}_{tag}" if tag_prefix else tag
            tag_d[key_name] = value
    return tag_d
def apply_window(dcm,arr):
    tag_d = extract_all_tags(dcm)
    w_min,w_max = get_window_param(tag_d)
    if w_min and w_max: 
        arr[arr<=w_min]=w_min 
        arr[arr>=w_max] =w_max
    return arr