import logging
import pydicom as pyd

# pydicom imports needed to handle data errors
from pydicom import config
from pydicom import values


class ExtractorRegister:
    __data = {}

    @classmethod
    def register(cls, cls_name=None):
        def decorator(cls_obj):
            cls.__data[cls_name] = cls_obj
            return cls_obj

        return decorator

    @classmethod
    def get_extractor(cls, key):
        return cls.__data[key]

    @classmethod
    def get_extractors(cls):
        return list(cls.__data.keys())

    @classmethod
    def build_extractor(cls, conf):
        key = conf["Extractor"]
        extractor = cls.get_extractor(key)
        return extractor(conf)


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
