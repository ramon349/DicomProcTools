from ast import Import
import pytest 
from A3IDicomTools.configs import build_args 
def test_import(): 
    try: 
        from A3IDicomTools import  DicomExtract
    except ImportError: 
        pytest.fail("Fail Main Package Import")

def test_parse_valid():
    parser = build_args()
    default_args=['--ConfigPath',"./tests/test_data/example_config.json"]
    parser.parse_args(args=default_args)
def test_parse_invalid():
    parser = build_args() 
    default_args=['--ConfigPath',"./tests/test_data/bad_config.json"]
    try: 
        parser.parse_args(args=default_args) 
    except SystemExit: 
        pass



