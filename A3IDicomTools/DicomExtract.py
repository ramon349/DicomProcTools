
from .configs import get_params
from .extractors.PngExtractor import ExtractorRegister
import os 

def main() : 
    config = get_params() 
    os.environ['PYTHONHASHSEED'] = str(config['HashSeed'])
    extractor = ExtractorRegister.build_extractor(config)
    extractor.execute() 


if __name__=="__main__": 
    main() 
