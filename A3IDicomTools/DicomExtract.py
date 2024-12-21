
from .configs import get_params
from .extractors.PngExtractor import ExtractorRegister

def main() : 
    config = get_params() 
    extractor = ExtractorRegister.build_extractor(config)
    extractor.execute() 


if __name__=="__main__": 
    main() 
