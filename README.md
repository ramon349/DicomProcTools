# Dicom Processing Tool 
- This builds ontop of the original Niffler toool the group used 
- Code is reduced/modified so that extraction of PNGS/Tomo/CT/MRI are in a single interface with up todate dependenceis 


# Installation 
- Have an enviroment with python 3.10 installed 
- 
```bash 
    cd /path/To/Repo
    python3 -m pip install -r ./reqs.txt 
    python3 -m pip install -e .     
```
# Running Extraction 
```bash 
    python3 -m A3IDicomTools.DicomExtract --ConfigPath ./YourConfig.json
```
```json
{
 "DICOMHome": "PathToDicomFolders",
 "OutputDirectory":"DirectoryToSaveOutputs",
 "SaveBatchSize": 20,
 "SavePNGs": true,
 "PublicHeadersOnly": true,
 "SpecificHeadersOnly": false,
 "NumProcesses": 10,
 "ApplyVOILUT": true,
 "Extractor":"TOMO"
}
```

# Parameters Explained
- SaveBatchSize: Sometimes dicom tags are excessively large. As a workaround we save batches of metadata with sizes (100-200) if using privateTages
- SavePngs: Save the images. If set to False no images are saved 
- PublicHeadersOnly: If set to false we will also output Private dicom tags. 99.9% of the time private tags are not used 
- SpecificHeadersOnly: DONOT modify 
- NumProcesses: Number of processes to use for parallel extraction. Warning more does not always mean better. 
- ApplyVoiLut: Apply Windowing operation only used for mammograms and x-ray images 
- Extractor: Type of extractor to use Currently support 
    - "PNG" for Mammograms,X-ray,RetinalFundus any Modality that is stored as 2D 
    - "TOMO" Process multiframe Tomography images. Only processes images with specific SOPClassUIDs. 


