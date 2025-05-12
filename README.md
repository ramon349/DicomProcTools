# Dicom Processing Tool 
- This builds ontop of the original Niffler toool the group used 
- Code is reduced/modified so that extraction of PNGS/Tomo/CT/MRI are in a single interface with up todate dependenceis 


# Installation 
- Have an enviroment with python 3.11 installed 
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
"DICOMHome": "DummyPath",
"OutputDirectory": "DummyStuff",
"SaveBatchSize": 500,
"PublicHeadersOnly": true,
"SpecificHeadersOnly":false,
"SaveImages": true,
"ApplyVOILUT":true,
"NumProcesses": 12 ,
"Extractor":"General"
}
```

# Parameters Explained
- SaveBatchSize: Sometimes dicom tags are excessively large. As a workaround we save batches of metadata with batchSizes (100-200) 
- SaveImages: Save the images. If set to False no images are saved 
- PublicHeadersOnly: If set to false we will also output Private dicom tags. 99.9% of the time private tags are not used 
- SpecificHeadersOnly: DONOT modify 
- NumProcesses: Number of processes to use for parallel extraction. Warning more does not always mean better. 
- ApplyVoiLut: Apply Windowing operation only used for mammograms and x-ray images 
- Extractor: Type of extractor to use Currently support 
    - "General" Will run extraction of all the images. Will apply unique processing to MRI/CT,X-ray,Tomogram 
    - Other modalities will be ignored for now 
- ApplyParentFilter: Should only be set to true if running extraction  CT/MR images. Since it will filter studies in the following way 
/patient/study/series/f1.dcm 
/patient/study/series/f2.dcm 
if set to true we will only read 1 dcm file. making the metadatafile like this 
/patient/study/series/f1.dcm  
- Reorient:  When making niftis we usually reorient to RAS orientation. This is set to True as the default to maintain consistency with older projects 


# Differences from Niffler extraction code 
- When extracting NIFTI's the  column 'file' will be the path to the dicom file used to extract metadata. Old extractions would have the directory to the series 
- image_path: the hashing for the nifti file has been updated to be the path of the dicom file. This avoids issues with overlapping  ids in some rare cases 
- There is no mapping file. the metadata file contains an 'image_path' column.  
- Failed extractions WILL HAVE NaN 'image_path' columns. So you must drop them. 
- Based on the dataset available to you. there will be pngs and niftis in the file. You must filter accordingly.

