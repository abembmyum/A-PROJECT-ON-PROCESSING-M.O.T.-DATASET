# This Python script contains several functions that handle downloading, 
# extracting, and cleaning up data files related to MOT (Ministry of Transport) test results. 
# The script utilizes the wget library to download files from a specified URL, 
# the zipfile library to extract the contents of a downloaded zip file, 
# and the os and shutil libraries for file and folder operations.

import wget
from zipfile import ZipFile
import os
import shutil

#This function checks whether the input year is 2020 or 2021 and calls the downloadFiles function with the appropriate year, folder path, and data file type.
# ARGUMENTS:
# dataFileType: a string that should be either "result" or "item"
# LocalFolderPath: a string representing the file path to the local folder where the extracted files will be stored
# year: an integer that should be either 2020, 2021, 2, or 1
def downloadForBothYears(dataFileType, LocalFolderPath, year):
    if(year == 2020 or year == 2):
        downloadFiles(2020, dataFileType, LocalFolderPath)
    if(year == 2021 or year == 2 or year == 1):
        downloadFiles(2021, dataFileType, LocalFolderPath)


#This function checks whether the input year is 2020 or 2021 and calls the extractFiles function with the appropriate year, folder path, and data file type.
# ARGUMENTS:
# dataFileType: a string that should be either "result" or "item"
# LocalFolderPath: a string representing the file path to the local folder where the extracted files will be stored
# year: an integer that should be either 2020, 2021, 2, or 1
def extractForBothYears(year, dataFileType, LocalFolderPath):
    if(year == 2020 or year == 2):
        extractFiles(2020, dataFileType, LocalFolderPath)
    if(year == 2021 or year == 2 or year == 1):
        extractFiles(2021, dataFileType, LocalFolderPath)


#Checks whether the input year is 2020 or 2021 and calls the cleanDownloadedFiles function with the appropriate year, folder path, and data file type.
# ARGUMENTS:
# dataFileType: a string that should be either "result" or "item"
# LocalFolderPath: a string representing the file path to the local folder where the extracted files will be stored
# year: an integer that should be either 2020, 2021, 2, or 1
def cleanForBothYears(year, dataFileType, LocalFolderPath):
    if(year == 2020 or year == 2):
        cleanDownloadedFiles(2020, dataFileType, LocalFolderPath)
    if(year == 2021 or year == 2 or year == 1):
        cleanDownloadedFiles(2021, dataFileType, LocalFolderPath)

#Constructs the URL and file name of the file to be downloaded, and uses the wget library to download the file to the specified local folder.
def downloadFiles(year, dataFileType, LocalFolderPath):
    baseUrl = "https://data.dft.gov.uk/anonymised-mot-test/test_data/"

    fileName = f"dft_test_{dataFileType}_{str(year)}.zip"

    print(f"Downloading file {baseUrl}{fileName} ...")
    print(f"to {LocalFolderPath}{fileName}")
    response = wget.download(url=f"{baseUrl}{fileName}",
                             out=f"{LocalFolderPath}{fileName}", bar=None)
    print(f"Download Completed to {response}")

#Uses the zipfile library to extract the contents of the downloaded zip file to the specified local folder. Then it will do a renaming operation on the extracted files.
def extractFiles(year, dataFileType, LocalFolderPath):

    ExtractionFolderName = f"test_{dataFileType}_{year}"
    fileName = f"dft_test_{dataFileType}_{year}.zip"

    print(f"Starting to unzip {LocalFolderPath}{fileName} ...")
    with ZipFile(f"{LocalFolderPath}{fileName}", 'r') as zObject:
        if(year == 2020):
            zObject.extractall(
                path=f"{LocalFolderPath}{ExtractionFolderName}")
        else:
            zObject.extractall(path=f"{LocalFolderPath}")
    print(f"Files extracted to {LocalFolderPath}{ExtractionFolderName}")

    # accomodating for the naming error from the source
    actualFolderName = ExtractionFolderName
    if (year == 2021 and dataFileType == "result"):
        actualFolderName = "test_result_2022"

    print(
        f"Renaming files in folder {LocalFolderPath}{actualFolderName} ...")
    for count, filename in enumerate(os.listdir(f"{LocalFolderPath}{actualFolderName}")):
        dst = f"test_{dataFileType}_{str(year)}_{str(count+1)}.csv"
        # foldername/filename, if .py file is outside folder
        src = f"{LocalFolderPath}{actualFolderName}/{filename}"
        dst = f"{LocalFolderPath}{dst}"
        print(f"File {LocalFolderPath}{dst} Done!")
        os.rename(src, dst)

#Removes the downloaded zip file and the folder containing the extracted files from the local folder.
def cleanDownloadedFiles(year, dataFileType, LocalFolderPath):

    actualFolderName = f"test_{dataFileType}_{str(year)}"
    fileName = f"dft_test_{dataFileType}_{str(year)}.zip"

    if (year == 2021 and dataFileType == "result"):
        actualFolderName = "test_result_2022"
    os.remove(f"{LocalFolderPath}{fileName}")
    shutil.rmtree(f"{LocalFolderPath}{actualFolderName}")
