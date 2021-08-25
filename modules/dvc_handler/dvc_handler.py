import os
import sys
import subprocess
import shutil
import pandas as pd
import json
from modules.dataset_preparation.dataset_preparation import prepare_dataset

path = os.getcwd()
args = sys.argv
p = None


def printResponse(p):
    for line in p.stdout.readlines():
        print(line)

def checkPushError(p):
    for line in p.stdout.readlines():
        if "failed to push" in str(line):
            print("Data upload failed, retrying...")
            return True
    print("No push errors encountered")
    return False

def getCommitID(p, arg):
    version = "dataset {0}".format(arg)
    for line in p.stdout.readlines():
        if version in str(line):
            return str(line)[2:10]
    return 0

def handleCheckout(commitID):
    try:
        shutil.rmtree(path + '\DataSet')
    except:
        print("dataset folder was already erased")
            
    p = subprocess.Popen('git checkout {0} DataSet.dvc'.format(commitID), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)
    p = subprocess.Popen('dvc checkout', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)

    # check the download
    successful_download = os. path. isdir(path + '\DataSet')
    while not successful_download:
        print("retrying files download...")
        p = subprocess.Popen('dvc checkout', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        printResponse(p)
        successful_download = os. path. isdir(path + '\DataSet')

def handleCheckout_Linux(commitID):
    try:
        shutil.rmtree(path + '/DataSet')
    except:
        print("dataset folder was already erased")
            
    p = subprocess.Popen('git checkout {0} DataSet.dvc'.format(commitID), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)
    p = subprocess.Popen('dvc checkout', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)

    # check the download
    successful_download = os. path. isdir(path + '/DataSet')
    while not successful_download:
        print("retrying files download...")
        p = subprocess.Popen('dvc checkout', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        printResponse(p)
        successful_download = os. path. isdir(path + '/DataSet')
        



def processMetada(Version):
    data = pd.read_csv(path + "/DataSet/dataset_labels.csv")
    TotalSeconds = data["length"].sum()
    TotalMinutes = TotalSeconds / 60
    TotalAudios = len(data["id_audio"].unique()) 

    # average amount of clips by agent
    total_clips_agent = []
    for audio in data["id_audio"].unique():
        total_clips_agent.append(len(data[(data.id_audio == audio) & (data.speaker == "agent")]))

    AVG_clips_agent = sum(total_clips_agent)/len(total_clips_agent)

    # average amount of clips by client
    total_clips_agent = []
    for audio in data["id_audio"].unique():
        total_clips_agent.append(len(data[(data.id_audio == audio) & (data.speaker == "client")]))

    AVG_clips_client = sum(total_clips_agent)/len(total_clips_agent)

    #----------------------------------------------------------------------------------------------------
    # average lenght of clips by agent
    total_clips_agent = []
    for audio in data["id_audio"].unique():
        total_clips_agent.append(sum(data[(data.id_audio == audio) & (data.speaker == "agent")]["length"].astype('int32')) / len(data[(data.id_audio == audio) & (data.speaker == "agent")]["length"]))

    AVG_lenght_agent = sum(total_clips_agent)/len(total_clips_agent)

    # average lenght of clips by client
    total_clips_agent = []
    for audio in data["id_audio"].unique():
        total_clips_agent.append(sum(data[(data.id_audio == audio) & (data.speaker == "client")]["length"].astype('int32')) / len(data[(data.id_audio == audio) & (data.speaker == "client")]["length"]))

    AVG_lenght_client = sum(total_clips_agent)/len(total_clips_agent)

    metadata = {
        "dataset" : Version,
        "TotalAudios" : TotalAudios,
        "TotalSeconds" : TotalSeconds,
        "TotalMinutes" : TotalMinutes,
        "AVG_ClipsByAgent" : AVG_clips_agent,
        "AVG_ClipsByClient" : AVG_clips_client,
        "AVG_LenghtByAgent" : AVG_lenght_agent,
        "AVG_LenghtByClient" : AVG_lenght_client
    }


    return metadata

def updateVersion(version):
    number = version.split(".")[1]
    number = int(number) + 1
    version = version[:3] + str(number)
    return version

def generateMetadata(metadata):
    with open(path + '/DataSet/metadata.json', 'w') as json_file:
        json.dump(metadata, json_file)

def readMetadata():
    f = open(path + '/DataSet/metadata.json')
    data = json.load(f)
    return data["dataset"]

def updateLabels():
    dir_audios = path + "/DataSet/audio_files/"
    destination = path + "/DataSet/"
    prepare_dataset(root_directory=dir_audios, dest_directory=destination, overwrite_df=False)

def Push(version):
    p = subprocess.Popen('dvc add DataSet', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)
    p = subprocess.Popen('git add DataSet.dvc', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)
    p = subprocess.Popen('git commit -m "dataset {0}"'.format(version), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    printResponse(p)
    print("Uploading files...")
    p = subprocess.Popen('dvc push', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    pushFailed = checkPushError(p)

    # retry push
    while pushFailed:
        p = subprocess.Popen('dvc push', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        pushFailed = checkPushError(p)
            
    print("Files uploaded")


def getVersion(args):
    try:
        # check if the version number whas explicitly passed
        if args[2]:
            version = args[2]
    except:
        print("Commiting a new dataset version")
        try:
            if os.path.exists(path + "/DataSet/metadata.json"):
                version = updateVersion(readMetadata())
        except:
            print("Couldn't find the metadata.json file, check that you're attempting to upload on top of the latest version of the dataset or manually input the version of this dataset.")
            version = "n/a"
    return version




def handle_Windows(args):
    if args[1] == "pull":
        if args[2] == "latest":
            handleCheckout("head")

        else:       
            p = subprocess.Popen('git log --oneline', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            commitID = getCommitID(p, args[2])

            if commitID == 0:
                print("Dataset version not found")
            else:
                handleCheckout(commitID)

    if args[1] == "push":
        version = getVersion(args)

        # if the version number can't be defined the process shouldn't continue
        if version != "n/a":
        

            #update dataset labels
            updateLabels()

            # update metadata
            metadata = processMetada(version)
            generateMetadata(metadata)


            # push new dataset changes
            Push(version)



def handle_Linux(args):
    if args[1] == "pull":
        if args[2] == "latest":
            handleCheckout_Linux("HEAD")
            print("Dataset ready")

        else:       
            p = subprocess.Popen('git log --oneline', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            commitID = getCommitID(p, args[2])

            if commitID == 0:
                print("Dataset version not found")
            else:
                handleCheckout_Linux(commitID)
                print("Dataset ready")


    if args[1] == "push":
        version = getVersion(args)

        # if the version number can't be defined the process shouldn't continue
        if version != "n/a":
        

            #update dataset labels
            updateLabels()

            # update metadata
            metadata = processMetada(version)
            generateMetadata(metadata)


            # push new dataset changes
            Push(version)



