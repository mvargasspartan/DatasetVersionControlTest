import os
import sys
import platform
from modules.dataset_preparation.dataset_preparation import prepare_dataset
from modules.dvc_handler.dvc_handler import handle_Windows, handle_Linux



yourOS = platform.system()

if yourOS == "Windows":
    # Windows stuff
    handle_Windows(sys.argv)
    pass
elif yourOS == "Linux":
    # Linux stuff
    handle_Linux(sys.argv)




