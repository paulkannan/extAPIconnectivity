import os
from pathlib import Path

class FileUtils:
    def emptyDir(self, folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' %(file_path, e))

    def createLocalDirIfNotExists(self,path):
        Path(path).mkdir(parents=True, exist_ok= True)


    def listFiles(self,path):
        filesList=[]
        files = os.listdir(path)
        for file in files:
            filesList.append(file)
        return filesList
    
