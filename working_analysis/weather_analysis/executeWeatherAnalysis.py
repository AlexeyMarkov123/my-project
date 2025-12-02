import subprocess
import yaml
import sys
import math

# yaml_file_name = sys.argv[1]
 # kommandozeile weather.... .py ....name der yaml datei -> python pipeline.py Config.yml
# yaml_file_name = "AustinAnalysis.yml" # geht
# yaml_file_name = "HamburgAnalysis_visibility.yml"
# yaml_file_name = "Oslo_vis_rain_snow.yml"
yaml_file_name = "Los_Angeles.yml"

window_size = 5

analysisConfig = yaml.safe_load(open(yaml_file_name, "r"))

for location in analysisConfig["locations"]:

    if "additional_files" in analysisConfig["locations"][location].keys():
        files = [analysisConfig["locations"][location]["data_file_name"]] + analysisConfig["locations"][location]["additional_files"]

        separateTaskAmount = math.ceil(len(files)/window_size)
        currFile = 0

        for i in range(0, separateTaskAmount):
            nextConfig = analysisConfig
            nextConfig["locations"][location]["data_file_name"] = files[currFile]
            
            lastFile = min(currFile + 1+ window_size, len(files))
            nextConfig["locations"][location]["additional_files"] = files[currFile+1: currFile + 1 + window_size]

            nextConfigFileName = yaml_file_name.split('.')[0] + "_P" + str(i) + ".yml"
            yaml.dump(nextConfig, open(nextConfigFileName, "w"), sort_keys=False)

            tee = subprocess.Popen(['/usr/bin/tee', nextConfigFileName.split('.')[0] + ".txt"], stdin= subprocess.PIPE)
            print("Start " + nextConfigFileName)
            subprocess.run(["python3", "-u", "pipeline.py", nextConfigFileName], stdout=tee.stdin, stderr=tee.stdin)
            tee.stdin.close()
            
            currFile = lastFile 
            if currFile == len(files):
            	break
    else:
        subprocess.run(["python3","-u", "pipeline.py", yaml_file_name])
