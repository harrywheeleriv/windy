

import os
import sys
import csv
import datetime
from collections import namedtuple
import numpy as np


class extract():
    def __init__(self,*args,**kwargs):
        self.path = kwargs["datalocation"]
        self.keys = [keys for keys, values in kwargs.items()]
        
        if 'interval' in self.keys:
            self.interval_grab(*args,**kwargs)
        else:
            print("Looks like you're missing some keyword arguments, try using 'interval'")
            print(keys)
        
    def directory_datetime_object(self,*args,**kwargs):
        """
            First argument inputted is a string in the format of YYDDD, returns a datetime object.
        """
        return datetime.datetime.strptime(args[0], '%y%j')
        
    def interval_grab(self,*args,**kwargs):
        """
            Identifies a list of files that should be browsed for a given interval date.
                Ex. interval format = [ 'YY:DD:HH:MM:SS', 'YY:DD:HH:MM:SS']
        """
        print kwargs["interval"]
        self.interval_object = [self.time_stamps(row) for row in kwargs["interval"]]
        
        self.datafiles = [self.path + row for index, row in enumerate(os.listdir(self.path)[:-1]) if "$" not in row and (self.interval_object[0] > self.directory_datetime_object(row[1:6]) and self.interval_object[1] < self.directory_datetime_object(os.listdir(self.path)[index+1][1:6]))]
        
        
        print self.datafiles
        
            
    def rawdata_grab(self,*args,**kwargs):
        """
            Grabs an interval of data from given input files.
            Keywords: dataFiles, interval, dataset
        """
        
        for datafile in self.datafiles:
            with open(datafile) as rawfile:
                rawfiledata = csv.reader(rawfile, delimiter = ',',)
                self.rawdata = [[element for element in row[0].split(' ') if element is not ''] for row in rawfiledata]
                for row in self.rawdata:
                    yield row
        
    def time_stamps(self,row):
        """
            Creates a datetime object for the dataset thats inputted into the function. The list must be structured as 
        """
        if ':' in row:
            timeObj = datetime.datetime.strptime(row, '%y:%j:%H:%M:%S')
        else:
            try:
                timeObj = datetime.datetime.strptime(self.time_stamp_fix(row), '%y:%j:%H:%M:%S')
            except ValueError:
                print('Failed to create datetime object for ' + self.time_stamp_fix(row))
        
        return timeObj
    
    def time_stamp_fix(self,row):
        """
            This function puts together the time info into a single string. It also fixes issues with bad times, such as 60 seconds, 60 minutes, etc.
        """
        try:
            if (int(np.floor(float(row[4]))) is 60) and ((int(row[3])) is 60):
                stamp = str(row[0]) + ':' + str(row[1]) + ':' + str(row[2]+1) + ':' + str(0*int(row[3])+1) + ':' + str(0*int(np.floor(float(row[4]))))
            elif (int(np.floor(float(row[4]))) is 60) and ((int(row[3])) is 59):
                stamp = str(row[0]) + ':' + str(row[1]) + ':' + str(int(row[2])+1) + ':' + str(0*int(row[3])+1) + ':' + str(0*int(np.floor(float(row[4]))))
            elif (int(row[3])) is 60:
                stamp = str(row[0]) + ':' + str(row[1]) + ':' + str(int(row[2])+1) + ':' + str(0*int(row[3])) + ':' + str(int(np.floor(float(row[4]))))
            elif int(np.floor(float(row[4]))) is 60:
                stamp = str(row[0]) + ':' + str(row[1]) + ':' + str(row[2]) + ':' + str(int(row[3])+1) + ':' + str(0*int(np.floor(float(row[4]))))
            else:
                stamp = str(row[0]) + ':' + str(row[1]) + ':' + str(row[2]) + ':' + str(int(row[3])) + ':' + str(int(np.floor(float(row[4]))))
        except IndexError:
            print "Failed to create string. Issue with " + str(row)
        return stamp



class ions(extract):
    """
        This class will take in an interval and return the data for that interval for the SWOOPS ion dataset.
            Data Format:
                timestamp     - datetime object containing the time stamp
                rau           - sun-spacecraft distance, AU
                hlat          - heliosperic latitude
                hlong         - heliospheric longitude 
                                (Carrington longitude of spacecraft)
                densp         - proton number density per cubic centimeter
                densa         - alpha number density per cubic centimeter
                tlarge        - proton temperature, Kelvin
                tsmall        - proton temperature, Kelvin
                vr            - radian component plasma velocity (km/sec) in heliospheric RTN coordinates in the solar system frame
                vt            - tangential component
                vn            - normal component
                qual          - condition flags or quality of data, 1 means data is "bad" 0 means data is "good"
                
    """
    def __init__(self,*args,**kwargs):
        self.extractionclass = extract(*args,**kwargs)
        self.extractionclass.interval_grab(*args,**kwargs)
        self.ionheader = ['rau','hlat','hlong','densp','densa','tlarge','tsmall','vr','vt','vn','qual']
    
    def pull(self,*args,**kwargs):
        """
            Process the raw ion data into a suitable format.
        """
        
        formated_tuple = namedtuple('ions', ['timestamp'] + self.ionheader)
        print len(['timestamp'] + self.ionheader)
        self.data = [formated_tuple(self.time_stamps(row), row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15]) for row in self.extractionclass.rawdata_grab(*args,**kwargs)]
        #self.data = []
        #for row in  self.rawdata_grab(*args,**kwargs):
        #    self.data.append(formated_tuple(self.time_stamps(row), row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
        
        
class electrons(extract):
    """
        This class will take in an interval and return the data for that interval for the SWOOPS electron dataset.
    """
    def __init__(self,*args,**kwargs):
        self.electronheader = []
        
        






def main():
    """
        Main function used for testing.
    """
    
    
    iondata = ions(datalocation = "C:\\Users\\e304987\\Documents\\research\\SWOOPS\\ions\\",interval=['00:10:00:00:00', '00:20:00:00:00'])
    
    iondata.pull()
    return iondata

if __name__ == "__main__":
    iondata = main()
    