import csv
import os.path

class DB:

    #default constructor
    def __init__(self):
        self.filestream = None
        self.numRecords = -1
        self.recordSize = -1

    
    # Formatting files with spaces so each field is fixed length, i.e. ID field has a fixed length of 10
    def writeRecord(self, filestream, id, experience, marriage, wage, industry):
        try:

            filestream.write("{:10.10}".format(id))
            filestream.write("{:5.5}".format(experience))
            filestream.write("{:5.5}".format(marriage))
            filestream.write("{:20.20}".format(wage))
            filestream.write("{:30.30}".format(industry))
            filestream.write("\n")
            return True
        except IOError:
            return False
        
    #create database
    def createDB(self,filename):
        #Generate file names
        csv_filename = filename + ".csv"
        data_filename = filename + ".data"

        # Read the CSV file line by line and write into data file
        with open(csv_filename, "r") as csv_file, open(data_filename, "w") as outfile:
            for line in csv_file:
                csv_reader = csv.reader([line])
                row = next(csv_reader)
                self.writeRecord(outfile, row[0], row[1], row[2], row[3], row[4])
        csv_file.close()
        outfile.close()
   	

    # #read the database
    def open(self, filename):
        data_filename = filename + ".data"
        
        if not os.path.isfile(data_filename):
            print(str(data_filename)+" not found")
        else:
            self.filestream = open(data_filename, 'r+')
            self.numRecords = 10
            self.recordSize = 71

    def readRecord(self, recordNum, id, experience, marriage, wage, industry):
        status = False

        if 0 <= recordNum < self.numRecords:
            self.filestream.seek(recordNum * self.recordSize)
            line = self.filestream.readline().rstrip('\n')
            id[0] = line[:10].strip()
            experience[0] = line[10:15].strip()
            marriage[0] = line[15:20].strip()
            wage[0] = line[20:40].strip()
            industry[0] = line[40:70].strip()
            status = True
            
        return status

    #overwrite record method
    def overwriteRecord(self, record_num, record_id, experience, married, wage, industry):
        try:
            # Calculate the byte offset of the record
            offset = record_num * self.recordSize

            # Move to the beginning of the specified record
            self.filestream.seek(offset)

            # Call writeRecord to output the passed-in parameters
            self.writeRecord(self.filestream, record_id, experience, married, wage, industry)
            return True
        except IOError:
            return False


    def binarySearch(self, id, experience, marriage, wage, industry):

        low = 0
        high = self.numRecords - 1
        self.found = False
        failure = False

        target_id = id[0]  # Do not strip leading zeros

        while not self.found and high >= low and not failure:
            self.middle = (low + high) // 2
            try:
                temp_id = [None]  # Use a list to hold the ID read from the record
                self.readRecord(self.middle, temp_id, experience, marriage, wage, industry)
            except Exception as e:
                failure = True
                break

            mid_id = temp_id[0]  # Do not strip leading zeros


            if mid_id == target_id:
                self.found = True
                # print("Record found at record number: ", self.middle)
            elif mid_id < target_id:
                low = self.middle + 1
            else:
                high = self.middle - 1

        return self.found


    #close the database
    def close(self):
        self.filestream.close()

