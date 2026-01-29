# KAI POKRANDT WORKED WITH DHAIRYA DODIA

import os

class DB:
    def __init__(self):
        self.numSortedRecords = 0
        self.recordSize = 0
        self.dataFile = None
        self.dbName = None
        self.dbOpen = False

        # Fixed field widths
        self.FIELD_SIZES = {
            "KEY": 10,
            "NAME": 40,
            "RANK": 6,
            "CITY": 20,
            "STATE": 5,
            "ZIP": 10,
            "EMPLOYEES": 10
        }

        self.totalFieldSize = sum(self.FIELD_SIZES.values()) # +  1  if on windows

    # REQUIRED PART I METHODS createDB, open, close, writeRecord, readRecord

    # Gets the total amount of lines in the provided CSV
    def getCsvTotalRecords(self, filepath):
        # Open the CSV file and get the total amount of lines
        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            lines = (1 for _ in f if _.strip())
            total = sum(lines)

        f.close()
        return total
    
    # createDB method to create database from CSV file
    def createDB(self, csvFilename, dbName):
        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        if os.path.exists(dataFilename):
            os.remove(dataFilename)
        if os.path.exists(configFilename):
            os.remove(configFilename)

        self.dataFile = open(dataFilename, "w+")
        self.dbName = dbName
        maxRecords = self.getCsvTotalRecords(csvFilename)
        print(maxRecords)

        with open(csvFilename, "r") as csvFile:

            
            first_line = csvFile.readline().rstrip("\n") # Get the first line

            fields = first_line.split(",") # Use the comma separator
            if len(fields) == 6:
                # Provide the fields to write the record to the .data file
                if self.writeRecord(str(self.numSortedRecords), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]): 
                    self.numSortedRecords += 1

            # Now read the rest up to maxRecords total
            while self.numSortedRecords < maxRecords:
                line = csvFile.readline()
                if not line:
                    break
                fields = line.rstrip("\n").split(",") # Use the comma separator
                if len(fields) != 6:
                    continue
                if self.writeRecord(str(self.numSortedRecords), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]):
                    self.numSortedRecords += 1


        
        # Close the .data file
        self.dataFile.flush()
        self.dataFile.close()
        self.recordSize = os.path.getsize(dataFilename)
        self.dataFile = None
        self.dbName = None
        
        
        # Write to the .config file
        with open(configFilename, "w") as cfg:
            cfg.write(str(self.numSortedRecords) + "\n")
            cfg.write(str(self.recordSize) + "\n")
        
        cfg.close()
        
        # Reset values
        self.numSortedRecords = 0
        self.recordSize = 0

        return True

    # open method to open existing database
    def open(self, dbName):
        
        if(self.dbOpen):
            print("A database is already open, close the current database before opening a new one")
            return
        else:
            self.dbOpen = True

        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        
        if not os.path.exists(dataFilename) or not os.path.exists(configFilename):
            print("ERROR: {dataFileName} Database does not exist")
            return False

        # Open the .config file in read mode
        with open(configFilename, "r") as cfg:
            self.numSortedRecords = int(cfg.readline().strip())
            self.recordSize = int(cfg.readline().strip())

        self.dataFile = open(dataFilename, "r+")
        self.dbName = dbName
        return True
    
    # close method to close the database
    def close(self):
        if(not self.dbOpen):
            print("No Database is currently opened.")
            return
        
        if self.dataFile:
            self.dataFile.close()

        self.dataFile = None
        self.dbName = None
        self.numSortedRecords = 0
        self.dbOpen = False

    # writeRecord method to write a record to the data file
    def writeRecord(self, key, name, rank, city, state, zip_code, employees):
        if self.dataFile is None:
            return False


        record = (
            key.ljust(self.FIELD_SIZES["KEY"]) + # Primary KEY
            name.ljust(self.FIELD_SIZES["NAME"]) +
            rank.ljust(self.FIELD_SIZES["RANK"]) +
            city.ljust(self.FIELD_SIZES["CITY"]) +
            state.ljust(self.FIELD_SIZES["STATE"]) +
            zip_code.ljust(self.FIELD_SIZES["ZIP"]) +
            employees.ljust(self.FIELD_SIZES["EMPLOYEES"]) +
            "\n"
        )


        self.dataFile.write(record)

        return True

    # readRecord method to read a record by record number
    def readRecord(self, recordNum):
        if self.dataFile is None:
            return (False, "ERROR: database not open")

        if recordNum < 0 or recordNum >= self.numSortedRecords:
            return (False, f"ERROR: invalid record number {recordNum}")

        self.dataFile.seek(recordNum * self.totalFieldSize) # Read every field size byte
        record = self.dataFile.read(self.totalFieldSize)

        if not record:
            return (False, f"ERROR: could not read record {recordNum}")

        pos = 0
        out = {}
        for field, size in self.FIELD_SIZES.items():
            out[field] = record[pos:pos + size].strip()
            pos += size #Seek ahead every item size

        return (True, out) # Return a tuple
    
