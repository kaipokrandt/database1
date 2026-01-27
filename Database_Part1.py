import os

class DB:
    def __init__(self):
        self.numSortedRecords = 0
        self.recordSize = 0
        self.dataFile = None
        self.dbName = None

        # Fixed field widths
        self.FIELD_SIZES = {
            "NAME": 40,
            "RANK": 6,
            "CITY": 20,
            "STATE": 5,
            "ZIP": 10,
            "EMPLOYEES": 10
        }

        self.recordSize = sum(self.FIELD_SIZES.values()) + 1  # + newline

    # REQUIRED PART I METHODS createDB, open, close, writeRecord, readRecord
    
    # createDB method to create database from CSV file
    def createDB(self, csvFilename, dbName, maxRecords=10):
        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        if os.path.exists(dataFilename):
            os.remove(dataFilename)
        if os.path.exists(configFilename):
            os.remove(configFilename)

        self.dataFile = open(dataFilename, "w+")
        self.dbName = dbName
        self.numSortedRecords = 0

        with open(csvFilename, "r") as csvFile:
            csvFile.readline()  # skip header
            for _ in range(maxRecords):
                line = csvFile.readline()
                if not line:
                    break

                fields = line.rstrip("\n").split(",")
                if len(fields) != 6:
                    continue

                if self.writeRecord(fields[0], fields[1], fields[2],
                                    fields[3], fields[4], fields[5]):
                    self.numSortedRecords += 1

        self.dataFile.flush()
        self.dataFile.close()
        self.dataFile = None
        self.dbName = None

        with open(configFilename, "w") as cfg:
            cfg.write(str(self.numSortedRecords) + "\n")
            cfg.write(str(self.recordSize) + "\n")

        return True

    # open method to open existing database
    def open(self, dbName):
        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        if not os.path.exists(dataFilename) or not os.path.exists(configFilename):
            return False

        with open(configFilename, "r") as cfg:
            self.numSortedRecords = int(cfg.readline().strip())
            self.recordSize = int(cfg.readline().strip())

        self.dataFile = open(dataFilename, "r+")
        self.dbName = dbName
        return True
    
    # close method to close the database
    def close(self):
        if self.dataFile:
            self.dataFile.close()

        self.dataFile = None
        self.dbName = None
        self.numSortedRecords = 0

    # writeRecord method to write a record to the data file
    def writeRecord(self, name, rank, city, state, zip_code, employees):
        if self.dataFile is None:
            return False

        record = (
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

        self.dataFile.seek(recordNum * self.recordSize)
        record = self.dataFile.read(self.recordSize)

        if not record:
            return (False, f"ERROR: could not read record {recordNum}")

        pos = 0
        out = {}
        for field, size in self.FIELD_SIZES.items():
            out[field] = record[pos:pos + size].strip()
            pos += size

        return (True, out)
