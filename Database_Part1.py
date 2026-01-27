import os

class DB:
    def __init__(self):
        self.numSortedRecords = 0
        self.recordSize = 0
        self.dataFile = None
        self.configFile = None
        self.dbName = None

        # Fixed field sizes (must not change once chosen)
        self.FIELD_SIZES = {
            "NAME": 40,
            "RANK": 6,
            "CITY": 20,
            "STATE": 5,
            "ZIP": 10,
            "EMPLOYEES": 10
        }

        # recordSize = sum(fields) + newline
        self.recordSize = sum(self.FIELD_SIZES.values()) + 1

    def isOpen(self):
        return self.dataFile is not None

    def writeRecord(self, name, rank, city, state, zip_code, employees):
        if not self.isOpen():
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

    def createDB(self, csvFilename, dbName, maxRecords=10):
        self.dbName = dbName
        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        # Remove old files if they exist
        if os.path.exists(dataFilename):
            os.remove(dataFilename)
        if os.path.exists(configFilename):
            os.remove(configFilename)

        self.dataFile = open(dataFilename, "w+")
        self.configFile = open(configFilename, "w+")

        self.numSortedRecords = 0

        with open(csvFilename, "r") as csvFile:
            header = csvFile.readline()  # skip header

            for _ in range(maxRecords):
                line = csvFile.readline()
                if not line:
                    break

                fields = line.strip().split(",")
                if len(fields) != 6:
                    continue

                self.writeRecord(
                    fields[0],
                    fields[1],
                    fields[2],
                    fields[3],
                    fields[4],
                    fields[5]
                )

                self.numSortedRecords += 1

        # Write config file
        self.configFile.write(str(self.numSortedRecords) + "\n")
        self.configFile.write(str(self.recordSize) + "\n")

        self.close()
        return True

    def open(self, dbName):
        self.dbName = dbName
        dataFilename = dbName + ".data"
        configFilename = dbName + ".config"

        if not os.path.exists(dataFilename) or not os.path.exists(configFilename):
            return False

        self.configFile = open(configFilename, "r")
        self.numSortedRecords = int(self.configFile.readline().strip())
        self.recordSize = int(self.configFile.readline().strip())
        self.configFile.close()

        self.dataFile = open(dataFilename, "r+")
        return True

    def close(self):
        if self.dataFile:
            self.dataFile.close()
        self.dataFile = None
        self.configFile = None
        self.dbName = None

    def readRecord(self, recordNum):
        if not self.isOpen():
            return None

        if recordNum < 0 or recordNum >= self.numSortedRecords:
            return None

        offset = recordNum * self.recordSize
        self.dataFile.seek(offset)
        record = self.dataFile.read(self.recordSize)

        if not record:
            return None

        pos = 0
        result = {}
        for field, size in self.FIELD_SIZES.items():
            result[field] = record[pos:pos+size].strip()
            pos += size

        return result
