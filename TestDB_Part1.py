# KAI POKRANDT WORKED WITH DHAIRYA DODIA

import os
from Database_Part1 import DB

def print_record_result(recordNum, result):
    ok, payload = result
    if ok:
        print(f"\nRecord {recordNum}:")
        for k, v in payload.items():
            print(f"  {k}: {v}")
    else:
        print(f"\nRecord {recordNum}: {payload}")

def main():
    db = DB()

    while True:
        print("\n1) Create new database (first 10 records)")
        print("2) Open database")
        print("3) Close database")
        print("4) Display record")
        print("5) Quit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            prefix = input("Enter CSV prefix (Fortune500/Fortune500cut): ").strip()
            csvFile = prefix + ".csv"
            dbName = prefix

            if not os.path.exists(csvFile):
                print(f"CSV file not found: {csvFile}")
                continue

            db.createDB(csvFile, dbName, maxRecords=10)

            dataFile = dbName + ".data"
            size = os.path.getsize(dataFile)

            print("Database created:")
            print("  Config:", dbName + ".config")
            print("  Data:  ", dbName + ".data")
            print("Expected size:", 10 * db.recordSize)
            print("Actual size:  ", size)

        elif choice == "2":
            dbName = input("Enter database name to open (Fortune500/Fortune500cut): ").strip()
            if db.open(dbName):
                print("Database opened.")

        elif choice == "3":
            db.close()
            print("Database closed.")

        elif choice == "4":
            recordNum = int(input("Enter record number: ").strip())
            print_record_result(recordNum, db.readRecord(recordNum))

        elif choice == "5":
            db.close()
            print("Exiting.")
            break

        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
