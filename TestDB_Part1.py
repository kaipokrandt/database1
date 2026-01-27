import os
from Database_Part1 import DB

def main():
    db = DB()

    while True:
        print("\n1) Create new database (first 10 records)")
        print("2) Open database")
        print("3) Close database")
        print("4) Display record")
        print("5) Update record")
        print("6) Print report")
        print("7) Add a record (pairs method)")
        print("8) Delete a record")
        print("9) Quit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            db.createDB("Fortune500.csv", "Fortune500", 10)
            size = os.path.getsize("Fortune500.data")
            print("Database created.")
            print("Expected size:", 10 * db.recordSize)
            print("Actual size:  ", size)

        elif choice == "2":
            if db.open("Fortune500"):
                print("Database opened.")
            else:
                print("Failed to open database.")

        elif choice == "3":
            db.close()
            print("Database closed.")

        elif choice == "4":
            recNum = int(input("Enter record number (0-9): "))
            rec = db.readRecord(recNum)
            if rec:
                for k, v in rec.items():
                    print(f"{k}: {v}")
            else:
                print("Record not found.")

        elif choice == "5":
            print("Update function is currently disabled.")
            break
        elif choice == "6":
            print("Print function is currently disabled.")
            break
        elif choice == "7":
            print("Add function is currently disabled.")
            break
        elif choice == "8":
            print("Delete function is currently disabled.")
            break
        elif choice == "9":
            db.close()
            print("Exiting.")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
