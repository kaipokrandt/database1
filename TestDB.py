
from Database import DB

def print_record_number(recordNum, id, experience, marriage, wage, industry):
    print(f"Record {recordNum}, ID: {id[0]:<10} experience: {experience[0]:<5} marriage: {marriage[0]:<5} wages: {wage[0]:<15} industry: {industry[0]:<30}")


def print_record_id(id, experience, marriage, wage, industry):
    print(f"ID: {id[0]:<10} experience: {experience[0]:<5} marriage: {marriage[0]:<5} wages: {wage[0]:<20} industry: {industry[0]:<30}")


def main():
    filepath = "input"

    # Create an instance of the DB class
    sample = DB()

    # Create a database 
    sample.createDB(filepath)

    # opens filepath and sets the record size
    sample.open(filepath)

    # Reading the record
    print("\n------------- Testing readRecord ------------\n")
    id = [""]
    experience = [""]
    marriage = [""]
    wage = [""]
    industry = [""]

    # Gets record 0
    recordNum = 0
    status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
    if status:
        print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)

    # Gets record 9 (last record)
    recordNum = sample.numRecords - 1
    status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
    if status:
        print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)

    # Gets middle record
    recordNum = sample.numRecords // 2
    status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
    if status:
        print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)

    # reads record 10 (out of range)
    recordNum = sample.numRecords
    status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
    if status:
        print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)

    # Gets record -1 (out of range)
    recordNum = -1
    status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
    if status:
        print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)


    print("\n------------- Testing overwriteRecord ------------\n")
    recordNum = sample.numRecords // 2
    print(f"overwrite the record number: {recordNum}")
    status_overwrite = sample.overwriteRecord(recordNum, "00005", "25", "yes", "1.01234567890123456789", "Software Engineer")

    if status_overwrite:
        status = sample.readRecord(recordNum, id, experience, marriage, wage, industry)
        if status:
            print_record_number(recordNum, id, experience, marriage, wage, industry)
    else:
        print("Failed to read record number ", recordNum)


    print("\n------------- Testing binarySearch ------------\n")
    # Find record with id 42 (should not be found)
    id = ["042"]
    status = sample.binarySearch(id, experience, marriage, wage, industry)
    if status:
        print_record_id(id, experience, marriage, wage, industry)
    else:
        print("ID ", id[0], " not found")

    # Find record with id 00000 (the first one in the file)
    id = ["00000"]
    status = sample.binarySearch(id, experience, marriage, wage, industry)
    if status:
        print_record_id(id, experience, marriage, wage, industry)
    else:
        print("ID ", id[0], " not found")

    # Find record with id 00015 (the last one in the file)
    id = ["00015"]
    status = sample.binarySearch(id, experience, marriage, wage, industry)
    if status:
        print_record_id(id, experience, marriage, wage, industry)
    else:
        print("ID ", id[0], " not found")

    # Find record with id 00005 (somewhere in the middle)
    id = ["00005"]
    status = sample.binarySearch(id, experience, marriage, wage, industry)
    if status:
        print_record_id(id, experience, marriage, wage, industry)
    else:
        print("ID ", id[0], " not found")


    # Find record with id 2025 (not in the file)
    id = ["02025"]
    status = sample.binarySearch(id, experience, marriage, wage, industry)
    if status:
        print_record_id(id, experience, marriage, wage, industry)
    else:
        print("ID ", id[0], " not found")

    # Close database
    sample.close()

if __name__ == "__main__":
    main()
