
from Database_new import DB, Record, create_database_from_csv

def _print_record(recno: int, r: Record) -> None:
    print(f"\nRecord #{recno}")
    print(f"  NAME      : {r.name}")
    print(f"  RANK      : {r.rank}")
    print(f"  CITY      : {r.city}")
    print(f"  STATE     : {r.state}")
    print(f"  ZIP       : {r.zip}")
    print(f"  EMPLOYEES : {r.employees}")

def _prompt_record_fields(existing: Record | None = None) -> Record:
    ex = existing or Record()
    name = ex.name or input("Company NAME (primary key): ").strip()
    if not name:
        name = ex.name

    rank = input(f"RANK [{ex.rank}]: ").strip() or ex.rank
    city = input(f"CITY [{ex.city}]: ").strip() or ex.city
    state = input(f"STATE [{ex.state}]: ").strip() or ex.state
    zipc = input(f"ZIP [{ex.zip}]: ").strip() or ex.zip
    emp = input(f"EMPLOYEES [{ex.employees}]: ").strip() or ex.employees
    return Record(name=name, rank=rank, city=city, state=state, zip=zipc, employees=emp)

def main():
    db = DB()

    while True:
        print("\n==== Simple File DB Menu ====")
        print("1) Create new database")
        print("2) Open database")
        print("3) Close database")
        print("4) Display record (by NAME)")
        print("5) Update record (by NAME)")
        print("6) Print report (first 10 records)")
        print("7) Add record (append unsorted)")
        print("8) Delete record (by NAME)")
        print("9) Quit")

        choice = input("Select: ").strip()

        if choice == "1":
            prefix = input('CSV prefix (e.g., "Fortune500" for Fortune500.csv): ').strip()
            ok = create_database_from_csv(prefix)
            if ok:
                print(f"Created {prefix}.data and {prefix}.config")
            else:
                print("Create failed (CSV not found?)")

        elif choice == "2":
            if db.isOpen():
                print("A database is already open. Close it first.")
                continue
            prefix = input('Database prefix to open (e.g., "Fortune500"): ').strip()
            ok = db.open(prefix)
            print("Opened." if ok else "Open failed (missing .config/.data?)")

        elif choice == "3":
            if db.isOpen():
                db.close()
                print("Closed.")
            else:
                print("No database is open.")

        elif choice == "4":
            if not db.isOpen():
                print("Open a database first.")
                continue
            key = input("Enter company NAME to display: ").strip()
            recno, r = db._binarySearch(key)  # uses sorted portion
            if recno == -1:
                recno, r = db._linearSearch(key)
            if recno == -1 or r is None:
                print("Not found.")
            else:
                _print_record(recno, r)

        elif choice == "5":
            if not db.isOpen():
                print("Open a database first.")
                continue
            key = input("Enter company NAME to update: ").strip()
            recno, r = db._binarySearch(key)
            if recno == -1:
                recno, r = db._linearSearch(key)
            if recno == -1 or r is None:
                print("Not found.")
                continue

            _print_record(recno, r)
            print("\nEnter new values (blank keeps current). Primary key NAME cannot change.")
            updated = _prompt_record_fields(existing=r)
            updated.name = r.name  # enforce key unchanged
            ok = db.updateRecord(updated)
            print("Updated." if ok else "Update failed.")

        elif choice == "6":
            if not db.isOpen():
                print("Open a database first.")
                continue
            print("\n--- First 10 Records (sorted portion) ---")
            n = min(10, db.numSortedRecords if db.numSortedRecords > 0 else 0)
            for i in range(n):
                ok, r = db.readRecord(i)
                if ok and r:
                    print(f"{i:>3}: {r.name:<40} {r.city:<20} {r.state:<2} {r.zip:<10} EMP={r.employees}")

        elif choice == "7":
            if not db.isOpen():
                print("Open a database first.")
                continue
            print("Adding a record appends to the bottom (unsorted overflow).")
            r = _prompt_record_fields(existing=None)
            ok = db.addRecord(r)
            print("Added." if ok else "Add failed.")

        elif choice == "8":
            if not db.isOpen():
                print("Open a database first.")
                continue
            key = input("Enter company NAME to delete: ").strip()
            ok = db.deleteRecord(key)
            print("Deleted." if ok else "Delete failed (not found).")

        elif choice == "9":
            if db.isOpen():
                db.close()
            print("Goodbye.")
            break

        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
