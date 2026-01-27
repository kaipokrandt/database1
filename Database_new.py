
import csv
import os
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any

@dataclass
class Record:
    name: str = ""
    rank: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    employees: str = ""

class DB:
    """
    Simple fixed-length record database backed by two files:
      <prefix>.config  (text)
      <prefix>.data    (fixed-length records)
    Primary key: company name (string), assumed sorted for the first numSortedRecords records.
    """

    # -----------------------------
    # default constructor
    # -----------------------------
    def __init__(self):
        # file handle (opened in binary so seek offsets are byte-accurate)
        self.dataFilestream = None

        # required instance variables
        self.numSortedRecords = -1
        self.numUnsortedRecords = -1
        self.numRecords = -1
        self.recordSize = -1
        self.numOverflow = 0

        # field widths (fixed-length formatting)
        self._widths = {
            "name": 40,
            "rank": 4,
            "city": 20,
            "state": 2,
            "zip": 10,
            "employees": 10,
        }

        self._prefix: Optional[str] = None

    # -----------------------------
    # helpers for config
    # -----------------------------
    def _config_filename(self, prefix: str) -> str:
        return f"{prefix}.config"

    def _data_filename(self, prefix: str) -> str:
        return f"{prefix}.data"

    def _write_config(self, prefix: str) -> None:
        cfg = self._config_filename(prefix)
        with open(cfg, "w", encoding="utf-8", newline="\n") as f:
            f.write(f"numSortedRecords={self.numSortedRecords}\n")
            f.write(f"numUnsortedRecords={self.numUnsortedRecords}\n")
            f.write(f"recordSize={self.recordSize}\n")
            f.write(
                "widths="
                + ",".join(
                    str(self._widths[k])
                    for k in ("name", "rank", "city", "state", "zip", "employees")
                )
                + "\n"
            )

    def _read_config(self, prefix: str) -> bool:
        cfg = self._config_filename(prefix)
        if not os.path.isfile(cfg):
            return False

        vals: Dict[str, str] = {}
        with open(cfg, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                vals[k.strip()] = v.strip()

        try:
            self.numSortedRecords = int(vals.get("numSortedRecords", "-1"))
            self.numUnsortedRecords = int(vals.get("numUnsortedRecords", "0"))
            self.recordSize = int(vals.get("recordSize", "-1"))

            widths = vals.get("widths", "")
            if widths:
                parts = [int(x) for x in widths.split(",")]
                if len(parts) == 6:
                    self._widths["name"], self._widths["rank"], self._widths["city"], self._widths["state"], self._widths["zip"], self._widths["employees"] = parts

            self.numRecords = self.numSortedRecords + self.numUnsortedRecords
            self.numOverflow = self.numUnsortedRecords
            return True
        except Exception:
            return False

    # -----------------------------
    # fixed-length record pack/unpack
    # -----------------------------
    def _pack_record(self, r: Record) -> bytes:
        # NOTE: truncation is intentional to maintain fixed record size
        s = (
            f"{r.name:<{self._widths['name']}.{self._widths['name']}}"
            f"{r.rank:<{self._widths['rank']}.{self._widths['rank']}}"
            f"{r.city:<{self._widths['city']}.{self._widths['city']}}"
            f"{r.state:<{self._widths['state']}.{self._widths['state']}}"
            f"{r.zip:<{self._widths['zip']}.{self._widths['zip']}}"
            f"{r.employees:<{self._widths['employees']}.{self._widths['employees']}}"
            "\n"
        )
        b = s.encode("utf-8", errors="replace")
        # ensure record is exactly recordSize bytes
        if self.recordSize > 0:
            if len(b) < self.recordSize:
                b = b[:-1] + (b" " * (self.recordSize - len(b))) + b"\n"
            elif len(b) > self.recordSize:
                b = b[: self.recordSize - 1] + b"\n"
        return b

    def _unpack_record(self, b: bytes) -> Record:
        s = b.decode("utf-8", errors="replace")
        # strip only final newline; keep padding for slicing
        if s.endswith("\n"):
            s = s[:-1]
        i = 0
        w = self._widths
        name = s[i : i + w["name"]].rstrip(); i += w["name"]
        rank = s[i : i + w["rank"]].rstrip(); i += w["rank"]
        city = s[i : i + w["city"]].rstrip(); i += w["city"]
        state = s[i : i + w["state"]].rstrip(); i += w["state"]
        zipc = s[i : i + w["zip"]].rstrip(); i += w["zip"]
        emp = s[i : i + w["employees"]].rstrip()
        return Record(name, rank, city, state, zipc, emp)

    def _valid_record_num(self, recordNum: int) -> bool:
        return self.isOpen() and 0 <= recordNum < self.numRecords

    # -----------------------------
    # open/close/isOpen
    # -----------------------------
    def isOpen(self) -> bool:
        return self.dataFilestream is not None and not self.dataFilestream.closed

    def open(self, prefix: str) -> bool:
        if self.isOpen():
            return False

        if not self._read_config(prefix):
            return False

        data_path = self._data_filename(prefix)
        if not os.path.isfile(data_path):
            return False

        # r+b so we can read and overwrite
        self.dataFilestream = open(data_path, "r+b")
        self._prefix = prefix
        return True

    def close(self) -> None:
        # write config (only if we have a prefix)
        if self._prefix is not None and self.numSortedRecords >= 0 and self.recordSize > 0:
            try:
                self._write_config(self._prefix)
            except Exception:
                pass

        # close file
        if self.dataFilestream is not None:
            try:
                self.dataFilestream.close()
            except Exception:
                pass

        # reset instance vars
        self.dataFilestream = None
        self.numSortedRecords = -1
        self.numUnsortedRecords = -1
        self.numRecords = -1
        self.recordSize = -1
        self.numOverflow = 0
        self._prefix = None

    # -----------------------------
    # public helper: readRecord
    # -----------------------------
    def readRecord(self, recordNum: int,
                   name=None, rank=None, city=None, state=None, zipc=None, employees=None,
                   record: Optional[Record] = None) -> Tuple[bool, Optional[Record]]:
        """
        Reads record recordNum. You can either:
          - pass record=Record() and receive a Record back, OR
          - pass list-wrappers for fields (e.g., name=[""]) like typical assignments.
        Returns: (status, Record or None)
        """
        if not self._valid_record_num(recordNum):
            return (False, None)

        try:
            self.dataFilestream.seek(recordNum * self.recordSize)
            b = self.dataFilestream.read(self.recordSize)
            if len(b) != self.recordSize:
                return (False, None)
            r = self._unpack_record(b)

            # fill wrappers if provided
            if isinstance(name, list): name[0] = r.name
            if isinstance(rank, list): rank[0] = r.rank
            if isinstance(city, list): city[0] = r.city
            if isinstance(state, list): state[0] = r.state
            if isinstance(zipc, list): zipc[0] = r.zip
            if isinstance(employees, list): employees[0] = r.employees

            if isinstance(record, Record):
                record.name, record.rank, record.city, record.state, record.zip, record.employees = (
                    r.name, r.rank, r.city, r.state, r.zip, r.employees
                )
            return (True, r)
        except Exception:
            return (False, None)

    # -----------------------------
    # public helper: writeRecord (writes at current file position)
    # -----------------------------
    def writeRecord(self, r: Record) -> bool:
        if not self.isOpen():
            return False
        try:
            self.dataFilestream.write(self._pack_record(r))
            return True
        except Exception:
            return False

    # overwrite at record number (handy internal helper)
    def _overwrite_at(self, recordNum: int, r: Record) -> bool:
        if not self._valid_record_num(recordNum):
            return False
        try:
            self.dataFilestream.seek(recordNum * self.recordSize)
            self.dataFilestream.write(self._pack_record(r))
            self.dataFilestream.flush()
            return True
        except Exception:
            return False

    # -----------------------------
    # private helper: binarySearch (sorted portion only)
    # -----------------------------
    def _binarySearch(self, target_name: str) -> Tuple[int, Optional[Record]]:
        if not self.isOpen() or self.numSortedRecords <= 0:
            return (-1, None)

        low, high = 0, self.numSortedRecords - 1
        target = (target_name or "").strip().upper().upper()

        while low <= high:
            mid = (low + high) // 2
            ok, r = self.readRecord(mid)
            if not ok or r is None:
                return (-1, None)

            mid_name = r.name.strip().upper()

            if mid_name == target:
                return (mid, r)
            elif mid_name < target:
                low = mid + 1
            else:
                high = mid - 1

        return (-1, None)

    # -----------------------------
    # private helper: linearSearch (unsorted overflow only)  (BONUS)
    # -----------------------------
    def _linearSearch(self, target_name: str) -> Tuple[int, Optional[Record]]:
        if not self.isOpen() or self.numUnsortedRecords <= 0:
            return (-1, None)

        target = (target_name or "").strip().upper()
        start = self.numSortedRecords
        end = self.numSortedRecords + self.numUnsortedRecords - 1

        for recno in range(start, end + 1):
            ok, r = self.readRecord(recno)
            if not ok or r is None:
                break
            if r.name.strip().upper() == target:
                return (recno, r)

        return (-1, None)

    # -----------------------------
    # public helper: findRecord
    # -----------------------------
    def findRecord(self,
                   name, rank=None, city=None, state=None, zipc=None, employees=None,
                   record: Optional[Record] = None) -> int:
        """
        Searches by primary key (name).
        Fills wrappers / record if found, else resets them to "".
        Returns recordNum or -1.
        """
        if not self.isOpen():
            return -1

        # allow passing name as list wrapper or string
        target = name[0] if isinstance(name, list) else str(name)
        recno, r = self._binarySearch(target)
        if recno == -1:
            recno, r = self._linearSearch(target)

        if recno != -1 and r is not None:
            if isinstance(name, list): name[0] = r.name
            if isinstance(rank, list) and rank is not None: rank[0] = r.rank
            if isinstance(city, list) and city is not None: city[0] = r.city
            if isinstance(state, list) and state is not None: state[0] = r.state
            if isinstance(zipc, list) and zipc is not None: zipc[0] = r.zip
            if isinstance(employees, list) and employees is not None: employees[0] = r.employees
            if isinstance(record, Record):
                record.name, record.rank, record.city, record.state, record.zip, record.employees = (
                    r.name, r.rank, r.city, r.state, r.zip, r.employees
                )
            return recno

        # not found: reset
        if isinstance(name, list): name[0] = target
        if isinstance(rank, list) and rank is not None: rank[0] = ""
        if isinstance(city, list) and city is not None: city[0] = ""
        if isinstance(state, list) and state is not None: state[0] = ""
        if isinstance(zipc, list) and zipc is not None: zipc[0] = ""
        if isinstance(employees, list) and employees is not None: employees[0] = ""
        if isinstance(record, Record):
            record.name, record.rank, record.city, record.state, record.zip, record.employees = (target, "", "", "", "", "")
        return -1

    # -----------------------------
    # public: updateRecord
    # -----------------------------
    def updateRecord(self, r: Record) -> bool:
        if not self.isOpen():
            return False

        recno, existing = self._binarySearch(r.name)
        if recno == -1:
            recno, existing = self._linearSearch(r.name)
        if recno == -1 or existing is None:
            return False

        # enforce stored key spelling/case to keep sorted section consistent
        r.name = existing.name
        return self._overwrite_at(recno, r)

    # -----------------------------
    # public: deleteRecord
    # -----------------------------
    def deleteRecord(self, name: str) -> bool:
        if not self.isOpen():
            return False

        recno, r = self._binarySearch(name)
        if recno == -1:
            recno, r = self._linearSearch(name)
        if recno == -1 or r is None:
            return False

        # keep key so binarySearch keeps working; blank the rest
        deleted = Record(
            name=r.name,
            rank="",
            city="",
            state="",
            zip="",
            employees="",
        )
        return self._overwrite_at(recno, deleted)

    # -----------------------------
    # public: addRecord (append unsorted)
    # -----------------------------
    def addRecord(self, r: Record) -> bool:
        if not self.isOpen():
            return False

        # append at end of file
        try:
            self.dataFilestream.seek(0, os.SEEK_END)
            self.dataFilestream.write(self._pack_record(r))
            self.dataFilestream.flush()

            self.numUnsortedRecords += 1
            self.numOverflow = self.numUnsortedRecords
            self.numRecords = self.numSortedRecords + self.numUnsortedRecords

            # persist config immediately to be safe
            if self._prefix is not None:
                self._write_config(self._prefix)

            return True
        except Exception:
            return False


# -----------------------------
# Create new database (menu option 1)
# -----------------------------
def create_database_from_csv(prefix: str,
                             csv_filename: Optional[str] = None,
                             widths: Optional[Dict[str, int]] = None) -> bool:
    """
    Reads <prefix>.csv (or csv_filename) and writes:
      <prefix>.data  fixed-length records
      <prefix>.config
    Assumes CSV rows are already sorted by company name.
    """
    csv_path = csv_filename or f"{prefix}.csv"
    if not os.path.isfile(csv_path):
        return False

    # Use default widths unless provided
    w = widths or {
        "name": 40,
        "rank": 4,
        "city": 20,
        "state": 2,
        "zip": 10,
        "employees": 10,
    }
    record_size = w["name"] + w["rank"] + w["city"] + w["state"] + w["zip"] + w["employees"] + 1

    data_path = f"{prefix}.data"
    cfg_path = f"{prefix}.config"

    # overwrite if exists
    for p in (data_path, cfg_path):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    num_records = 0
    with open(csv_path, newline="", encoding="utf-8") as inf, open(data_path, "wb") as outf:
        reader = csv.reader(inf)
        for row in reader:
            if len(row) < 6:
                continue
            r = Record(
                name=row[0].strip(),
                rank=row[1].strip(),
                city=row[2].strip(),
                state=row[3].strip(),
                zip=row[4].strip(),
                employees=row[5].strip(),
            )
            s = (
                f"{r.name:<{w['name']}.{w['name']}}"
                f"{r.rank:<{w['rank']}.{w['rank']}}"
                f"{r.city:<{w['city']}.{w['city']}}"
                f"{r.state:<{w['state']}.{w['state']}}"
                f"{r.zip:<{w['zip']}.{w['zip']}}"
                f"{r.employees:<{w['employees']}.{w['employees']}}"
                "\n"
            )
            b = s.encode("utf-8", errors="replace")
            # enforce size
            if len(b) < record_size:
                b = b[:-1] + (b" " * (record_size - len(b))) + b"\n"
            elif len(b) > record_size:
                b = b[: record_size - 1] + b"\n"

            outf.write(b)
            num_records += 1

    # write config
    with open(cfg_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"numSortedRecords={num_records}\n")
        f.write("numUnsortedRecords=0\n")
        f.write(f"recordSize={record_size}\n")
        f.write("widths=" + ",".join(str(w[k]) for k in ("name","rank","city","state","zip","employees")) + "\n")

    return True
