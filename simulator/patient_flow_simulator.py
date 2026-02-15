"""
Patient Flow Simulator — Healthcare Analytics Platform
=======================================================
Generates realistic patient admission & discharge events for 
Midwest Health Alliance (7 hospitals) and streams them to Azure Event Hubs.

Features:
  - Separate ADMISSION and DISCHARGE event lifecycle
  - Richer patient data model (diagnosis, insurance, priority, vitals)
  - Configurable dirty data injection (7 scenarios)
  - Department capacity awareness
  - Seasonal surge simulation (flu season, holiday spikes)
  - Config-driven via YAML

Author : Omkar
Project: End-to-End Healthcare Data Engineering (Azure Databricks + ADF)
"""

import json
import random
import uuid
import time
import copy
import logging
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("patient_flow_simulator")

# ─────────────────────────────────────────────
# DEFAULT CONFIGURATION
# ─────────────────────────────────────────────
DEFAULT_CONFIG = {
    "eventhub": {
        "namespace": "healthcare-analytics-namespace.servicebus.windows.net",
        "hub_name": "healthcare-analytics-eh",
        "connection_string": "${EVENTHUB_CONNECTION_STRING}"
    },

    "hospitals": {
        1: "MHA Central Hospital",
        2: "MHA Lakeside Medical Center",
        3: "MHA Riverside General",
        4: "MHA Prairie Health",
        5: "MHA Summit Medical",
        6: "MHA Valley Care",
        7: "MHA Metro Hospital"
    },

    # Department name → base bed capacity per hospital
    "departments": {
        "Emergency":  {"capacity": 40, "avg_stay_hrs": 6},
        "Surgery":    {"capacity": 30, "avg_stay_hrs": 48},
        "ICU":        {"capacity": 20, "avg_stay_hrs": 72},
        "Pediatrics": {"capacity": 25, "avg_stay_hrs": 36},
        "Maternity":  {"capacity": 20, "avg_stay_hrs": 48},
        "Oncology":   {"capacity": 25, "avg_stay_hrs": 96},
        "Cardiology": {"capacity": 25, "avg_stay_hrs": 60}
    },

    # Diagnosis codes (simplified ICD-10 style) mapped to likely departments
    "diagnoses": {
        "Emergency":  ["R07.9-Chest Pain", "S72.0-Hip Fracture", "R55-Syncope", "T78.2-Anaphylaxis"],
        "Surgery":    ["K35.8-Appendicitis", "K80.2-Gallstones", "M17.1-Knee Replacement", "K40.9-Hernia"],
        "ICU":        ["J96.0-Respiratory Failure", "I21.0-STEMI", "R57.0-Cardiogenic Shock", "G93.1-Brain Injury"],
        "Pediatrics": ["J06.9-Upper Respiratory Infection", "A09-Gastroenteritis", "J45.2-Asthma Exacerbation"],
        "Maternity":  ["O80-Normal Delivery", "O82-C-Section Delivery", "O14.1-Preeclampsia"],
        "Oncology":   ["C34.9-Lung Cancer", "C50.9-Breast Cancer", "C18.9-Colon Cancer", "C61-Prostate Cancer"],
        "Cardiology": ["I25.1-Coronary Artery Disease", "I48.0-Atrial Fibrillation", "I50.9-Heart Failure"]
    },

    "insurance_types": ["Private", "Medicare", "Medicaid", "Self-Pay", "Employer-Sponsored"],

    "admission_priorities": ["Emergency", "Urgent", "Scheduled", "Transfer"],

    # Dirty data injection rates (0.0 to 1.0)
    "dirty_data": {
        "invalid_age":            0.04,   # age > 120 or negative
        "future_admission":       0.03,   # admission_time in the future
        "missing_field":          0.05,   # null out a random field
        "duplicate_event":        0.03,   # send same event twice
        "discharge_before_admit": 0.02,   # discharge < admission
        "department_typo":        0.03,   # misspelled department name
        "invalid_hospital_id":    0.02    # hospital_id outside 1-7
    },

    # Simulation settings
    "simulation": {
        "events_per_second": 1,           # base rate
        "surge_multiplier": 3,            # multiplier during surge hours
        "max_active_patients": 500,       # memory cap for active patients
        "discharge_check_probability": 0.3 # chance to discharge an active patient each cycle
    }
}


# ─────────────────────────────────────────────
# DEPARTMENT TYPO MAP (for dirty data)
# ─────────────────────────────────────────────
DEPARTMENT_TYPOS = {
    "Emergency": ["Emergancy", "Emergncy", "ER", "EMERGENCY"],
    "Surgery":   ["Surgey", "Surgeyr", "SURGERY"],
    "ICU":       ["icu", "I.C.U.", "IntensiveCare"],
    "Pediatrics":["Paediatrics", "Pedeatrics", "PEDS"],
    "Maternity": ["Maternty", "Maternitiy", "OB/GYN"],
    "Oncology":  ["Oncolgy", "Oncologgy", "ONCO"],
    "Cardiology":["Cardiolgy", "Cardilogy", "CARDIO"]
}


# ─────────────────────────────────────────────
# SIMULATOR CLASS
# ─────────────────────────────────────────────
class PatientFlowSimulator:
    """
    Simulates realistic patient flow events for a hospital network.
    Tracks active patients to generate proper admission → discharge lifecycle.
    """

    def __init__(self, config: dict, dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run
        self.active_patients = {}          # patient_id → patient record
        self.stats = {
            "admissions_sent": 0,
            "discharges_sent": 0,
            "dirty_records": 0,
            "duplicates_sent": 0
        }

        if not dry_run:
            self.producer = self._create_producer()
        else:
            self.producer = None
            logger.info("DRY RUN mode — events will be printed but not sent to Event Hub")

    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer connected to Azure Event Hubs."""
        eh = self.config["eventhub"]
        return KafkaProducer(
            bootstrap_servers=[f"{eh['namespace']}:9093"],
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username="$ConnectionString",
            sasl_plain_password=eh["connection_string"],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
        )

    # ── Vitals Generation ──────────────────────
    def _generate_vitals(self, age, department: str) -> dict:
        """Generate realistic vital signs based on age and department."""
        age = age if isinstance(age, (int, float)) and age > 0 else 50  # default if dirty
        base_hr = 72 if age < 60 else 78
        base_bp_sys = 118 if age < 50 else 135

        # ICU / Emergency patients tend to have more extreme vitals
        if department in ("ICU", "Emergency"):
            base_hr += random.randint(-10, 25)
            base_bp_sys += random.randint(-15, 30)

        return {
            "heart_rate": max(40, base_hr + random.randint(-12, 12)),
            "bp_systolic": max(80, base_bp_sys + random.randint(-15, 15)),
            "bp_diastolic": max(50, random.randint(65, 90)),
            "temperature_f": round(random.uniform(97.0, 103.0), 1),
            "oxygen_saturation": min(100, max(85, random.randint(92, 100) - (5 if department == "ICU" else 0)))
        }

    # ── Admission Event ────────────────────────
    def generate_admission(self) -> dict:
        """Generate a new patient admission event."""
        department = random.choice(list(self.config["departments"].keys()))
        dept_info = self.config["departments"][department]
        hospital_id = random.randint(1, 7)
        age = random.randint(0, 100)
        gender = random.choice(["Male", "Female"])

        # Weighted admission priority — Emergency is most common
        priority = random.choices(
            self.config["admission_priorities"],
            weights=[40, 25, 25, 10],
            k=1
        )[0]

        # Pick a diagnosis matching the department
        diagnosis = random.choice(self.config["diagnoses"][department])

        admission_time = datetime.utcnow() - timedelta(minutes=random.randint(0, 30))

        # Estimate expected discharge based on department avg stay
        avg_stay = dept_info["avg_stay_hrs"]
        expected_discharge = admission_time + timedelta(
            hours=max(1, avg_stay + random.randint(-avg_stay // 3, avg_stay // 3))
        )

        event = {
            "event_type": "ADMISSION",
            "event_id": str(uuid.uuid4()),
            "patient_id": str(uuid.uuid4()),
            "gender": gender,
            "age": age,
            "department": department,
            "hospital_id": hospital_id,
            "hospital_name": self.config["hospitals"].get(hospital_id, "Unknown"),
            "bed_id": f"H{hospital_id}-{department[:3].upper()}-{random.randint(1, dept_info['capacity'])}",
            "admission_time": admission_time.isoformat(),
            "expected_discharge_time": expected_discharge.isoformat(),
            "admission_priority": priority,
            "diagnosis_code": diagnosis,
            "insurance_type": random.choice(self.config["insurance_types"]),
            "vitals": self._generate_vitals(age, department),
            "event_timestamp": datetime.utcnow().isoformat()
        }

        return event

    # ── Discharge Event ────────────────────────
    def generate_discharge(self, admission_record: dict) -> dict:
        """Generate a discharge event for an existing active patient."""
        admission_time_str = admission_record.get("admission_time")
        admission_time = (
            datetime.fromisoformat(admission_time_str)
            if admission_time_str
            else datetime.utcnow() - timedelta(hours=random.randint(1, 24))
        )
        dept = admission_record.get("department", "Emergency")
        # Handle typo'd department names — fall back to Emergency avg stay
        avg_stay = self.config["departments"].get(dept, {}).get("avg_stay_hrs", 12)

        actual_stay = max(1, avg_stay + random.randint(-avg_stay // 3, avg_stay // 2))
        discharge_time = admission_time + timedelta(hours=actual_stay)

        # If discharge time is still in the future, set it to now
        if discharge_time > datetime.utcnow():
            discharge_time = datetime.utcnow()

        discharge_status = random.choices(
            ["Discharged-Home", "Discharged-Transfer", "Discharged-AMA", "Deceased"],
            weights=[70, 15, 10, 5],
            k=1
        )[0]

        event = {
            "event_type": "DISCHARGE",
            "event_id": str(uuid.uuid4()),
            "patient_id": admission_record["patient_id"],
            "gender": admission_record["gender"],
            "age": admission_record["age"],
            "department": admission_record["department"],
            "hospital_id": admission_record["hospital_id"],
            "hospital_name": admission_record["hospital_name"],
            "bed_id": admission_record["bed_id"],
            "admission_time": admission_record["admission_time"],
            "discharge_time": discharge_time.isoformat(),
            "length_of_stay_hrs": round((discharge_time - admission_time).total_seconds() / 3600, 2),
            "discharge_status": discharge_status,
            "diagnosis_code": admission_record["diagnosis_code"],
            "insurance_type": admission_record["insurance_type"],
            "vitals": self._generate_vitals(admission_record["age"], dept),
            "event_timestamp": datetime.utcnow().isoformat()
        }

        return event

    # ── Dirty Data Injection ───────────────────
    def inject_dirty_data(self, record: dict) -> tuple[dict, bool]:
        """
        Inject realistic dirty data issues into a record.
        Returns (modified_record, was_dirtied).
        """
        dirty_conf = self.config["dirty_data"]
        was_dirtied = False
        record = copy.deepcopy(record)

        # 1. Invalid age (negative or impossibly old)
        if random.random() < dirty_conf["invalid_age"]:
            record["age"] = random.choice([-5, -1, 0, 125, 150, 200])
            was_dirtied = True

        # 2. Future admission timestamp
        if random.random() < dirty_conf["future_admission"]:
            future = datetime.utcnow() + timedelta(hours=random.randint(24, 168))
            record["admission_time"] = future.isoformat()
            was_dirtied = True

        # 3. Missing / null field
        if random.random() < dirty_conf["missing_field"]:
            nullable_fields = ["gender", "age", "diagnosis_code", "insurance_type", "admission_time"]
            field = random.choice(nullable_fields)
            record[field] = None
            was_dirtied = True

        # 4. Discharge time before admission time
        if random.random() < dirty_conf["discharge_before_admit"] and record["event_type"] == "DISCHARGE":
            adm = datetime.fromisoformat(record["admission_time"]) if record["admission_time"] else datetime.utcnow()
            record["discharge_time"] = (adm - timedelta(hours=random.randint(1, 24))).isoformat()
            record["length_of_stay_hrs"] = -abs(record.get("length_of_stay_hrs", 0))
            was_dirtied = True

        # 5. Department name typo
        if random.random() < dirty_conf["department_typo"]:
            dept = record.get("department", "Emergency")
            if dept in DEPARTMENT_TYPOS:
                record["department"] = random.choice(DEPARTMENT_TYPOS[dept])
            was_dirtied = True

        # 6. Invalid hospital ID
        if random.random() < dirty_conf["invalid_hospital_id"]:
            record["hospital_id"] = random.choice([-1, 0, 99, 999])
            record["hospital_name"] = "UNKNOWN"
            was_dirtied = True

        if was_dirtied:
            self.stats["dirty_records"] += 1

        return record, was_dirtied

    # ── Seasonal Surge Logic ───────────────────
    def _is_surge_period(self) -> bool:
        """
        Simulate seasonal surge: flu season (Dec-Feb) and 
        holiday weekends have higher admission rates.
        Uses current real month for demo purposes.
        """
        now = datetime.utcnow()
        # Flu season months
        if now.month in (12, 1, 2):
            return True
        # Weekend surge (Fri-Sun evenings)
        if now.weekday() >= 4 and now.hour >= 18:
            return True
        return False

    # ── Send Event ─────────────────────────────
    def _send_event(self, event: dict):
        """Send event to Event Hub or print in dry-run mode."""
        hub_name = self.config["eventhub"]["hub_name"]

        if self.dry_run:
            logger.info(f"[DRY RUN] {event['event_type']} | patient={event['patient_id'][:8]}... | "
                        f"dept={event['department']} | hospital={event['hospital_id']}")
        else:
            self.producer.send(hub_name, event)
            logger.info(f"SENT {event['event_type']} | patient={event['patient_id'][:8]}... | "
                        f"dept={event['department']} | hospital={event['hospital_id']}")

    # ── Main Simulation Loop ───────────────────
    def run(self, max_events: int = None):
        """
        Run the simulator.
        
        Args:
            max_events: Stop after N events. None = run forever.
        """
        sim_conf = self.config["simulation"]
        event_count = 0

        logger.info("=" * 60)
        logger.info("  PATIENT FLOW SIMULATOR — Midwest Health Alliance")
        logger.info(f"  Target : {self.config['eventhub']['hub_name']}")
        logger.info(f"  Mode   : {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"  Events : {'Unlimited' if max_events is None else max_events}")
        logger.info("=" * 60)

        try:
            while max_events is None or event_count < max_events:

                # ── Determine event rate based on surge ──
                sleep_time = 1.0 / sim_conf["events_per_second"]
                if self._is_surge_period():
                    sleep_time /= sim_conf["surge_multiplier"]

                # ── Decide: new admission or discharge an active patient ──
                if (self.active_patients and
                    random.random() < sim_conf["discharge_check_probability"]):
                    
                    # Discharge a random active patient
                    pid = random.choice(list(self.active_patients.keys()))
                    admission_rec = self.active_patients.pop(pid)
                    event = self.generate_discharge(admission_rec)
                    event, _ = self.inject_dirty_data(event)
                    self._send_event(event)
                    self.stats["discharges_sent"] += 1

                    # Duplicate injection check
                    if random.random() < self.config["dirty_data"]["duplicate_event"]:
                        self._send_event(event)   # send same event again
                        self.stats["duplicates_sent"] += 1
                        event_count += 1

                else:
                    # New admission
                    event = self.generate_admission()
                    event, _ = self.inject_dirty_data(event)
                    self._send_event(event)
                    self.stats["admissions_sent"] += 1

                    # Track active patient (cap to avoid memory issues)
                    if len(self.active_patients) < sim_conf["max_active_patients"]:
                        self.active_patients[event["patient_id"]] = event

                    # Duplicate injection check
                    if random.random() < self.config["dirty_data"]["duplicate_event"]:
                        self._send_event(event)
                        self.stats["duplicates_sent"] += 1
                        event_count += 1

                event_count += 1
                time.sleep(sleep_time)

                # Print stats every 50 events
                if event_count % 50 == 0:
                    self._print_stats()

        except KeyboardInterrupt:
            logger.info("\nSimulation stopped by user.")
        finally:
            self._print_stats()
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed.")

    def _print_stats(self):
        logger.info(
            f"STATS | admissions={self.stats['admissions_sent']} | "
            f"discharges={self.stats['discharges_sent']} | "
            f"active_patients={len(self.active_patients)} | "
            f"dirty={self.stats['dirty_records']} | "
            f"duplicates={self.stats['duplicates_sent']}"
        )


# ─────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Patient Flow Simulator for Healthcare Analytics Platform"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print events to console without sending to Event Hub"
    )
    parser.add_argument(
        "--max-events", type=int, default=None,
        help="Stop after N events (default: run until interrupted)"
    )
    parser.add_argument(
        "--connection-string", type=str, default=None,
        help="Event Hub connection string (overrides config)"
    )
    args = parser.parse_args()

    config = DEFAULT_CONFIG.copy()

    # Override connection string from CLI if provided
    if args.connection_string:
        config["eventhub"]["connection_string"] = args.connection_string

    simulator = PatientFlowSimulator(
        config=config,
        dry_run=args.dry_run
    )
    simulator.run(max_events=args.max_events)


if __name__ == "__main__":
    main()