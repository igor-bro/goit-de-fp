from pathlib import Path

ROOT = Path(__file__).resolve().parent
LANDING = ROOT / "data" / "landing"
BRONZE = ROOT / "bronze"
SILVER = ROOT / "silver"
GOLD = ROOT / "gold"
JAR = ROOT / "jars" / "mysql-connector-j-8.0.32.jar"

TABLES = ("athlete_bio", "athlete_event_results")
