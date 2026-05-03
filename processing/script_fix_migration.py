"""
One-off migration script to correct non-USD currency labels for existing Adzuna rows.
Run this once manually on the EC2 instance.
"""

from sqlalchemy import text
from storage.db import SessionLocal
from monitoring.logger import get_logger

logger = get_logger(__name__)

def run_migration():
    db = SessionLocal()
    try:
        # Step A - Add backup columns (safe to re-run)
        logger.info("Adding backup columns to jobs table...")
        db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS salary_min_original FLOAT;"))
        db.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS salary_max_original FLOAT;"))
        db.commit()

        # Step B - Back up original values
        logger.info("Backing up original salaries...")
        db.execute(text("""
            UPDATE jobs 
            SET salary_min_original = salary_min, 
                salary_max_original = salary_max 
            WHERE salary_min_original IS NULL AND salary_min IS NOT NULL;
        """))
        db.commit()

        # Step C - Correct the currency label per country for Adzuna rows
        logger.info("Correcting currency labels for Adzuna rows...")
        updates = [
            "UPDATE jobs SET salary_currency = 'GBP' WHERE source = 'adzuna' AND country = 'GB';",
            "UPDATE jobs SET salary_currency = 'INR' WHERE source = 'adzuna' AND country = 'IN';",
            "UPDATE jobs SET salary_currency = 'CAD' WHERE source = 'adzuna' AND country = 'CA';",
            "UPDATE jobs SET salary_currency = 'AUD' WHERE source = 'adzuna' AND country = 'AU';",
            "UPDATE jobs SET salary_currency = 'EUR' WHERE source = 'adzuna' AND country IN ('DE', 'FR', 'NL');",
            "UPDATE jobs SET salary_currency = 'SGD' WHERE source = 'adzuna' AND country = 'SG';",
            "UPDATE jobs SET salary_currency = 'NZD' WHERE source = 'adzuna' AND country = 'NZ';",
            "UPDATE jobs SET salary_currency = 'ZAR' WHERE source = 'adzuna' AND country = 'ZA';"
        ]
        for query in updates:
            db.execute(text(query))
        db.commit()

        # Step D - Print verification report
        logger.info("Generating verification report...")
        result = db.execute(text("""
            SELECT source, country, salary_currency, COUNT(*) 
            FROM jobs 
            GROUP BY source, country, salary_currency 
            ORDER BY source, country;
        """))
        
        print("\n--- MIGRATION REPORT ---")
        for row in result:
            print(f"Source: {row[0]:<10} | Country: {row[1]:<5} | Currency: {row[2]:<5} | Count: {row[3]}")
        print("------------------------\n")

    except Exception as exc:
        db.rollback()
        logger.error(f"Migration failed: {exc}")
    finally:
        db.close()

if __name__ == "__main__":
    run_migration()