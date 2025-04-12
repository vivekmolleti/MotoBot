import postgresql_operations_code
import database_schema.json_data_to_db as json_data_to_db
import logging

# Configure logging
logging.basicConfig(
    filename="database_main.log",  # Log file name
    level=logging.INFO,  # Log level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    filemode="w"  # Overwrite the log file each time the script runs
)

def database_main():
    """Main function to handle database operations"""
    logging.info("Starting database operations.")
    # Connect to the PostgreSQL database
    conn = postgresql_operations_code.connect()
    if not conn:
        logging.error("Failed to connect to the PostgreSQL database.")
        return
    try:
        # Extract company ID
        logging.info("Extracting company ID.")
        company_id = postgresql_operations_code.extract_company_id(conn)
        if not company_id:
            logging.error("Failed to fetch company ID.")
            return
        logging.info(f"Fetched company ID: {company_id}")

        # Path to the JSON file
        json_path = r"D:\MotoBot\final_grouped_family.json"
        logging.info(f"Loading JSON data from: {json_path}")

        # Load JSON data into the database
        json_data_to_db.json_to_csv(json_path, conn, company_id)
        logging.info("JSON data successfully loaded into the database.")

    except Exception as e:
        logging.error(f"An error occurred during database operations: {e}", exc_info=True)

    finally:
        # Close the database connection
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("Database operations completed.")



if __name__ == "__main__":
    database_main()