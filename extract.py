"""
Employee data extraction module.

This module handles extraction of employee records from DB2 database,
matching the SAS PROC SQL pass-through query logic.
"""

import logging
from typing import List, Dict, Any

import ibm_db
import ibm_db_dbi

from config.settings import DatabaseConfig


class EmployeeExtractor:
    """
    Extracts employee records from DB2 database.
    
    Implements the SAS PROC SQL logic:
    CONNECT TO DB2 (SSID=DSNP);
    SELECT EMP_ID AS EMPID, LAST_NAME AS LASTNAME
    FROM PCDS.EMPLOYEE
    WHERE LAST_NAME <> '               ' AND
          LAST_NAME <> X'000000000000000000000000000000'
    """
    
    def __init__(self, db_config: DatabaseConfig):
        """
        Initialize the extractor with database configuration.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
    
    def _create_connection(self) -> ibm_db_dbi.Connection:
        """
        Create DB2 database connection.
        
        Returns:
            DB2 connection object
            
        Raises:
            Exception: If connection fails
        """
        try:
            # Build connection string
            # Format: DATABASE=dbname;HOSTNAME=host;PORT=port;PROTOCOL=TCPIP;UID=user;PWD=password
            conn_str = (
                f"DATABASE={self.db_config.database};"
                f"HOSTNAME={self.db_config.host};"
                f"PORT={self.db_config.port};"
                f"PROTOCOL=TCPIP;"
                f"UID={self.db_config.user};"
                f"PWD={self.db_config.password};"
            )
            
            self.logger.debug(f"Connecting to DB2: {self.db_config.host}:{self.db_config.port}/{self.db_config.database}")
            
            # Create connection
            ibm_conn = ibm_db.connect(conn_str, "", "")
            conn = ibm_db_dbi.Connection(ibm_conn)
            
            self.logger.info("DB2 connection established successfully")
            return conn
            
        except Exception as e:
            self.logger.error(f"Failed to connect to DB2: {str(e)}")
            raise
    
    def extract_employees(self) -> List[Dict[str, Any]]:
        """
        Extract employee records from DB2 PCDS.EMPLOYEE table.
        
        Executes SQL query matching SAS pass-through SQL:
        SELECT EMP_ID AS EMPID, LAST_NAME AS LASTNAME
        FROM PCDS.EMPLOYEE
        WHERE LAST_NAME <> '               ' AND
              LAST_NAME <> X'000000000000000000000000000000'
        
        Note: The WHERE clause filtering is handled in transform step
        to match SAS behavior of filtering after extraction.
        
        Returns:
            List of employee dictionaries with keys: EMPID, LASTNAME
            
        Raises:
            Exception: If extraction fails
        """
        conn = None
        cursor = None
        
        try:
            # Establish connection
            conn = self._create_connection()
            cursor = conn.cursor()
            
            # SQL query matching SAS logic
            # Note: We extract all records; filtering happens in transform step
            # to exactly match SAS processing order
            query = """
                SELECT 
                    EMP_ID AS EMPID,
                    LAST_NAME AS LASTNAME
                FROM PCDS.EMPLOYEE
            """
            
            self.logger.debug(f"Executing query: {query}")
            
            # Execute query
            cursor.execute(query)
            
            # Fetch all results
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                results.append(record)
            
            self.logger.info(f"Extracted {len(results)} employee records from DB2")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to extract employee data: {str(e)}")
            raise
            
        finally:
            # Clean up resources
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                self.logger.debug("DB2 connection closed")
