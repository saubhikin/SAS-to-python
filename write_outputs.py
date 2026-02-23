"""
Employee output file writer module.

This module handles writing formatted employee records to output file,
matching the SAS DATA _NULL_ step with FILE/PUT statements.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any

from config.settings import OutputConfig
from domain.employee_rules import EmployeeValidationRules


class EmployeeFileWriter:
    """
    Writes formatted employee records to output file.
    
    Implements SAS DATA _NULL_ logic:
    FILE OUT1;
    PUT @001 EMPID $CHAR07.
        @008 LASTNAME $CHAR15.;
    """
    
    def __init__(self, output_config: OutputConfig):
        """
        Initialize the file writer with output configuration.
        
        Args:
            output_config: Output file configuration
        """
        self.output_config = output_config
        self.logger = logging.getLogger(__name__)
        self.validation_rules = EmployeeValidationRules()
    
    def _format_record(self, employee: Dict[str, Any]) -> str:
        """
        Format a single employee record according to SAS PUT specifications.
        
        Matches SAS format:
        PUT @001 EMPID $CHAR07.
            @008 LASTNAME $CHAR15.
        
        Position 1-7:   EMPID (7 characters, left-aligned)
        Position 8-22:  LASTNAME (15 characters, left-aligned)
        
        Args:
            employee: Employee record dictionary
            
        Returns:
            Formatted record string (22 characters)
        """
        empid = employee.get('EMPID', '')
        lastname = employee.get('LASTNAME', '')
        
        # Format using business rules (matches SAS $CHAR formats)
        formatted_empid = self.validation_rules.format_empid(empid)
        formatted_lastname = self.validation_rules.format_lastname(lastname)
        
        # Build record with exact positioning matching SAS @001 and @008
        # Position 1-7: EMPID, Position 8-22: LASTNAME
        record = formatted_empid + formatted_lastname
        
        return record
    
    def write_employee_file(self, employee_data: List[Dict[str, Any]]) -> str:
        """
        Write formatted employee records to output file.
        
        Matches SAS DATA _NULL_ step:
        SET A;
        FILE OUT1;
        PUT @001 EMPID $CHAR07.
            @008 LASTNAME $CHAR15.;
        
        Args:
            employee_data: List of employee records to write
            
        Returns:
            Path to the output file
            
        Raises:
            Exception: If file writing fails
        """
        output_path = Path(self.output_config.file_path)
        
        try:
            # Create parent directories if they don't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.logger.debug(f"Writing {len(employee_data)} records to {output_path}")
            
            # Write records to file
            with open(output_path, 'w', encoding='utf-8') as f:
                for record in employee_data:
                    formatted_line = self._format_record(record)
                    f.write(formatted_line + '\n')
            
            # Verify file was created
            if not output_path.exists():
                raise IOError(f"Output file was not created: {output_path}")
            
            file_size = output_path.stat().st_size
            self.logger.info(f"Output file created: {output_path}")
            self.logger.info(f"File size: {file_size} bytes")
            self.logger.info(f"Records written: {len(employee_data)}")
            
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Failed to write output file: {str(e)}")
            raise
