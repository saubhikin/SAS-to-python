"""
Employee data transformation module.

This module handles filtering, validation, and sorting of employee records,
matching the SAS DATA step and PROC SORT logic.
"""

import logging
from typing import List, Dict, Any

from domain.employee_rules import EmployeeValidationRules


class EmployeeTransformer:
    """
    Transforms and filters employee records according to business rules.
    
    Implements SAS logic:
    - WHERE clause filtering (LAST_NAME validation)
    - PROC SORT by EMPID
    """
    
    def __init__(self):
        """Initialize the transformer."""
        self.logger = logging.getLogger(__name__)
        self.validation_rules = EmployeeValidationRules()
    
    def transform(self, employee_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter and validate employee records.
        
        Applies business rules matching SAS WHERE clause:
        WHERE LAST_NAME <> '               ' AND
              LAST_NAME <> X'000000000000000000000000000000'
        
        Args:
            employee_data: List of raw employee records
            
        Returns:
            List of validated employee records
        """
        if not employee_data:
            self.logger.warning("No employee data to transform")
            return []
        
        initial_count = len(employee_data)
        self.logger.debug(f"Transforming {initial_count} employee records")
        
        # Filter records based on validation rules
        valid_records = []
        invalid_count = 0
        
        for record in employee_data:
            if self.validation_rules.is_valid_employee_record(record):
                valid_records.append(record)
            else:
                invalid_count += 1
                self.logger.debug(
                    f"Filtered out invalid record: EMPID={record.get('EMPID', 'N/A')}, "
                    f"LASTNAME='{record.get('LASTNAME', '')}'"
                )
        
        self.logger.info(f"Filtered out {invalid_count} invalid records")
        self.logger.info(f"Retained {len(valid_records)} valid records")
        
        return valid_records
    
    def sort_by_empid(self, employee_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort employee records by EMPID in ascending order.
        
        Matches SAS PROC SORT:
        PROC SORT DATA=A;
           BY EMPID;
        
        Args:
            employee_data: List of employee records to sort
            
        Returns:
            Sorted list of employee records
        """
        if not employee_data:
            self.logger.warning("No employee data to sort")
            return []
        
        self.logger.debug(f"Sorting {len(employee_data)} records by EMPID")
        
        # Sort by EMPID (ascending)
        # Handle None/null values by treating them as empty string
        sorted_data = sorted(
            employee_data,
            key=lambda x: str(x.get('EMPID', ''))
        )
        
        self.logger.debug("Records sorted successfully")
        
        return sorted_data
