"""
Business rules for employee data validation and processing.

This module centralizes business logic for determining valid employee records
based on data quality rules matching the original SAS program logic.
"""

from typing import Dict, Any


class EmployeeValidationRules:
    """
    Centralized business rules for employee data validation.
    
    These rules match the SAS WHERE clause logic:
    - LAST_NAME <> '               ' (15 spaces)
    - LAST_NAME <> X'000000000000000000000000000000' (hex zeros)
    """
    
    # Constants matching SAS format specifications
    LASTNAME_LENGTH = 15
    EMPID_LENGTH = 7
    
    # Validation patterns
    EMPTY_LASTNAME_SPACE = ' ' * LASTNAME_LENGTH
    EMPTY_LASTNAME_HEX = '\x00' * LASTNAME_LENGTH
    
    @classmethod
    def is_valid_employee_record(cls, employee: Dict[str, Any]) -> bool:
        """
        Validate if an employee record meets business criteria.
        
        Matches SAS WHERE clause:
        WHERE LAST_NAME <> '               ' AND
              LAST_NAME <> X'000000000000000000000000000000'
        
        Args:
            employee: Dictionary containing employee data with 'LASTNAME' key
            
        Returns:
            True if employee record is valid, False otherwise
        """
        lastname = employee.get('LASTNAME', '')
        
        # Rule 1: LASTNAME must not be all spaces (15 spaces)
        if lastname == cls.EMPTY_LASTNAME_SPACE:
            return False
        
        # Rule 2: LASTNAME must not be all hex zeros
        if lastname == cls.EMPTY_LASTNAME_HEX:
            return False
        
        # Rule 3: LASTNAME must not be empty/None
        if not lastname or lastname.strip() == '':
            return False
        
        return True
    
    @classmethod
    def format_empid(cls, empid: str) -> str:
        """
        Format employee ID according to SAS $CHAR07. specification.
        
        Args:
            empid: Raw employee ID
            
        Returns:
            Left-aligned employee ID, padded/truncated to 7 characters
        """
        if empid is None:
            empid = ''
        
        empid_str = str(empid)
        
        # Left-align and pad with spaces or truncate to 7 chars
        # Matches SAS $CHAR07. format behavior
        return empid_str.ljust(cls.EMPID_LENGTH)[:cls.EMPID_LENGTH]
    
    @classmethod
    def format_lastname(cls, lastname: str) -> str:
        """
        Format last name according to SAS $CHAR15. specification.
        
        Args:
            lastname: Raw last name
            
        Returns:
            Left-aligned last name, padded/truncated to 15 characters
        """
        if lastname is None:
            lastname = ''
        
        # Left-align and pad with spaces or truncate to 15 chars
        # Matches SAS $CHAR15. format behavior
        return lastname.ljust(cls.LASTNAME_LENGTH)[:cls.LASTNAME_LENGTH]
