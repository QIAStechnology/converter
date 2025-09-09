#!/usr/bin/env python3
"""
Product Synchronization Script
==============================

This script synchronizes product data from a CSV file to an XML database.
It handles adding new products, updating existing ones, and validates data integrity.

Author: Qias Technology
Version: 1.0.0
"""

import csv
import datetime
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Configuration Constants
class Config:
    """Configuration constants for the sync script."""
    
    CSV_FILE = "carrefour_test_v2.csv"
    XML_FILE = r"c:\ProgramData\Avery Berkel\MXBusiness\DEFAULT_5.4.5.3503\Project\MXBusiness - 638907826887926093\Data\Database\database.xml"
    # XML_FILE = "databaseSafe.xml"
    CSV_ENCODING = "latin-1"
    CSV_DELIMITER = ";"
    
    # Price validation limits
    MIN_PRICE = 0.00
    MAX_PRICE = 999999.99
    
    # Logging configuration
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class ProductSyncError(Exception):
    """Custom exception for product sync operations."""
    pass


class DataValidator:
    """Handles data validation and normalization."""
    
    @staticmethod
    def normalize_price(value: str) -> str:
        """
        Normalize price value by removing currency symbols and converting to decimal.
        
        Args:
            value (str): Raw price value from CSV
            
        Returns:
            str: Normalized price as string with 2 decimal places
            
        Examples:
            normalize_price("€12,50") -> "12.50"
            normalize_price("$15.99") -> "15.99"
        """
        if not value or not value.strip():
            return "0.00"

        # Remove currency symbols and keep only digits, dots, commas, and minus
        cleaned_value = re.sub(r"[^\d.,-]", "", value.strip())

        # Handle comma as decimal separator (European format)
        if "," in cleaned_value and "." not in cleaned_value:
            cleaned_value = cleaned_value.replace(",", ".")

        try:
            decimal_value = Decimal(cleaned_value)
            return f"{decimal_value:.2f}"
        except InvalidOperation:
            logging.warning(f"Invalid price format: '{value}' - defaulting to 0.00")
            return "0.00"

    @staticmethod
    def normalize_ean(value: str) -> str:
        """
        Normalize EAN code by converting scientific notation to integer string.
        
        Args:
            value (str): Raw EAN value from CSV
            
        Returns:
            str: Normalized EAN as digit-only string
        """
        if not value or not value.strip():
            return "0"
            
        try:
            # Handle scientific notation (e.g., "2.52E+12" -> "2520000000000")
            return str(int(float(value)))
        except (ValueError, OverflowError):
            # Keep only digits if conversion fails
            digits_only = re.sub(r"\D", "", str(value))
            return digits_only if digits_only else "0"

    @staticmethod
    def safe_int_conversion(value: str, default: int = 0) -> int:
        """
        Safely convert string to integer with fallback.
        
        Args:
            value (str): String value to convert
            default (int): Default value if conversion fails
            
        Returns:
            int: Converted integer or default value
        """
        try:
            return int(value.strip()) if value and value.strip() else default
        except (ValueError, TypeError, AttributeError):
            return default

    @staticmethod
    def validate_price_range(price: str) -> bool:
        """
        Validate if price is within acceptable range.
        
        Args:
            price (str): Price value to validate
            
        Returns:
            bool: True if price is valid, False otherwise
        """
        try:
            price_val = float(price)
            return Config.MIN_PRICE <= price_val <= Config.MAX_PRICE
        except (ValueError, TypeError):
            return False


class CSVLoader:
    """Handles loading and parsing CSV files."""
    
    def __init__(self, file_path: str):
        """
        Initialize CSV loader with file path.
        
        Args:
            file_path (str): Path to the CSV file
        """
        self.file_path = Path(file_path)
        self.validator = DataValidator()
        
    def load(self) -> List[Dict]:
        """
        Load products from CSV file with validation.
        
        Returns:
            List[Dict]: List of validated product dictionaries
            
        Raises:
            ProductSyncError: If file cannot be loaded or processed
        """
        if not self.file_path.exists():
            raise ProductSyncError(f"CSV file not found: {self.file_path}")
            
        logging.info(f"Loading CSV file: {self.file_path}")
        
        products = []
        skipped_count = 0
        
        try:
            with open(self.file_path, newline="", encoding=Config.CSV_ENCODING) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=Config.CSV_DELIMITER)
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 for header
                    try:
                        product = self._process_row(row, row_num)
                        if product:
                            products.append(product)
                        else:
                            skipped_count += 1
                    except Exception as e:
                        logging.error(f"Error processing row {row_num}: {e}")
                        skipped_count += 1
                        
        except Exception as e:
            raise ProductSyncError(f"Failed to load CSV file: {e}")
            
        logging.info(f"Successfully loaded {len(products)} products from CSV")
        if skipped_count > 0:
            logging.warning(f"Skipped {skipped_count} invalid rows")
            
        return products
    
    def _process_row(self, row: Dict[str, str], row_num: int) -> Optional[Dict]:
        """
        Process a single CSV row into a product dictionary.
        
        Args:
            row (Dict[str, str]): Raw CSV row data
            row_num (int): Row number for logging
            
        Returns:
            Optional[Dict]: Product dictionary or None if invalid
        """
        # Extract and validate price
        raw_price = row.get("Retail Price (1st)", "").strip()
        normalized_price = self.validator.normalize_price(raw_price)
        
        if not self.validator.validate_price_range(normalized_price):
            logging.warning(f"Row {row_num}: Invalid price '{raw_price}' - skipping")
            return None
            
        # Extract PLU and validate
        plu = self.validator.safe_int_conversion(row.get("PLU Number", ""))
        if plu == 0:
            logging.warning(f"Row {row_num}: Missing or invalid PLU - skipping")
            return None
        
        # Extract Product Type and validate that is in [0, 1, 2, 4, 6, 9, 99]
        #  where 0 by weight, 1 by count, 2 fixed price, 4 fixed weight (total price), 6 by , 9 by manual weight, 99 Negative by count
        product_type = self.validator.safe_int_conversion(row.get("Product Type", ""))
        if product_type not in [0, 1, 2, 4, 6, 9, 99]:
            logging.warning(f"Row {row_num}: Invalid Product Type '{product_type}' - skipping")
            return None

        # check if Price Modifier Multiplier is valid integer in [1, 99]
        price_modifier_multiplier = self.validator.safe_int_conversion(row.get("Price Modifier Multiplier", "1"), default=1)
        if price_modifier_multiplier < 1 or price_modifier_multiplier > 100:
            logging.warning(f"Row {row_num}: Invalid Price Modifier Multiplier '{price_modifier_multiplier}' - defaulting to 1")
            price_modifier_multiplier = 1
            # return None
        
            
        # Create product dictionary
        product = {
            "PLU": plu,
            "Name": row.get("Display Text", "").strip(),
            "EAN": self.validator.safe_int_conversion(
                self.validator.normalize_ean(row.get("EAN Code", ""))
            ),
            "Price": normalized_price,
            "Department ID": self.validator.safe_int_conversion(row.get("Department ID", "")),
            "Text Area (1)": row.get("Text Area (1)", "").strip(),
            "Product Type": product_type,
            # "Price Modifier Multiplier": self.validator.safe_int_conversion(row.get("Price Modifier Multiplier", "1"), default=1),
            "Price Modifier Multiplier": price_modifier_multiplier,
            "Barcode Format ID": self.validator.safe_int_conversion(row.get("Barcode Format ID", "0"), default=0),
            "Print Format ID": self.validator.safe_int_conversion(row.get("Print Format ID", "0"), default=0),
            "_TS": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
        
        return product


class XMLManager:
    """Handles XML database operations."""
    
    def __init__(self, file_path: str):
        """
        Initialize XML manager with file path.
        
        Args:
            file_path (str): Path to the XML database file
        """
        self.file_path = Path(file_path)
        self.validator = DataValidator()
        self.tree = None
        self.root = None
        
    def load(self) -> Tuple[List[Dict], ET.ElementTree, ET.Element]:
        """
        Load products from XML database.
        
        Returns:
            Tuple[List[Dict], ET.ElementTree, ET.Element]: Products list, tree, and root
            
        Raises:
            ProductSyncError: If XML file cannot be loaded
        """
        if not self.file_path.exists():
            raise ProductSyncError(f"XML database file not found: {self.file_path}")
            
        logging.info(f"Loading XML database: {self.file_path}")
        
        try:
            self.tree = ET.parse(self.file_path)
            self.root = self.tree.getroot()
            
            products = self._extract_products()
            logging.info(f"Successfully loaded {len(products)} products from XML")
            
            return products, self.tree, self.root
            
        except ET.ParseError as e:
            raise ProductSyncError(f"Failed to parse XML file: {e}")
        except Exception as e:
            raise ProductSyncError(f"Failed to load XML file: {e}")
            
    def _extract_products(self) -> List[Dict]:
        """
        Extract products from XML tree.
        
        Returns:
            List[Dict]: List of product dictionaries from XML
        """
        products = []
        
        for record in self.root.findall(".//table[@name='ITEM']/record"):
            fields = {
                field.attrib["column_name"]: (field.text or "").strip() 
                for field in record.findall("field")
            }
            
            product = {
                "PLU": self.validator.safe_int_conversion(fields.get("PLU Number", "")),
                "Name": fields.get("Display Text", ""),
                "EAN": self.validator.safe_int_conversion(
                    self.validator.normalize_ean(fields.get("EAN Code", ""))
                ),
                "Price": self.validator.normalize_price(fields.get("Retail Price (1st)", "")),
                "Department ID": self.validator.safe_int_conversion(fields.get("Department ID", "")),
                "Text Area (1)": fields.get("Text Area (1)", ""),
                "Product Type": self.validator.safe_int_conversion(fields.get("Product Type", "")),
                "Price Modifier Multiplier": self.validator.safe_int_conversion(fields.get("Price Modifier Multiplier", "1"), default=1),
                "Barcode Format ID": self.validator.safe_int_conversion(fields.get("Barcode Format ID", "0"), default=0),
                "Print Format ID": self.validator.safe_int_conversion(fields.get("Print Format ID", "0"), default=0),
                "_TS": fields.get("_TS", ""),
            }
            products.append(product)
            
        return products
    
    def save(self, tree: ET.ElementTree, backup: bool = True) -> None:
        """
        Save XML tree to file with optional backup.
        
        Args:
            tree (ET.ElementTree): XML tree to save
            backup (bool): Whether to create backup before saving
            
        Raises:
            ProductSyncError: If save operation fails
        """
        try:
            if backup and self.file_path.exists():
                backup_path = self.file_path.with_suffix(f'.backup_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.xml')
                self.file_path.rename(backup_path)
                logging.info(f"Backup created: {backup_path}")
                
            # Format XML with proper indentation
            ET.indent(tree, space="  ", level=0)
            tree.write(self.file_path, encoding="utf-8", xml_declaration=True)
            logging.info(f"XML database saved successfully: {self.file_path}")
            
        except Exception as e:
            raise ProductSyncError(f"Failed to save XML file: {e}")


class ProductSynchronizer:
    """Main synchronization logic."""
    
    def __init__(self):
        """Initialize the synchronizer."""
        self.stats = {
            "added": 0,
            "updated": 0,
            "deleted": 0,
            "errors": 0
        }
        
    def sync(self, csv_products: List[Dict], xml_products: List[Dict], 
             tree: ET.ElementTree, root: ET.Element) -> Dict[str, int]:
        """
        Synchronize products from CSV to XML database.
        
        Args:
            csv_products (List[Dict]): Products from CSV file
            xml_products (List[Dict]): Products from XML database
            tree (ET.ElementTree): XML tree
            root (ET.Element): XML root element
            
        Returns:
            Dict[str, int]: Synchronization statistics
        """
        logging.info("Starting product synchronization")
        
        # Create lookup dictionaries using composite key (PLU, Department ID)
        csv_dict = {
            (p["PLU"], p["Department ID"]): p 
            for p in csv_products 
            if p["PLU"] and p["Department ID"]
        }
        
        xml_dict = {
            (p["PLU"], p["Department ID"]): p 
            for p in xml_products 
            if p["PLU"] and p["Department ID"]
        }
        
        logging.info(f"CSV products with valid keys: {len(csv_dict)}")
        logging.info(f"XML products with valid keys: {len(xml_dict)}")
        
        # Process updates and additions
        self._process_updates_and_additions(csv_dict, xml_dict, root)
        
        # Note: Deletions are commented out in original code - keeping same behavior
        # self._process_deletions(csv_dict, xml_dict, root)
        
        logging.info("Product synchronization completed")
        return self.stats.copy()
    
    def _process_updates_and_additions(self, csv_dict: Dict, xml_dict: Dict, root: ET.Element) -> None:
        """
        Process product updates and additions.
        
        Args:
            csv_dict (Dict): CSV products dictionary
            xml_dict (Dict): XML products dictionary  
            root (ET.Element): XML root element
                """
        for (plu, dept_id), csv_prod in csv_dict.items():
            try:
                # Ensure timestamp is added here
                csv_prod["_TS"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

                if (plu, dept_id) in xml_dict:
                    self._update_existing_product(plu, dept_id, csv_prod, root)
                else:
                    self._add_new_product(plu, dept_id, csv_prod, root)

            except Exception as e:
                logging.error(f"Error processing product PLU={plu}, Dept={dept_id}: {e}")
                self.stats["errors"] += 1
    
    def _update_existing_product(self, plu: int, dept_id: int, csv_prod: Dict, root: ET.Element) -> None:
        """
        Update existing product in XML.
        
        Args:
            plu (int): Product PLU number
            dept_id (int): Department ID
            csv_prod (Dict): Product data from CSV
            root (ET.Element): XML root element
        """
        for record in root.findall(".//table[@name='ITEM']/record"):
            fields = {f.attrib["column_name"]: f for f in record.findall("field")}
            
            if (self._match_product_record(fields, plu, dept_id)):
                updated = False
                
                # Define fields to update (excluding _TS from comparison)
                update_mappings = [
                    ("Display Text", "Name"),
                    ("EAN Code", "EAN"),
                    ("Retail Price (1st)", "Price"),
                    ("Text Area (1)", "Text Area (1)"),
                    ("Product Type", "Product Type"),
                    ("Price Modifier Multiplier", "Price Modifier Multiplier"),
                    ("Barcode Format ID", "Barcode Format ID"),
                    ("Print Format ID", "Print Format ID"),
                    ("Display Button Text", "Name")
                ]
                
                # Check if any data fields have changed
                for xml_col, csv_key in update_mappings:
                    if xml_col in fields:
                        old_val = fields[xml_col].text or ""
                        new_val = str(csv_prod[csv_key])
                        
                        if old_val != new_val:
                            logging.info(f"Updating {xml_col} for PLU={plu}, Dept={dept_id}: '{old_val}' -> '{new_val}'")
                            fields[xml_col].text = new_val
                            updated = True
                
                # Only update _TS if other fields were updated OR if _TS field doesn't exist
                if updated or "_TS" not in fields:
                    # Generate current timestamp only when needed
                    current_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    
                    if "_TS" not in fields:
                        # Create new _TS field if it doesn't exist
                        ts_field = ET.SubElement(record, "field", 
                                               column_name="_TS", 
                                               exclusion="false")
                        ts_field.text = current_ts
                        logging.info(f"Added _TS field for PLU={plu}, Dept={dept_id}: '{current_ts}'")
                    else:
                        # Update existing _TS field only if other data changed
                        fields["_TS"].text = current_ts
                        logging.info(f"Updated _TS for PLU={plu}, Dept={dept_id}: '{current_ts}'")
                
                if updated:
                    logging.info(f"Product updated: PLU={plu}, Dept={dept_id}")
                    self.stats["updated"] += 1
                break
    
    def _add_new_product(self, plu: int, dept_id: int, csv_prod: Dict, root: ET.Element) -> None:
        """
        Add new product to XML database.
        
        Args:
            plu (int): Product PLU number
            dept_id (int): Department ID
            csv_prod (Dict): Product data from CSV
            root (ET.Element): XML root element
        """
        # Get default field values for new products
        field_defaults = self._get_default_field_values(csv_prod, plu, dept_id)
        
        # Create new record element
        item_table = root.find(".//table[@name='ITEM']")
        if item_table is None:
            raise ProductSyncError("ITEM table not found in XML structure")
            
        new_record = ET.SubElement(item_table, "record")
        
        # Add all fields to the new record
        for column_name, value in field_defaults.items():
            field_element = ET.SubElement(new_record, "field", 
                                        column_name=column_name, 
                                        exclusion="false")
            field_element.text = str(value)
        
        logging.info(f"Product added: PLU={plu}, Dept={dept_id}, Name='{csv_prod['Name']}'")
        self.stats["added"] += 1
    
    def _match_product_record(self, fields: Dict, plu: int, dept_id: int) -> bool:
        """
        Check if XML record matches the given PLU and Department ID.
        
        Args:
            fields (Dict): XML record fields
            plu (int): PLU number to match
            dept_id (int): Department ID to match
            
        Returns:
            bool: True if record matches
        """
        return (
            fields.get("PLU Number") is not None and
            fields.get("Department ID") is not None and
            fields["PLU Number"].text and
            fields["Department ID"].text and
            fields["PLU Number"].text.strip() == str(plu) and
            fields["Department ID"].text.strip() == str(dept_id)
        )
    
    def _get_default_field_values(self, csv_prod: Dict, plu: int, dept_id: int) -> Dict[str, str]:
        """
        Get default field values for new product records.
        
        Args:
            csv_prod (Dict): Product data from CSV
            plu (int): PLU number
            dept_id (int): Department ID
            
        Returns:
            Dict[str, str]: Dictionary of field names and default values
        """
        return {
            "Text Area (1)": csv_prod["Text Area (1)"],
            "Cost Price": "0",
            "Display Text": csv_prod["Name"],
            "EAN Code": str(csv_prod["EAN"]),
            "GTIN": "0",
            "Retail Price (1st)": csv_prod["Price"],
            "PLU Number": str(plu),
            "Container ID": "0",
            "Department ID": str(dept_id),
            "Product Type": str(csv_prod["Product Type"]),
            "Margin": "100",
            "Barcode Print Control": "0",
            "Barcode Format ID": str(csv_prod["Barcode Format ID"]),
            "Sales Only ITEM": "0",
            "Scale ITEM Type": "0",
            "Container Tare Type": "0",
            "Nominal Weight Value": "0",
            "Proportional Tare Value": "0",
            "Nominal Volume": "0",
            "Date Offset (1)": "0",
            "Date Offset (2)": "0",
            "Date Print Control (1)": "0",
            "Date Print Control (2)": "0",
            "Text Area (2)": "",
            "Text Area (3)": "",
            "Text Area (4)": "",
            "Text Area (5 Serving Size Description)": "",
            "Text Area (6 Servings Per Description)": "",
            "Message ID (1)": "0",
            "Message ID (2)": "0",
            "Message Category ID (1)": "14",
            "Message Category ID (2)": "14",
            "Discount Percentage (1)": "0",
            "Items Free (1)": "0",
            "ITEM Discount Type": "1",
            "Promotion Control": "0",
            "Print Promotion Message": "0",
            "Promotion Type": "0",
            "Promotion Voucher Id": "0",
            "Retail Price (2nd / Freq Shopper Alternate Price)": "0",
            "Message ID (Promotion Message)": "0",
            "Time Period ID": "0",
            "Voucher Amount": "0",
            "Weight Free (1)": "0",
            "Discount Amount Money Off (1)": "0",
            "Weight Break Quantity (1)": "0",
            "Weight Break Quantity (2)": "0",
            "Item Break Quantity (1)": "0",
            "Item Break Quantity (2)": "0",
            "Item Promotion Quantity Limit": "0",
            "Weight Promotion Quantity Limit": "0",
            "Promotion Transaction Limit": "0",
            "Retail Price (Break 1)": "0",
            "Retail Price (Break 2)": "0",
            "Keyboard ID (Dynamic 1)": "0",
            "Group ID": "0",
            "Information Voucher Id": "0",
            "Print Format ID": str(csv_prod["Print Format ID"]),
            "Print Format Type Control": "0",
            "Print Format ID (Nutritional Label)": "100",
            "Media ID (1)": "0",
            "ITEM Logo Control": "0",
            "ITEM Logo Promotion Mode": "0",
            "ITEM Logo Type": "0",
            "Interactive Traceability Mode": "0",
            "Traceability Linked ITEM": "0",
            "Traceability ITEM": "0",
            "Traceability Scheme Id": "1",
            "Negative By Count": "0",
            "Tax Rate ID (Primary)": "0",
            "Tax Rate ID (Secondary)": "0",
            "Price Modifier Multiplier": str(csv_prod["Price Modifier Multiplier"]),
            "Price Modifier Divider": "1",
            "Message Category ID (Promotion Message)": "14",
            #       <field column_name="_TS" exclusion="false">2025-08-18T09:30:05</field>
            "_TS": str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
            "_CF": "1",
            "Display Button Text": csv_prod["Name"],
        }


def setup_logging() -> None:
    """
    Configure logging for the application.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=Config.LOG_FORMAT,
        datefmt=Config.LOG_DATE_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),  # Console output for bat script
        ]
    )


def print_summary_statistics(csv_count: int, xml_count: int, stats: Dict[str, int], duration: datetime.timedelta) -> None:
    """
    Print comprehensive summary statistics.
    
    Args:
        csv_count (int): Number of products in CSV
        xml_count (int): Number of products in XML
        stats (Dict[str, int]): Synchronization statistics
        duration (datetime.timedelta): Execution duration
    """
    print("\n" + "="*60)
    print("SYNCHRONIZATION SUMMARY")
    print("="*60)
    print(f"CSV Products (Source):     {csv_count:,}")
    print(f"XML Products (Target):     {xml_count:,}")
    print(f"Products Added:            {stats['added']:,}")
    print(f"Products Updated:          {stats['updated']:,}")
    print(f"Products Deleted:          {stats['deleted']:,}")
    print(f"Errors Encountered:        {stats['errors']:,}")
    print(f"Execution Duration:        {duration}")
    print(f"Completion Time:           {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


def main() -> int:
    """
    Main execution function.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    setup_logging()
    
    start_time = datetime.datetime.now()
    logging.info(f"=== Product Sync Script Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    try:
        # Initialize components
        csv_loader = CSVLoader(Config.CSV_FILE)
        xml_manager = XMLManager(Config.XML_FILE)
        synchronizer = ProductSynchronizer()
        
        # Load data
        logging.info("Phase 1: Loading data files")
        csv_products = csv_loader.load()
        xml_products, tree, root = xml_manager.load()
        
        # Perform synchronization
        logging.info("Phase 2: Synchronizing products")
        stats = synchronizer.sync(csv_products, xml_products, tree, root)
        
        # Save results
        logging.info("Phase 3: Saving updated database")
        xml_manager.save(tree, backup=True)
        
        # Calculate duration and print summary
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        
        print_summary_statistics(len(csv_products), len(xml_products), stats, duration)
        
        logging.info("=== Product Sync Script Completed Successfully ===")
        return 0
        
    except ProductSyncError as e:
        logging.error(f"Sync operation failed: {e}")
        return 1
    except KeyboardInterrupt:
        logging.warning("Operation cancelled by user")
        return 1
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}", exc_info=True)
        return 1
    finally:
        end_time = datetime.datetime.now()
        total_duration = end_time - start_time
        logging.info(f"Script execution finished. Total duration: {total_duration}")


if __name__ == "__main__":
    sys.exit(main())