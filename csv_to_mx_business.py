#!/usr/bin/env python3
"""
Product Synchronization Script
==============================

This script synchronizes product data from a CSV file to an XML database.
It handles adding new products, updating existing ones, validates data integrity,
and logs products in XML not found in CSV as needing deletion.

Author: Qias Technology
Version: 1.0.1
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
    
    CSV_FILE = "carrefour_test3.csv"
    XML_FILE = r"c:\ProgramData\Avery Berkel\MXBusiness\DEFAULT_5.4.5.3503\Project\MXBusiness - 638907826887926093\Data\Database\database.xml"
    CSV_ENCODING = "utf-8-sig"
    # CSV_ENCODING = "latin-1"
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
        if not value or not value.strip():
            return "0.00"
        cleaned_value = re.sub(r"[^\d.,-]", "", value.strip())
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
        if not value or not value.strip():
            return "0"
        try:
            return str(int(float(value)))
        except (ValueError, OverflowError):
            digits_only = re.sub(r"\D", "", str(value))
            return digits_only if digits_only else "0"

    @staticmethod
    def safe_int_conversion(value: str, default: int = 0) -> int:
        try:
            return int(value.strip()) if value and value.strip() else default
        except (ValueError, TypeError, AttributeError):
            return default

    @staticmethod
    def validate_price_range(price: str) -> bool:
        try:
            price_val = float(price)
            return Config.MIN_PRICE <= price_val <= Config.MAX_PRICE
        except (ValueError, TypeError):
            return False


class CSVLoader:
    """Handles loading and parsing CSV files."""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.validator = DataValidator()
        
    def load(self) -> List[Dict]:
        if not self.file_path.exists():
            raise ProductSyncError(f"CSV file not found: {self.file_path}")
        logging.info(f"Loading CSV file: {self.file_path}")
        products = []
        skipped_count = 0
        try:
            with open(self.file_path, newline="", encoding=Config.CSV_ENCODING) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=Config.CSV_DELIMITER)
                for row_num, row in enumerate(reader, start=2):
                    try:
                        # print(row)
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
        raw_price = row.get("Retail Price (1st)", "").strip()
        normalized_price = self.validator.normalize_price(raw_price)
        if not self.validator.validate_price_range(normalized_price):
            logging.warning(f"Row {row_num}: Invalid price '{raw_price}' - skipping")
            return None
        plu = self.validator.safe_int_conversion(row.get("PLU Number", ""))
        if plu == 0:
            logging.warning(f"Row {row_num}: Missing or invalid PLU - skipping")
            return None
        product_type = self.validator.safe_int_conversion(row.get("Product Type", ""))
        if product_type not in [0, 1, 2, 4, 6, 9, 99]:
            logging.warning(f"Row {row_num}: Invalid Product Type '{product_type}' - skipping")
            return None
        price_modifier_multiplier = self.validator.safe_int_conversion(row.get("Price Modifier Multiplier", "1"), default=1)
        if price_modifier_multiplier < 1 or price_modifier_multiplier > 100:
            logging.warning(f"Row {row_num}: Invalid Price Modifier Multiplier '{price_modifier_multiplier}' - defaulting to 1")
            price_modifier_multiplier = 1
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
            "Price Modifier Multiplier": price_modifier_multiplier,
            "Barcode Format ID": self.validator.safe_int_conversion(row.get("Barcode Format ID", "0"), default=0),
            "Print Format ID": self.validator.safe_int_conversion(row.get("Print Format ID", "0"), default=0),
            "_TS": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
        return product


class XMLManager:
    """Handles XML database operations."""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.validator = DataValidator()
        self.tree = None
        self.root = None
        
    def load(self) -> Tuple[List[Dict], ET.ElementTree, ET.Element]:
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
                "_TS": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            products.append(product)
        return products
    
    def save(self, tree: ET.ElementTree, backup: bool = True) -> None:
        try:
            if backup and self.file_path.exists():
                backup_path = self.file_path.with_suffix(f'.backup_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.xml')
                self.file_path.rename(backup_path)
                logging.info(f"Backup created: {backup_path}")
            ET.indent(tree, space="  ", level=0)
            tree.write(self.file_path, encoding="utf-8", xml_declaration=True)
            logging.info(f"XML database saved successfully: {self.file_path}")
        except Exception as e:
            raise ProductSyncError(f"Failed to save XML file: {e}")


class ProductSynchronizer:
    """Main synchronization logic."""
    
    def __init__(self):
        self.stats = {
            "added": 0,
            "updated": 0,
            "deleted": 0,
            "errors": 0
        }
        
    def sync(self, csv_products: List[Dict], xml_products: List[Dict], 
             tree: ET.ElementTree, root: ET.Element) -> Dict[str, int]:
        logging.info("Starting product synchronization")
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
        self._process_deletions(csv_dict, xml_dict)
        self._process_updates_and_additions(csv_dict, xml_dict, root)
        logging.info("Product synchronization completed")
        return self.stats.copy()
    
    def _process_updates_and_additions(self, csv_dict: Dict, xml_dict: Dict, root: ET.Element) -> None:
        for (plu, dept_id), csv_prod in csv_dict.items():
            try:
                if (plu, dept_id) in xml_dict:
                    self._update_existing_product(plu, dept_id, csv_prod, root)
                else:
                    csv_prod["_TS"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    self._add_new_product(plu, dept_id, csv_prod, root)
            except Exception as e:
                logging.error(f"Error processing product PLU={plu}, Dept={dept_id}: {e}")
                self.stats["errors"] += 1
    
    def _process_deletions(self, csv_dict: Dict, xml_dict: Dict) -> None:
        """
        Log products in XML not found in CSV as needing deletion.
        
        Args:
            csv_dict (Dict): CSV products dictionary
            xml_dict (Dict): XML products dictionary
        """
        for (plu, dept_id), xml_prod in xml_dict.items():
            try:
                if (plu, dept_id) not in csv_dict:
                    logging.info(f"Product not found in CSV, should be deleted: PLU={plu}, Dept={dept_id}, Name='{xml_prod['Name']}'")
                    self.stats["deleted"] += 1
            except Exception as e:
                logging.error(f"Error processing deletion for PLU={plu}, Dept={dept_id}: {e}")
                self.stats["errors"] += 1
    
    def _update_existing_product(self, plu: int, dept_id: int, csv_prod: Dict, root: ET.Element) -> None:
        for record in root.findall(".//table[@name='ITEM']/record"):
            fields = {f.attrib["column_name"]: f for f in record.findall("field")}
            if (self._match_product_record(fields, plu, dept_id)):
                updated = False
                price_updated = False
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
                for xml_col, csv_key in update_mappings:
                    if xml_col in fields:
                        old_val = fields[xml_col].text or ""
                        new_val = str(csv_prod[csv_key])
                        if old_val != new_val:
                            logging.info(f"Updating {xml_col} for PLU={plu}, Dept={dept_id}: '{old_val}' -> '{new_val}'")
                            fields[xml_col].text = new_val
                            updated = True
                            if xml_col == "Retail Price (1st)":
                                price_updated = True
                if updated or "_TS" not in fields:
                    current_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    if "_TS" not in fields:
                        ts_field = ET.SubElement(record, "field", 
                                               column_name="_TS", 
                                               exclusion="false")
                        ts_field.text = current_ts
                        logging.info(f"Added _TS field for PLU={plu}, Dept={dept_id}: '{current_ts}'")
                    else:
                        fields["_TS"].text = current_ts
                        logging.info(f"Updated _TS for PLU={plu}, Dept={dept_id}: '{current_ts}'")
                if updated and "_CF" not in fields:
                    cf_field = ET.SubElement(record, "field", 
                                           column_name="_CF", 
                                           exclusion="false")
                    cf_field.text = "1"
                    logging.info(f"Added _CF field for PLU={plu}, Dept={dept_id}: '1'")
                elif updated and "_CF" in fields:
                    fields["_CF"].text = "1"
                    logging.info(f"Updated _CF for PLU={plu}, Dept={dept_id}: '1'")
                if price_updated:
                    self._update_item_in_band(plu, dept_id, csv_prod["Price"], root)
                if updated:
                    logging.info(f"Product updated: PLU={plu}, Dept={dept_id}")
                    self.stats["updated"] += 1
                break
    
    def _add_new_product(self, plu: int, dept_id: int, csv_prod: Dict, root: ET.Element) -> None:
        field_defaults = self._get_default_field_values(csv_prod, plu, dept_id)
        item_table = root.find(".//table[@name='ITEM']")
        if item_table is None:
            raise ProductSyncError("ITEM table not found in XML structure")
        new_record = ET.SubElement(item_table, "record")
        for column_name, value in field_defaults.items():
            field_element = ET.SubElement(new_record, "field", 
                                        column_name=column_name, 
                                        exclusion="false")
            field_element.text = str(value)
        self._add_item_in_band(plu, dept_id, csv_prod["Price"], root)
        logging.info(f"Product added: PLU={plu}, Dept={dept_id}, Name='{csv_prod['Name']}'")
        self.stats["added"] += 1
    
    def _match_product_record(self, fields: Dict, plu: int, dept_id: int) -> bool:
        return (
            fields.get("PLU Number") is not None and
            fields.get("Department ID") is not None and
            fields["PLU Number"].text and
            fields["Department ID"].text and
            fields["PLU Number"].text.strip() == str(plu) and
            fields["Department ID"].text.strip() == str(dept_id)
        )
    
    def _update_item_in_band(self, plu: int, dept_id: int, new_price: str, root: ET.Element) -> None:
        current_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for record in root.findall(".//table[@name='ITEM in Band']/record"):
            fields = {f.attrib["column_name"]: f for f in record.findall("field")}
            plu_field = fields.get("PLU Number")
            dept_field = fields.get("Department ID")
            if (plu_field is not None and dept_field is not None and
                plu_field.text and dept_field.text and
                plu_field.text.strip() == str(plu) and
                dept_field.text.strip() == str(dept_id)):
                if "Retail Price (1st)" in fields:
                    old_price = fields["Retail Price (1st)"].text or "0"
                    if old_price != new_price:
                        fields["Retail Price (1st)"].text = new_price
                        logging.info(f"Updated ITEM in Band price for PLU={plu}, Dept={dept_id}: '{old_price}' -> '{new_price}'")
                if "_TS" in fields:
                    fields["_TS"].text = current_ts
                else:
                    ts_field = ET.SubElement(record, "field", 
                                           column_name="_TS", 
                                           exclusion="false")
                    ts_field.text = current_ts
                if "_CF" in fields:
                    fields["_CF"].text = "1"
                else:
                    cf_field = ET.SubElement(record, "field", 
                                           column_name="_CF", 
                                           exclusion="false")
                    cf_field.text = "1"
                logging.info(f"Updated ITEM in Band _TS for PLU={plu}, Dept={dept_id}: '{current_ts}' _CF= 1'")
                
                return
        logging.warning(f"No ITEM in Band record found for PLU={plu}, Dept={dept_id}")
    
    def _add_item_in_band(self, plu: int, dept_id: int, price: str, root: ET.Element) -> None:
        current_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        item_in_band_table = root.find(".//table[@name='ITEM in Band']")
        if item_in_band_table is None:
            logging.warning("ITEM in Band table not found - skipping band record creation")
            return
        new_record = ET.SubElement(item_in_band_table, "record")
        band_fields = {
            "Band ID": "0",
            "Department ID": str(dept_id),
            "PLU Number": str(plu),
            "Retail Price (1st)": price,
            "Retail Price (Break 1)": "0",
            "Retail Price (Break 2)": "0",
            "Retail Price (Break 3)": "0",
            "Retail Price (2nd / Freq Shopper Alternate Price)": "0",
            "_TS": current_ts,
            "_CF": "1"
        }
        for column_name, value in band_fields.items():
            field_element = ET.SubElement(new_record, "field", 
                                        column_name=column_name, 
                                        exclusion="false")
            field_element.text = value
        logging.info(f"Added ITEM in Band record: PLU={plu}, Dept={dept_id}, Price={price}")
    
    def _get_default_field_values(self, csv_prod: Dict, plu: int, dept_id: int) -> Dict[str, str]:
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
            "_TS": str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
            "_CF": "1",
            "Display Button Text": csv_prod["Name"],
        }


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=Config.LOG_FORMAT,
        datefmt=Config.LOG_DATE_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def print_summary_statistics(csv_count: int, xml_count: int, stats: Dict[str, int], duration: datetime.timedelta) -> None:
    print("\n" + "="*60)
    print("SYNCHRONIZATION SUMMARY")
    print("="*60)
    print(f"CSV Products (Source):     {csv_count:,}")
    print(f"XML Products (Target):     {xml_count:,}")
    print(f"Products Added:            {stats['added']:,}")
    print(f"Products Updated:          {stats['updated']:,}")
    print(f"Products to be Deleted:    {stats['deleted']:,}")
    print(f"Errors Encountered:        {stats['errors']:,}")
    print(f"Execution Duration:        {duration}")
    print(f"Completion Time:           {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


def main() -> int:
    setup_logging()
    start_time = datetime.datetime.now()
    logging.info(f"=== Product Sync Script Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
    try:
        csv_loader = CSVLoader(Config.CSV_FILE)
        xml_manager = XMLManager(Config.XML_FILE)
        synchronizer = ProductSynchronizer()
        logging.info("Phase 1: Loading data files")
        csv_products = csv_loader.load()
        xml_products, tree, root = xml_manager.load()
        logging.info("Phase 2: Synchronizing products")
        stats = synchronizer.sync(csv_products, xml_products, tree, root)
        logging.info("Phase 3: Saving updated database")
        xml_manager.save(tree, backup=True)
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
