import csv
import datetime
import re
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation

CSV_FILE = "carrefour_test.csv"
# XML_FILE = "databaseSafe.xml"
XML_FILE = "c:\ProgramData\Avery Berkel\MXBusiness\DEFAULT_5.4.5.3503\Project\MXBusiness - 638907826887926093\Data\Database\database.xml"


def normalize_price(value: str) -> str:
    """Remove currency symbols and normalize to plain decimal string."""
    if not value:
        return "0.00"

    # Remove any currency sign or non-numeric except . , and -
    value = re.sub(r"[^\d.,-]", "", value).strip()

    # Replace comma decimal with dot (e.g. 1,25 -> 1.25)
    if "," in value and "." not in value:
        value = value.replace(",", ".")

    try:
        return f"{Decimal(value):.2f}"  # always 2 decimal places
    except InvalidOperation:
        return "0.00"


def normalize_ean(value: str) -> str:
    """Convert scientific notation or float to integer-like string (digits only)."""
    if not value:
        return "0"
    try:
        # Convert "2.52E+12" → "2520000000000"
        return str(int(float(value)))
    except (ValueError, OverflowError):
        return re.sub(r"\D", "", value)  # keep only digits if malformed


def load_csv(file_path):
    print(f"Loading CSV file: {file_path}")
    products = []
    with open(file_path, newline="", encoding="latin-1") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")  
        for row in reader:
            price = normalize_price(row.get("Retail Price (1st)", "").strip())
            
            try:
                price_val = float(price)
            except ValueError:
                print(f"Skipping product with invalid price format: {row}")
                continue

            # Range validation
            if not (0 <= price_val <= 999999.99):
                print(f"Skipping product with out-of-range price {price_val}: {row}")
                continue

            # Parse numeric fields safely
            def safe_int(value):
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0

            product = {
                "PLU": safe_int(row.get("PLU Number", "").strip()),
                "Name": row.get("Display Text", "").strip(),
                "EAN": safe_int(normalize_ean(row.get("EAN Code", "").strip())),
                "Price": f"{price_val:.2f}",
                "Department ID": safe_int(row.get("Department ID", "").strip()),
                "Text Area (1)": row.get("Text Area (1)", "").strip(),
            }

            if product["PLU"] == 0:
                print(f"Skipping product with empty PLU: {product}")
                continue
            products.append(product)

    return products




def load_xml(file_path):
    print(f"Loading XML database: {file_path}")
    products = []
    tree = ET.parse(file_path)
    root = tree.getroot()

    def safe_int(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    for record in root.findall(".//table[@name='ITEM']/record"):
        fields = {f.attrib["column_name"]: (f.text or "").strip() for f in record.findall("field")}
        product = {
            "PLU": safe_int(fields.get("PLU Number", "")),
            "Name": fields.get("Display Text", ""),
            "EAN": safe_int(normalize_ean(fields.get("EAN Code", ""))),
            "Price": normalize_price(fields.get("Retail Price (1st)", "")),
            "Department ID": safe_int(fields.get("Department ID", "")),
            "Text Area (1)": fields.get("Text Area (1)", ""),
        }
        products.append(product)

    return products



def save_xml(path, tree):
    tree.write(path, encoding="utf-8", xml_declaration=True)




def sync_products(csv_products, xml_products, tree, root, xml_file=XML_FILE):
    # Use (PLU, Department ID) as composite key
    csv_dict = {(p["PLU"], p["Department ID"]): p for p in csv_products if p["PLU"] and p["Department ID"]}
    xml_dict = {(p["PLU"], p["Department ID"]): p for p in xml_products if p["PLU"] and p["Department ID"]}

    added_count = 0
    updated_count = 0
    deleted_count = 0

    # === UPDATE + ADD ===
    for (plu, dept_id), csv_prod in csv_dict.items():
        if (plu, dept_id) in xml_dict:
            # Update record if it exists
            for record in root.findall(".//table[@name='ITEM']/record"):
                fields = {f.attrib["column_name"]: f for f in record.findall("field")}
                if (
                    fields.get("PLU Number") is not None
                    and fields.get("Department ID") is not None
                    and fields["PLU Number"].text.strip() == plu
                    and fields["Department ID"].text.strip() == dept_id
                ):
                    updated = False
                    for col, key in [
                        ("Display Text", "Name"),
                        ("EAN Code", "EAN"),
                        ("Retail Price (1st)", "Price"),
                        ("Text Area (1)", "Text Area (1)"),
                    ]:
                        old_val = fields[col].text or ""
                        new_val = csv_prod[key]
                        if old_val != new_val:
                            msg = f" Updating {col} for PLU={plu}, Dept={dept_id}: {old_val} ::: {new_val}"
                            print(msg)
                            # logging.info(msg)
                            fields[col].text = new_val
                            updated = True

                    if updated:
                        msg = f" Product (PLU={plu}, Dept={dept_id}) updated in XML"
                        print(msg)
                        updated_count += 1
                        # logging.info(msg)
        else:
            # Define full set of fields with defaults
            field_defaults = {
                "Text Area (1)": csv_prod["Text Area (1)"],
                "Cost Price": "0",
                "Display Text": csv_prod["Name"],
                "EAN Code": csv_prod["EAN"],
                "GTIN": "0",
                "Retail Price (1st)": csv_prod["Price"],
                "PLU Number": plu,
                "Container ID": "0",
                "Department ID": dept_id,
                "Product Type": "0",
                "Margin": "100",
                "Barcode Print Control": "0",
                "Barcode Format ID": "0",
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
                "Print Format ID": "0",
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
                "Price Modifier Multiplier": "1",
                "Price Modifier Divider": "1",
                "Message Category ID (Promotion Message)": "14",
                # "_TS": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "_CF": "1",
                "Display Button Text": csv_prod["Name"],  
            }

            # Create new record
            new_record = ET.SubElement(root.find(".//table[@name='ITEM']"), "record")

            # Insert all fields with exclusion="false"
            for col, value in field_defaults.items():
                ET.SubElement(new_record, "field", column_name=col, exclusion="false").text = value

            msg = f" Added product (PLU={plu}, Dept={dept_id}) with full schema to XML"
            print(msg)
            # logging.info(msg)
            added_count += 1


    # === DELETE ===
    # for (plu, dept_id), xml_prod in list(xml_dict.items()):
    #     if (plu, dept_id) not in csv_dict:
    #         for record in root.findall(".//table[@name='ITEM']/record"):
    #             fields = {f.attrib["column_name"]: f for f in record.findall("field")}
    #             if (
    #                 fields.get("PLU Number") is not None
    #                 and fields.get("Department ID") is not None
    #                 and fields["PLU Number"].text.strip() == plu
    #                 and fields["Department ID"].text.strip() == dept_id
    #             ):
    #                 root.find(".//table[@name='ITEM']").remove(record)
    #                 msg = f" Deleted product (PLU={plu}, Dept={dept_id}) from XML"
    #                 print(msg)
    #                 # logging.info(msg)
    #                 deleted_count += 1

    # Save XML after sync
    ET.indent(tree, space="  ", level=0)  # 2 spaces indentation
    tree.write(xml_file, encoding="utf-8", xml_declaration=True)

    summary = f"Summary : Added: {added_count}, Updated: {updated_count}"
    print(summary)
    # logging.info(summary)




def main():
    csv_products = load_csv(CSV_FILE)
    xml_products = load_xml(XML_FILE)
    tree = ET.parse(XML_FILE)
    root = tree.getroot()

    print(" Products in CSV (Source):")
    print(f"Total products in CSV: {len(csv_products)}")
    # for p in csv_products[:10]:
    #     print(f"PLU={p['PLU']}, Name={p['Name']}, EAN={p['EAN']}, Price={p['Price']}, Dept={p['Department ID']}, TextArea1={p['Text Area (1)']}")

    print("\n Products in XML (Target):")
    print(f"Total products in XML: {len(xml_products)}")
    # for p in xml_products[:10]:
    #     print(f"PLU={p['PLU']}, Name={p['Name']}, EAN={p['EAN']}, Price={p['Price']}, Dept={p['Department ID']}, TextArea1={p['Text Area (1)']}")

    start_time = datetime.datetime.now()
    print("\n Starting sync at", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    sync_products(csv_products, xml_products, tree, root)
    print(" Sync complete.")
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    print(f" Duration: {duration}")

if __name__ == "__main__":
    main()
