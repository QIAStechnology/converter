import csv
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation

CSV_FILE = "articles.csv"
XML_FILE = "database.xml"


import csv
import re
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation

CSV_FILE = "articles.csv"
XML_FILE = "database.xml"


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
        # Skip the first line ("ITEM:PLU...")
        next(csvfile)

        reader = csv.DictReader(csvfile)  # now the 2nd line is the header
        i = 0
        for row in reader:
            product = {
                "PLU": row.get("PLU Number:PLU", "").strip(),
                "Name": row.get("Display Text:DYT", "").strip(),
                "EAN": normalize_ean(row.get("EAN Code:EAN", "").strip()),
                "Price": normalize_price(row.get("Standard Price:P1", "").strip()),
            }
            products.append(product)
            i += 1
            if i >= 10:
                break
    return products


def load_xml(file_path):
    print(f"Loading XML database: {file_path}")
    products = []
    tree = ET.parse(file_path)
    root = tree.getroot()
    i = 0
    for record in root.findall(".//table[@name='ITEM']/record"):
        fields = {f.attrib["column_name"]: (f.text or "").strip() for f in record.findall("field")}
        product = {
            "PLU": fields.get("PLU Number", ""),
            "Name": fields.get("Display Text", ""),
            "EAN": normalize_ean(fields.get("EAN Code", "")),
            "Price": normalize_price(fields.get("Retail Price (1st)", "")),
        }
        products.append(product)
        i += 1
        if i >= 10:
            break
    return products


def save_xml(path, tree):
    tree.write(path, encoding="utf-8", xml_declaration=True)


def sync_products(csv_products, xml_products, tree, root):
    csv_dict = {p["PLU"]: p for p in csv_products if p["PLU"]}
    xml_dict = {p["PLU"]: p for p in xml_products if p["PLU"]}

    # Check products from CSV (source)
    for plu, csv_prod in csv_dict.items():
        if plu in xml_dict:
            # Update existing record in XML if any field differs
            for record in root.findall(".//table[@name='ITEM']/record"):
                fields = {f.attrib["column_name"]: f for f in record.findall("field")}
                if fields.get("PLU Number") is not None and fields["PLU Number"].text.strip() == plu:
                    updated = False
                    for col, key in [
                        ("Display Text", "Name"),
                        ("EAN Code", "EAN"),
                        ("Retail Price (1st)", "Price"),
                    ]:
                        old_val = fields[col].text or ""
                        new_val = csv_prod[key]
                        if old_val != new_val:
                            print(f"✏️ Updating {col} for PLU={plu}: {old_val} → {new_val}")
                            fields[col].text = new_val
                            updated = True
                    if updated:
                        print(f"✅ Product {plu} updated in XML")
        else:
            # Add new record if PLU from CSV is missing in XML
            new_record = ET.SubElement(root.find(".//table[@name='ITEM']"), "record")
            ET.SubElement(new_record, "field", column_name="PLU Number").text = plu
            ET.SubElement(new_record, "field", column_name="Display Text").text = csv_prod["Name"]
            ET.SubElement(new_record, "field", column_name="EAN Code").text = csv_prod["EAN"]
            ET.SubElement(new_record, "field", column_name="Retail Price (1st)").text = csv_prod["Price"]
            print(f"➕ Added product {plu} to XML")

    # Check products from XML that are missing in CSV
    for plu in xml_dict:
        if plu not in csv_dict:
            print(f"➖ PLU={plu} exists in XML but missing in CSV (no deletion performed)")

    # Save changes back to file
    tree.write(XML_FILE, encoding="utf-8", xml_declaration=True)
    print("💾 XML database updated.")


# def sync_products(csv_products, xml_products):
#     csv_dict = {p["PLU"]: p for p in csv_products if p["PLU"]}
#     xml_dict = {p["PLU"]: p for p in xml_products if p["PLU"]}

#     for plu, csv_prod in csv_dict.items():
#         if plu in xml_dict:
#             xml_prod = xml_dict[plu]
#             # Compare field by field
#             for key, label in [("Name", "Display Text"),
#                                ("EAN", "EAN Code"),
#                                ("Price", "Retail Price")]:
#                 if csv_prod[key] != xml_prod[key]:
#                     print(f"⚠️ Difference for PLU={plu} [{label}]: "
#                           f"CSV='{csv_prod[key]}' vs XML='{xml_prod[key]}'")
#         else:
#             print(f"➕ PLU={plu} exists in CSV but missing in XML")

#     for plu in xml_dict.keys():
#         if plu not in csv_dict:
#             print(f"➖ PLU={plu} exists in XML but missing in CSV")


def main():
    csv_products = load_csv(CSV_FILE)
    xml_products = load_xml(XML_FILE)
    tree = ET.parse(XML_FILE)
    root = tree.getroot()

    print("📂 Products in CSV (Source):")
    for p in csv_products[:10]:
        print(f"PLU={p['PLU']}, Name={p['Name']}, EAN={p['EAN']}, Price={p['Price']}")

    print("\n📂 Products in XML (Target):")
    for p in xml_products[:10]:
        print(f"PLU={p['PLU']}, Name={p['Name']}, EAN={p['EAN']}, Price={p['Price']}")

    sync_products(csv_products, xml_products, tree, root)
    print("🔄 Sync complete.")


if __name__ == "__main__":
    main()
