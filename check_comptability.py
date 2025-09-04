import csv
import xml.etree.ElementTree as ET

def get_xml_columns(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    # Find the first <record> under <table name="ITEM">
    record = root.find(".//table[@name='ITEM']/record")
    if record is None:
        raise ValueError("No <record> found under <table name='ITEM'>")

    # Extract all column_name attributes
    return [field.attrib["column_name"] for field in record.findall("field")]

def get_csv_header(csv_file):
    with open(csv_file, newline='', encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
    return header

def compare_headers(xml_file, csv_file):
    xml_columns = get_xml_columns(xml_file)
    csv_header = get_csv_header(csv_file)

    
    # print("CSV Header:", csv_header)
    # print("XML Columns:", xml_columns)

    print("Comparing CSV header with XML columns...")

    # Check if each of the csv header columns exists in xml_columns
    not_found = False
    for col in csv_header:
        if col not in xml_columns:
            print(f"Column '{col}' from CSV header not found in XML column_name.")
            not_found = True
    if not not_found:
        print("All CSV header columns found in XML column_name.")

    print("Comparison complete.")

# Example usage:
compare_headers("database.xml", "carrefour_test.csv")
