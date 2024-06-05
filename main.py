import pandas as pd

class DataExtractor:
  def __init__(self):
    self.expired_invoices = self.get_expired_invoices()
    self.flat_rows = []

  def clean_invoice_id(self, invoice_id):
    digits = [char for char in str(invoice_id) if char.isdigit()]
    return int(''.join(digits))

  def get_expired_invoices(self):
    expired_invoices = []
    with open("./expired_invoices.txt") as file:
      expired_invoices = file.readline().split(",")
    expired_invoices = [int(string.strip()) for string in expired_invoices]
    return expired_invoices

  def get_invoices(self):
    invoices = pd.read_pickle("./invoices_new.pkl")
    return invoices

  def resolve_item_type(self, type):
    if type == 0:
      return "Material"
    elif type == 1:
      return "Equipment"
    elif type == 2:
      return "Service"
    elif type == 3:
      return "Other"
    else:
      return "Invalid"

  def get_quantity(self, quantity):
    if type(quantity) is int: 
      return quantity
    else: 
      return self.number_word_to_int(quantity)

  # This is not a really great solution, but given the time limit and the fact
  # that the anomalies in the data are not that many, I will just use this function.
  def number_word_to_int(self, word):
    word = word.lower()
    if word == "one":
      return 1
    elif word == "two":
      return 2
    elif word == "three":
      return 3
    elif word == "four":
      return 4
    elif word == "five":
      return 5
    elif word == "six":
      return 6
    elif word == "seven":
      return 7
    elif word == "eight":
      return 8
    elif word == "nine":
      return 9
    elif word == "ten":
      return 10
    else:
      return None

  def get_invoice_total(self, invoice):
    total = 0
    for item_quantity in invoice.get("items"):
      item = item_quantity.get("item")
      quantity = self.get_quantity(item_quantity.get("quantity"))
      total += item.get("unit_price") * quantity
    return total

  def process_invoices(self):
    invoices = self.get_invoices()
    for invoice in invoices:
      if (invoice.get("items") is not None): # Some invoices have no items
        for item_quantity in invoice.get("items"):
          item = item_quantity.get("item")
          quantity = self.get_quantity(item_quantity.get("quantity"))
          flat_row = {
            "invoice_id": self.clean_invoice_id(invoice.get("id")),
            "created_on": invoice.get("created_on"),
            "invoice_item_id": item.get("id"),
            "invoice_item_name": item.get("name"),
            "unit_price": item.get("unit_price"),
            "type": self.resolve_item_type(item.get("type")),
            "total_price": item.get("unit_price") * quantity,
            "percentage_in_invoice": item.get("unit_price") / self.get_invoice_total(invoice),
            "is_expired": invoice.get("id") in self.get_expired_invoices()
          }
          self.flat_rows.append(flat_row)


extractor = DataExtractor()

extractor.process_invoices()
flat_rows = extractor.flat_rows

df = pd.DataFrame(flat_rows)

print("Column Types")
print(df.dtypes)

print("---------------------------------------------- \n")

print("The Dataframe")
print(df.to_string())
