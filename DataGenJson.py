import pandas as pd
from faker import Faker
import random as rd
import json
import datetime

fake = Faker()
rd.seed(42)  # Set a random seed for reproducibility

# Create Order Variables
order_data = []

# Move these variables outside of the loop
customer_id = rd.randrange(1, 21)
first_name = fake.first_name()
last_name = fake.last_name()
phone_number = fake.phone_number()
email = fake.email()
load_Group_Number = rd.randrange(1, 10)
load_Sequence_Number = rd.randrange(1, 1000)

# Loop through each order ID in the range
for order_id in range(1, 10):
    order_lines = []  # Move this line inside the order creation loop

    # Generate other order-specific variables
    order_due_date = str(datetime.datetime.now() + datetime.timedelta(days=1))
    customer_Pick_Up_ByDate = str(datetime.datetime.now() + datetime.timedelta(days=1, hours=-4))
    delivery_ETA = str(datetime.datetime.now() + datetime.timedelta(days=1, hours=4))
    ship_eta = str(datetime.datetime.now() + datetime.timedelta(days=1, hours=4))
    delivery_schedule_start_time = str(datetime.datetime.now() + datetime.timedelta(days=1, hours=4))
    delivery_schedule_end_time = str(datetime.datetime.now() + datetime.timedelta(days=1, hours=8))
    number_of_order_lines = rd.randrange(1, 5)
    last_name = fake.last_name()

    # Loop to create order lines
    for i in range(1, number_of_order_lines + 1):
        # Generate order line variables
        value = rd.randrange(1, 10)
        itemonHandQty = rd.randrange(1, 100)
        webUnitPrice = rd.randrange(1, 100)
        storeUnitPrice = rd.randrange(1, 100)
        itemNumber = rd.randrange(10000, 7856953211414)
        item_description = fake.sentence()
        imageURL = "https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.pinterest.com%2Fpin%2F397000000000000000%2"
        deptNbr = rd.randrange(1, 1000)

        order_line = {
            "lineNbr": i,
            "quantity": {
                "value": value, "unitOfMeasure": "EACH"
            },
            "isSubstitutionAllowed": "true", "isManualSubAllowed": "true", "itemonHandQty": itemonHandQty,
            "webUnitPrice": webUnitPrice, "storeUnitPrice": storeUnitPrice, "itemNumber": itemNumber,
            "itemDescription": item_description,
            "imageURL": "https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.pinterest.com%2Fpin%2F397000000000000000%2",
            "department": {"deptNbr": deptNbr}
        }

        order_lines.append(order_line)

    # Create the order dictionary and load it with the order data
    order = {
        "orderid": order_id,
        "customer": {
            "id": customer_id,
            "contact": {
                "name": {
                    "firstname": first_name,
                    "lastname": last_name,
                }
            }
        },
        # ... (other order attributes)

        "orderLines": order_lines
    }

    order_data.append(order)


# Write each order data to a JSON file
for order_id, order in enumerate(order_data, start=1):
    json_data = json.dumps(order, indent=4)
    filename = f"C:/Users/Aryan/OneDrive/Documents/GitHub/MigrationProject/New folder/order_{order_id}.json"
    with open(filename, "w") as json_file:
        json_file.write(json_data)
