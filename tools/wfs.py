# Create a DataFrame
import pandas as pd

df = pd.read_csv('/Users/huzhang/Desktop/WFS 箱柜.csv',encoding='gb18030')


# Function to calculate shipping cost
def calculate_shipping_cost(package_size, weight_kg):
    # Constants
    CM_TO_INCH = 0.393701
    KG_TO_LB = 2.20462
    VOLUME_DIVISOR = 1728  # cubic inches

    # Split the package size into L, W, H and convert to inches
    length_cm, width_cm, height_cm = map(float, package_size.split('*'))
    length_in = length_cm * CM_TO_INCH
    width_in = width_cm * CM_TO_INCH
    height_in = height_cm * CM_TO_INCH

    # Convert weight to pounds
    weight_lb = weight_kg * KG_TO_LB

    # Calculate volumetric weight
    volumetric_weight = (length_in * width_in * height_in) / VOLUME_DIVISOR

    # Determine which is greater
    chargeable_weight = max(volumetric_weight, weight_lb)

    # Shipping cost calculation based on the provided rates
    if chargeable_weight <= 1:
        cost = 3.45
    elif chargeable_weight <= 2:
        cost = 4.95
    elif chargeable_weight <= 3:
        cost = 5.45
    elif chargeable_weight <= 20:
        cost = 5.75 + (chargeable_weight - 4) * 0.40
    elif chargeable_weight <= 30:
        cost = 15.55 + (chargeable_weight - 21) * 0.40
    elif chargeable_weight <= 50:
        cost = 14.55 + (chargeable_weight - 31) * 0.40
    else:  # over 51 lb
        cost = 17.55 + (chargeable_weight - 51) * 0.40

    return cost


# Apply the function to each row
df['Shipping Cost ($)'] = df.apply(lambda row: calculate_shipping_cost(row['包装尺寸'], row['包装实重(KGS)']),
                                   axis=1)
print(df)
df.to_csv('/Users/huzhang/PycharmProjects/Guangxin/tools/shipping_costs.csv', index=False)