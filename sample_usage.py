# Run this locally to test
from export import export

export(
    "sqlite:///naija_employees.db",
    "employees",
    format="excel",
    output="nigerian_employees_full.xlsx"
)
print("100,000 employees exported to Excel!")
