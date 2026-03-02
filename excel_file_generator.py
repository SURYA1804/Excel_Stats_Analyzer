import pandas as pd

print("🚀 Creating 6 Excel files for Multi-Level Join Testing...")

# 1. EMPLOYEES.XLSX (MAIN - Level 1)
employees = pd.DataFrame({
    'emp_id': [1, 2, 3, 4, 5, 6],
    'name': ['John Doe', 'Jane Smith', 'Mike Brown', 'Sara Khan', 'Raj Patel', 'Priya Singh'],
    'dept_id': ['D1', 'D2', 'D1', 'D3', 'D2', 'D1'],
    'region_id': ['R1', 'R2', 'R1', 'R3', 'R2', 'R1'],
    'salary': [75000, 65000, 85000, 72000, 68000, 78000],
    'join_year': [2020, 2021, 2019, 2022, 2021, 2020]
})
employees.to_excel('employees.xlsx', index=False)

# 2. DEPARTMENTS.XLSX (Level 1 Direct join)
departments = pd.DataFrame({
    'dept_id': ['D1', 'D2', 'D3'],
    'dept_name': ['Sales', 'Marketing', 'HR'],
    'manager': ['Alice Wong', 'Bob Lee', 'Carol Devi'],
    'budget': [5000000, 3000000, 2000000]
})
departments.to_excel('departments.xlsx', index=False)

# 3. REGIONS.XLSX (Level 2 Left)
regions = pd.DataFrame({
    'region_id': ['R1', 'R2', 'R3'],
    'region_name': ['South India', 'West India', 'North India'],
    'country_code': ['IN-S', 'IN-W', 'IN-N']
})
regions.to_excel('regions.xlsx', index=False)

# 4. COUNTRY_CODES.XLSX (Level 2 Right - joins ONLY with regions)
country_codes = pd.DataFrame({
    'country_code': ['IN-S', 'IN-W', 'IN-N', 'US-E'],
    'country_name': ['India South', 'India West', 'India North', 'USA East'],
    'currency': ['INR', 'INR', 'INR', 'USD'],
    'tax_rate': [0.18, 0.20, 0.22, 0.30]
})
country_codes.to_excel('country_codes.xlsx', index=False)

# 5. SALARY_BANDS.XLSX (Level 2 Left)
salary_bands = pd.DataFrame({
    'salary_band_id': ['B1', 'B2', 'B3'],
    'band_name': ['Junior', 'Mid Level', 'Senior'],
    'min_salary': [0, 60001, 80001],
    'max_salary': [60000, 80000, 120000]
})
salary_bands.to_excel('salary_bands.xlsx', index=False)

# 6. TAX_BRACKETS.XLSX (Level 2 Right - joins ONLY with salary_bands)
tax_brackets = pd.DataFrame({
    'salary_band_id': ['B1', 'B2'],
    'tax_slab': ['Slab 1', 'Slab 2'],
    'tax_rate': [0.05, 0.20],
    'description': ['Entry Level Tax', 'Mid Level Tax']
})
tax_brackets.to_excel('tax_brackets.xlsx', index=False)

print("✅ ALL 6 FILES CREATED SUCCESSFULLY!")
print("\n📁 Files:")
print("• employees.xlsx (MAIN - Level 1)")
print("• departments.xlsx (Level 1 Direct)")
print("• regions.xlsx (Level 2 Left #1)")
print("• country_codes.xlsx (Level 2 Right #1)")
print("• salary_bands.xlsx (Level 2 Left #2)")
print("• tax_brackets.xlsx (Level 2 Right #2)")

print("\n🎯 EXPECTED WORKFLOW:")
print("L2 #1: regions ⟷ country_codes on `country_code` → regions_countries")
print("L2 #2: salary_bands ⟷ tax_brackets on `salary_band_id` → salary_tax")
print("L1: employees + departments + regions_countries")
print("\n💬 SAMPLE QUERIES:")
print("• 'Average salary by region_name?'")
print("• 'Total budget by country_name?'")
print("• 'Count employees by dept_name?'")
