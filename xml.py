import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

class Loan:
    def __init__(self, loan_amount, annual_interest_rate, loan_period_years, payments_per_year, start_date):
        self.loan_amount = loan_amount
        self.annual_interest_rate = annual_interest_rate
        self.loan_period_years = loan_period_years
        self.payments_per_year = payments_per_year
        self.start_date = start_date
        self.monthly_interest_rate = annual_interest_rate / payments_per_year
        self.total_payments = loan_period_years * payments_per_year
        self.monthly_payment = self.calculate_monthly_payment()
    
    def calculate_monthly_payment(self):
        P = self.loan_amount
        r = self.monthly_interest_rate
        n = self.total_payments
        M = P * (r * (1 + r)**n) / ((1 + r)**n - 1)
        return M

    def generate_amortization_schedule(self):
        schedule = []
        remaining_balance = self.loan_amount
        payment_date = self.start_date

        for i in range(1, self.total_payments + 1):
            interest_payment = remaining_balance * self.monthly_interest_rate
            principal_payment = self.monthly_payment - interest_payment
            remaining_balance -= principal_payment
            schedule.append({
                'Payment Number': i,
                'Payment Date': payment_date,
                'Monthly Payment': self.monthly_payment,
                'Principal Payment': principal_payment,
                'Interest Payment': interest_payment,
                'Remaining Balance': remaining_balance
            })
            payment_date += timedelta(days=30)

        return pd.DataFrame(schedule)

def plot_repayment_schedule(schedule):
    plt.figure(figsize=(12, 6))
    
    plt.plot(schedule['Payment Date'], schedule['Principal Payment'], label='Principal Payment')
    plt.plot(schedule['Payment Date'], schedule['Interest Payment'], label='Interest Payment')
    plt.plot(schedule['Payment Date'], schedule['Remaining Balance'], label='Remaining Balance', linestyle='--')
    
    plt.title('Loan Repayment Schedule')
    plt.xlabel('Date')
    plt.ylabel('Amount ($)')
    plt.legend()
    plt.grid(True)
    plt.show()

# Sample data
loan_amount = 7500000
annual_interest_rate = 0.04
loan_period_years = 20
payments_per_year = 12
start_date = datetime(2024, 6, 15)

# Create a Loan object
loan = Loan(loan_amount, annual_interest_rate, loan_period_years, payments_per_year, start_date)

# Generate the amortization schedule
amortization_schedule = loan.generate_amortization_schedule()

# Plot the repayment schedule
plot_repayment_schedule(amortization_schedule)
