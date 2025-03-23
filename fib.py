import datetime
# Adding current datetime in microseconds
print
print
print(datetime.datetime.now())
def F(n):
    
    
    if n <= 1:
        return n
    else:
        return F(n - 1) + F(n - 2)

print(F(19))
print(datetime.datetime.now())