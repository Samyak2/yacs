import datetime

format = "%Y-%m-%dT%H:%M:%S.%f"

start = datetime.datetime.strptime(input("Enter start timestamp: "), format)
end = datetime.datetime.strptime(input("Enter end timestamp: "), format)

print(end - start)
