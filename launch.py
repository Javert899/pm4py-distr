import sys

parameters = {}
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--type":
        parameters[sys.argv[i]] = sys.argv[i+1]
    elif sys.argv[i] == "--port":
        parameters[sys.argv[i]] = sys.argv[i+1]
    elif sys.argv[i] == "--master-host":
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == "--master-port":
        parameters[sys.argv[i]] = sys.argv[i + 1]
    elif sys.argv[i] == "--conf":
        parameters[sys.argv[i]] = sys.argv[i + 1]
    i = i + 1

print(parameters)