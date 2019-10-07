
with open("C:\\Users\\liu.6544\\Downloads\\COTA APC 2018-2019 May Sep Jan\\Jan19") as infile:
    count = 0
    for line in infile:
        print(line)
        count += 1
        if count == 3:
            break
