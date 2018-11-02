import json
with open('test.json') as f:
    with open('test2.json') as g:
        for idx, f_line in enumerate(f):
            first = json.loads(f_line)
            second = json.loads(g.readline())
            if first != second:
                print(idx+1)
                print(first)
                print(second)
