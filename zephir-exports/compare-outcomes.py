with open("outcome1.tsv") as f:
    with open("outcome2.tsv") as g:
        for idx, f_line in enumerate(f):
            first = f_line
            second = g.readline()
            if first != second:
                print(idx + 1)
                print(first)
                print(second)
