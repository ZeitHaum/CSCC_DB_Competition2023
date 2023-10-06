import re

pattern = re.compile("[\.|\/][^ \t]+\.csv")
string = "select a.id"
res = pattern.findall(string)
for ri in res:
    print(ri + "|end")