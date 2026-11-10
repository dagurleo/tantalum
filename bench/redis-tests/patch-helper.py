import sys
p = sys.argv[1]
s = open(p).read()
old = "    } elseif {$status eq {testing}} {\n"
assert old in s
s = s.replace(old, old + '        puts "\\[testing\\]: $data"\n', 1)
open(p, "w").write(s)
