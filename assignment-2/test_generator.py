import random
import math

E_MAX = 1e5+7
e = random.randint(5, E_MAX)
V_MAX = random.randint(3, int(math.sqrt(e)))

edges = set()

with open("random_input.txt", "w") as f:
    for i in range(e):
        u = i%V_MAX
        v = random.randint(1, V_MAX)
        if u == v or (u,v) in edges or (v,u) in edges:
            continue
        edges.add((u, v))
        print(f"{u} {v}", file=f)