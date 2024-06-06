#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from datetime import datetime


A = int(1312412489172984721389417234124312491239412012312312)
Af = float(1312412489172984721389417234124312491239412012312312)
B = int(3249723904891234810923810283901273490128390128390128301)
Bf = float(3249723904891234810923810283901273490128390128390128301)



t0 = datetime.now()
for i in range (100000000):
    C = A * (B + i)
print((datetime.now() - t0).total_seconds())

t0 = datetime.now()
for i in range (100000000):
    Cf = Af * (Bf + i)
print((datetime.now() - t0).total_seconds())

