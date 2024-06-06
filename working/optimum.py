#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 16:15:32 2023

@author: anton
"""

from sympy import symbols, solve, diff, simplify
from sympy import lambdify

import numpy as np
import pandas as pd

def amount_out_v2(x, x0, y0, f):
    return y0 * x * (1 - f) / (x0 + x * (1 - f))

def solve_equations():
    Xa = symbols('Xa')
    X0 = symbols('X0')
    Ya = symbols('Ya')
    Y0 = symbols('Y0')
    # f = 0.003
    f = symbols('f')
    Xv = symbols('Xv')
    
    y = symbols('y')
    x = symbols('x')

    expr1 = X0*Y0 - (X0+Xa*(1-f))*(Y0-y)
    Ya = solve(expr1, y)[0]
    
    expr2 = (X0+Xa*(1-f))*(Y0-Ya) - (X0+Xa*(1-f)+Xv*(1-f))*(Y0-Ya-y)
    Yv = solve(expr2, y)[0]
    
    expr3 = (X0+Xa*(1-f)+Xv*(1-f))*(Y0-Ya-Yv) - (X0+Xa*(1-f)+Xv*(1-f)-x)*(Y0-Ya-Yv+Ya*(1-f))
    Xe = solve(expr3, x)[0]

    str(Xe-Xa)
    str(Yv)

def solve_equations_eat_all():
    X0 = symbols('X0')
    Ya = symbols('Ya')
    Y0 = symbols('Y0')
    # f = 0.003
    f = symbols('f')
    Xv = symbols('Xv')
    Yv = symbols('Yv')
    
    y = symbols('y')
    x = symbols('x')

    expr11 = X0*Y0 - (X0+x*(1-f))*(Y0-y)
    Ya = solve(expr11, y)[0]
    
    Ya = amount_out_v2(x, X0, Y0, f)
        
    expr21 = (X0 + x * (1 - f)) * (Y0 - Ya) - (X0 + x * (1-f) + Xv * (1 - f)) * (Y0 - Ya - Yv)
    Xa = solve(expr21, x)
    
    str(Xa[1])
    display(Xa[1])
    
    Xa[1].subs({X0: 200, Y0: 1e9, Xv: 0.65, Yv: 0.65 * 1e9 /200 /1.1})

def solve_equations_max():
    
    X0 = symbols('X0')
    Ya = symbols('Ya')
    Y0 = symbols('Y0')
    Xv = symbols('Xv')
    Yv = symbols('Yv')
    f = 0.003
    # f = symbols('f')
    
    x = symbols('x')

    Ya = amount_out_v2(x, X0, Y0, f)
    X1 = X0 + x * (1 - f)
    Y1 = Y0 - Ya
    
    Yv = amount_out_v2(Xv, X1, Y1, f)
    X2 = X1 + Xv * (1 - f)
    Y2 = Y1 - Yv

    Xe = amount_out_v2(Ya, Y2, X2, f)

    profit = Xe - x

    # df = diff(profit, x)
  
    # _df = simplify(df)
    
    # _x = solve(_df, x)
    
    # simplify(_x)




    res = lambdify(x, profit.subs({X0: 200, Y0: 1e9, Xv: 0.65}))
    A = np.zeros((2,500))
    A[0] = np.arange(500)/10
    A[1] = res(A[0])
    df = pd.DataFrame(A[1], index=A[0])
    df.plot()

