#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from math import isqrt
Q96 = 0x1000000000000000000000000

def token0_optimum(X_v, Y_v, fee, L, P_sqrt):
    return ((2 * L * Y_v * fee * Q96 
            - 2 * L * Y_v * Q96 
            - P_sqrt * X_v * Y_v * fee**2 
            + 2 * P_sqrt * X_v * Y_v * fee 
            - P_sqrt * X_v * Y_v 
            + P_sqrt * isqrt(int(-4 * L**2 * X_v * Y_v * fee**3 
                            + 12 * L**2 * X_v * Y_v * fee**2
                            - 12 * L**2 * X_v * Y_v * fee 
                            + 4 * L**2 * X_v * Y_v 
                            + X_v**2 * Y_v**2 * fee**4 
                            - 4 * X_v**2 * Y_v**2 * fee**3 
                            + 6 * X_v**2 * Y_v**2 * fee**2 
                            - 4 * X_v**2 * Y_v**2 * fee 
                            + X_v**2 * Y_v**2))
            )/(2 * P_sqrt * Y_v * fee**2 
               - 4 * P_sqrt * Y_v * fee 
               + 2 * P_sqrt * Y_v))
     
               
def token0_profit(X_v, Y_v, X_a, fee, L, P_sqrt):
    return (-X_a 
            - (1 - fee) * (L*Q96 + P_sqrt * X_a * (1 - fee))
                * (L * P_sqrt * Q96 / (L * Q96 + P_sqrt * X_a * (1 - fee))- P_sqrt) 
                * (L * P_sqrt * X_v * Q96 * (1 - fee) / (L * Q96 + P_sqrt * X_a * (1 - fee)) + L * Q96) 
                / (L * P_sqrt * Q96 * (L**2 * P_sqrt * Q96**2 
                                       / ((L * Q96 + P_sqrt * X_a * (1 - fee)) 
                                          * (L * P_sqrt * X_v * Q96 * (1 - fee) 
                                             / (L * Q96 + P_sqrt * X_a * (1 - fee)) + L * Q96)
                                          ) - (1 - fee) * (L * P_sqrt * Q96 
                                                           / (L * Q96 + P_sqrt * X_a * (1 - fee)) - P_sqrt))))

def token0_victim_output(X_v, X_a, fee, L, P_sqrt):
    return (-L * (L**2 * P_sqrt * Q96**2
                  / ((L * Q96 + P_sqrt * X_a * (1 - fee)) 
                     * (L * P_sqrt * X_v * Q96 * (1 - fee) 
                        / (L * Q96 + P_sqrt * X_a * (1 - fee)) + L*Q96))
                  - L * P_sqrt * Q96 / (L * Q96 + P_sqrt * X_a * (1 - fee))) / Q96)

def token0_victim_input(Y_v_0, X_a, fee, L, P_sqrt):
    return (Y_v_0 * (L * Q96 
                     - P_sqrt * X_a * fee 
                     + P_sqrt * X_a)**3 
            / (P_sqrt * (-L**3 * P_sqrt * fee * Q96 
                         + L**3 * P_sqrt * Q96 
                         + L**2 * P_sqrt**2 * X_a * fee**2 
                         - 2 * L**2 * P_sqrt**2 * X_a * fee 
                         + L**2 * P_sqrt**2 * X_a 
                         + Y_v_0 * fee * (L * Q96 
                                          - P_sqrt * X_a * fee 
                                          + P_sqrt * X_a)**2 
                         - Y_v_0 * (L * Q96 
                                    - P_sqrt * X_a * fee 
                                    + P_sqrt * X_a)**2)))

def token1_optimum(X_v, Y_v, fee, L, P_sqrt):
    return ((Y_v * (fee**2 - 2 * fee + 1) * (2 * L * P_sqrt - X_v * fee * Q96 + X_v * Q96)
                + Q96 * isqrt(int(-X_v * Y_v 
                             * (fee - 1)**3 
                             * (4 * L**2 - X_v * Y_v * fee + X_v * Y_v))) * (fee - 1))
            / (2 * Y_v * Q96 * (fee - 1) * (fee**2 - 2 * fee + 1)))

def token1_profit(X_v, Y_v, X_a, fee, L, P_sqrt):
    return (-L * (L * Q96 * (P_sqrt + X_a * Q96 * (1 - fee) / L 
                             + X_v * Q96 * (1 - fee) / L)
                  / (L * Q96 + X_a * Q96**2 * (1 - fee)**2 * (P_sqrt 
                                                              + X_a * Q96 * (1 - fee) / L 
                                                              + X_v * Q96 * (1 - fee) / L)
                     /(P_sqrt * (P_sqrt + X_a * Q96 * (1 - fee) / L))) 
                  - P_sqrt 
                  - X_a * Q96 * (1 - fee) / L 
                  - X_v * Q96 * (1 - fee) / L)
            / Q96 - X_a)

def token1_victim_output(X_v, X_a, fee, L, P_sqrt):
    return (X_v * Q96**2 * (1 - fee) 
            / ((P_sqrt + X_a * Q96 * (1 - fee) / L) 
               * (P_sqrt + X_a * Q96 * (1 - fee) / L + X_v * Q96 * (1 - fee) / L)))

def token1_victim_input(Y_v_0, X_a, fee, L, P_sqrt):
    return (L**2 * Y_v_0 * Q96**2/(-L**2 * P_sqrt**2 * fee 
                                   + L**2 * P_sqrt**2 
                                   + L * P_sqrt * X_a * fee**2 * Q96 
                                   - 2 * L * P_sqrt * X_a * fee * Q96 
                                   + L * P_sqrt * X_a * Q96 * (fee - 1)**2 
                                   + L * P_sqrt * X_a * Q96 
                                   + L * P_sqrt * Y_v_0 * fee * Q96 
                                   - L * P_sqrt * Y_v_0 * Q96 
                                   - X_a**2 * fee * Q96**2 * (fee - 1)**2 
                                   + X_a**2 * Q96**2 * (fee - 1)**2 
                                   - X_a * Y_v_0 * fee**2 * Q96**2 
                                   + 2 * X_a * Y_v_0 * fee * Q96**2 
                                   - X_a * Y_v_0 * Q96**2))

