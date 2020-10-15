from math import exp

ffs_ao = 70
p_t = 2 / 100
g = -2 / 100
d = 1.5
caf_ao = 1
caf_mix_t = 0.53 * p_t ** 0.72
rho_mix_g = 8 * p_t if p_t < 0.01 else (0.03 * p_t)
caf_mix_g = (
    (0.126 - rho_mix_g)
    * max(0, 0.69 * (exp(12.9 * g) - 1))
    * max(0, 1.72 * (1 - 1.71 * exp(-3.16 * d)))
)
caf_mix_ffs = 0.25 * (1 + 0.7 * p_t) * ((70 - ffs_ao) / 100)
caf_mix = caf_ao - (caf_mix_t + caf_mix_g + caf_mix_ffs)

(1 - caf_mix * (1 - p_t)) / (caf_mix * p_t)
