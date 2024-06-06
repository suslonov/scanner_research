#!/usr/bin/env python3
# -*- coding: utf-8 -*-

MAX_UINT_256 = 115792089237316195423570985008687907853269984665640564039457584007913129639935
MAX_UINT_128 = 340282366920938463463374607431768211455
MAX_UINT_64 = 18446744073709551615
MAX_UINT_32 = 4294967295
MAX_UINT_16 = 65535
MAX_UINT_8 = 255

MIN_TICK = -887272
MAX_TICK = -MIN_TICK
MIN_SQRT_RATIO = 4295128739
MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342

def gt(a, b):
    return 1 if a > b else 0

def s64(q):
    return -(q & 0x8000000000000000000000000000000000000000000000000000000000000000) | (q & 0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff)

def get_sqrt_ratio_at_tick(tick):
    absTick = abs(tick)
    ratio = 0xfffcb933bd6fad37aa2d162d1a594001 if absTick & 0x1 != 0 else 0x100000000000000000000000000000000
    if absTick & 0x2 != 0:
        ratio = (ratio * 0xfff97272373d413259a46990580e213a) >> 128
    if absTick & 0x4 != 0:
        ratio = (ratio * 0xfff2e50f5f656932ef12357cf3c7fdcc) >> 128
    if absTick & 0x8 != 0:
        ratio = (ratio * 0xffe5caca7e10e4e61c3624eaa0941cd0) >> 128
    if absTick & 0x10 != 0:
        ratio = (ratio * 0xffcb9843d60f6159c9db58835c926644) >> 128
    if absTick & 0x20 != 0:
        ratio = (ratio * 0xff973b41fa98c081472e6896dfb254c0) >> 128
    if absTick & 0x40 != 0:
        ratio = (ratio * 0xff2ea16466c96a3843ec78b326b52861) >> 128
    if absTick & 0x80 != 0:
        ratio = (ratio * 0xfe5dee046a99a2a811c461f1969c3053) >> 128
    if absTick & 0x100 != 0:
        ratio = (ratio * 0xfcbe86c7900a88aedcffc83b479aa3a4) >> 128
    if absTick & 0x200 != 0:
        ratio = (ratio * 0xf987a7253ac413176f2b074cf7815e54) >> 128
    if absTick & 0x400 != 0:
        ratio = (ratio * 0xf3392b0822b70005940c7a398e4b70f3) >> 128
    if absTick & 0x800 != 0:
        ratio = (ratio * 0xe7159475a2c29b7443b29c7fa6e889d9) >> 128
    if absTick & 0x1000 != 0:
        ratio = (ratio * 0xd097f3bdfd2022b8845ad8f792aa5825) >> 128
    if absTick & 0x2000 != 0:
        ratio = (ratio * 0xa9f746462d870fdf8a65dc1f90e061e5) >> 128
    if absTick & 0x4000 != 0:
        ratio = (ratio * 0x70d869a156d2a1b890bb3df62baf32f7) >> 128
    if absTick & 0x8000 != 0:
        ratio = (ratio * 0x31be135f97d08fd981231505542fcfa6) >> 128
    if absTick & 0x10000 != 0:
        ratio = (ratio * 0x9aa508b5b7a84e1c677de54f3e99bc9) >> 128
    if absTick & 0x20000 != 0:
        ratio = (ratio * 0x5d6af8dedb81196699c329225ee604) >> 128
    if absTick & 0x40000 != 0:
        ratio = (ratio * 0x2216e584f5fa1ea926041bedfe98) >> 128
    if absTick & 0x80000 != 0:
        ratio = (ratio * 0x48a170391f7dc42444e8fa2) >> 128

    if tick > 0:
        ratio = MAX_UINT_256 // ratio

    return (ratio >> 32) + (0 if ratio % (1 << 32) == 0 else 1)


def get_tick_at_sqrt_ratio(sqrtPriceX96):
   
    ratio = sqrtPriceX96 << 32
    r = ratio
    msb = 0

    f = int(r > 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF) << 7
    msb = msb | f
    r = r >> f
    f = int(r > 0xFFFFFFFFFFFFFFFF) << 6
    msb = msb | f
    r = r >> f
    f = int(r > 0xFFFFFFFF) << 5
    msb = msb | f
    r = r >> f
    f = int(r > 0xFFFF) << 4
    msb = msb | f
    r = r >> f
    f = int(r > 0xFF) << 3
    msb = msb | f
    r = r >> f
    f = int(r > 0xF) << 2
    msb = msb | f
    r = r >> f
    f = int(r > 0x3) << 1
    msb = msb | f
    r = r >> f
    f = int(r > 0x1)
    msb = msb | f

    if msb >= 128:
        r = ratio >> (msb - 127)
    else:
        r = ratio << (127 - msb)

    log_2 = (msb - 128) << 64

    for i in range(63, 49, -1):
        r = (r * r) >> 127
        f = r >> 128
        log_2 = log_2 | (f << i)
        r = r >> f


    log_sqrt10001 = log_2 * 255738958999603826347141
    tickLow = (log_sqrt10001 - 3402992956809132418596140100660247210) >> 128
    tickHi = (log_sqrt10001 + 291339464771989622907027621153398088495) >> 128
    
    if tickLow == tickHi:
        return tickLow
    elif get_sqrt_ratio_at_tick(tickHi) <= sqrtPriceX96:
        return tickHi
    else:
        return tickLow


def most_significant_bit(x):
    r = 0
    if x >= 0x100000000000000000000000000000000 :
        x >>= 128
        r += 128
    if x >= 0x10000000000000000 :
        x >>= 64
        r += 64
    if x >= 0x100000000 :
        x >>= 32
        r += 32
    if x >= 0x10000 :
        x >>= 16
        r += 16
    if x >= 0x100 :
        x >>= 8
        r += 8
    if x >= 0x10 :
        x >>= 4
        r += 4
    if x >= 0x4 :
        x >>= 2
        r += 2
    if x >= 0x2:
        r += 1
    return r

def least_significant_bit(x):
    r = 255
    if x & MAX_UINT_128 > 0 :
        r -= 128
    else:
        x >>= 128
    if x & MAX_UINT_64 > 0 :
        r -= 64
    else:
        x >>= 64
    if x & MAX_UINT_32 > 0 :
        r -= 32
    else:
        x >>= 32
    if x & MAX_UINT_16 > 0 :
        r -= 16
    else:
        x >>= 16
    if x & MAX_UINT_8 > 0 :
        r -= 8
    else:
        x >>= 8
    if x & 0xf > 0 :
        r -= 4
    else:
        x >>= 4
    if x & 0x3 > 0 :
        r -= 2
    else:
        x >>= 2
    if x & 0x1 > 0:
        r -= 1
    return r
    
def next_initialized_tick_within_one_word(tick_bitmap, tick, tickSpacing, zeroForOne):
    compressed = tick // tickSpacing
    # if tick < 0 and tick % tickSpacing != 0:
    #     compressed -= 1

    if (zeroForOne):
        word_pos = compressed >> 8
        bit_pos = compressed % 256
        mask = (1 << bit_pos) - 1 + (1 << bit_pos)
        masked = tick_bitmap(word_pos) & mask

        initialized = masked != 0
        if initialized:
            next_tick = (compressed - int(bit_pos - most_significant_bit(masked))) * tickSpacing
        else:
            next_tick = (compressed - int(bit_pos)) * tickSpacing
    else:
        word_pos = (compressed + 1) >> 8
        bit_pos = (compressed + 1) % 256
        mask = ((1 << bit_pos) - 1) ^ 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        masked = tick_bitmap(word_pos) & mask

        initialized = masked != 0
        if initialized:
            next_tick = (compressed + 1 + (least_significant_bit(masked) - bit_pos)) * tickSpacing
        else:
            next_tick = (compressed + 1 + (255 - bit_pos)) * tickSpacing
    return next_tick, initialized
    


def next_initialized_tick(tick_bitmap, tick, new_tick, tickSpacing, zeroForOne):
    compressed = tick // tickSpacing
    if tick < 0 and tick % tickSpacing != 0:
        compressed -= 1
    compressed_new_tick = new_tick // tickSpacing
    if new_tick < 0 and new_tick % tickSpacing != 0:
        compressed_new_tick -= 1

    word_pos = compressed >> 8
    word_pos_new_tick = compressed_new_tick >> 8
    bit_pos = compressed % 256
    if tick_bitmap(word_pos) & (1 << bit_pos):
        return tick
    
    if (zeroForOne):
        mask = (1 << bit_pos) - 1 + (1 << bit_pos)
        masked = tick_bitmap(word_pos) & mask
        if masked:
            new_bit_pos = most_significant_bit(masked)
            return (compressed + 1 - bit_pos + new_bit_pos) * tickSpacing
        word_pos -= 1
    else:
        mask = ((1 << bit_pos) - 1) ^ 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        masked = tick_bitmap(word_pos) & mask
        if masked:
            new_bit_pos = least_significant_bit(masked)
            return (compressed + 1 - bit_pos + new_bit_pos) * tickSpacing
        word_pos += 1


    while tick_bitmap(word_pos):
        bitmap_word = tick_bitmap(word_pos)
        if (zeroForOne):
            if bitmap_word != 0:
                return ((word_pos << 8) + most_significant_bit(bitmap_word)) * tickSpacing
            else:
                word_pos -= 1
                if word_pos < word_pos_new_tick:
                    break
        else:
            if bitmap_word != 0:
                return ((word_pos << 8) + least_significant_bit(tick_bitmap(word_pos))) * tickSpacing
            else:
                word_pos -= 1
                if word_pos < word_pos_new_tick:
                    break

    return None
