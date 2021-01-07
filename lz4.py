import binascii

def decompress(data):
    p = 0
    l = len(data)
    out = b''
    while p < l:
        if l - p < 5:
            raise ValueError("Bad input, last sequence < 5 bytes")
        token = data[p]
        p += 1
        literal_len = token >> 4
        matchlen = token & 0xF
        cont = literal_len == 0xF
        while cont:
            literal_len += data[p]
            cont = data[p] == 0xFF
            p += 1
        literal = data[p:p + literal_len]
        p += literal_len
        out += literal
        if p == l and matchlen != 0:
            raise ValueError("Bad input, last sequence has matchlen")
        if p == l:
            return out
        offset = data[p] | ((data[p + 1]) << 8)
        p += 2
        if offset == 0:
            raise ValueError("Bad input, zero offset")
        cont = matchlen == 0xF
        while cont:
            matchlen += data[p]
            cont = data[p] == 0xFF
            p += 1
        matchlen += 4
        if len(out) < offset:
            raise ValueError("WTF?")
        literal = out[len(out) - offset:]
        out += literal * (matchlen // offset)
        if matchlen % offset != 0:
            out += literal[:matchlen % offset]
    raise ValueError("Bad input")
