def shift(s, level):
    return "\n".join([("    " * level) + line for line in s.split("\n")])
