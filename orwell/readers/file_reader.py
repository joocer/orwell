

#def file_reader(**kwargs):
#    return _inner_reader(**kwargs)


def file_reader(file_name="", chunk_size=16*1024*1024, delimiter="\n"):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(file_name, "r", encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward
