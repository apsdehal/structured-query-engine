import snappy


class Compressor:
    def __init__(self):
        return

    def compress(self, str):
        # Compress str here
        return snappy.compress(str)

    def decompress(self, str):
        # Decompress str here
        return snappy.decompress(str)
