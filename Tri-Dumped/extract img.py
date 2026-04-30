from __future__ import annotations
import argparse, struct, sys
from dataclasses import dataclass
from pathlib import Path
from PIL import Image
import numpy as np


# ---------------------------------------------------------------------------
# Header (32 bytes, little-endian)
# ---------------------------------------------------------------------------

@dataclass
class ImgHeader:
    magic:     bytes   # 0x00  4b  b'IMG '
    asset_id:  int     # 0x04  u32 per-asset identifier
    width:     int     # 0x08  u16 texture width
    height:    int     # 0x0A  u16 texture height
    flags:     int     # 0x0C  u32 format flags; bit 0x10000 -> 4bpp, else 8bpp
    _pad0:     int     # 0x10  u32 always 0
    _dim2:     int     # 0x14  u32 width|height repeated
    pix_len:   int     # 0x18  u32 compressed pixel-data length in bytes
    _pad1:     int     # 0x1C  u32 always 0

    # After the header:
    #   CLUT   -- 64 bytes  (16 RGBA8 entries) when 4bpp
    #          -- 1024 bytes (256 RGBA8 entries) when 8bpp
    #          -- Alpha is PS2 alpha range: 0x80 = opaque, 0x00 = transparent
    #   Pixels -- pix_len bytes, LZ77-compressed (see decode_pixels)
    #          -- decompressed to width*height bytes (8bpp)
    #                              or width*height/2 bytes packed (4bpp)
    #          -- 4bpp packing: low nibble = pixel 2N, high nibble = pixel 2N+1
    #          -- linear row-major, no GS swizzle
    #   Tail   -- 0..15 zero bytes padding to 16-byte file alignment

    MAGIC = b'IMG '
    SIZE  = 32

    @classmethod
    def parse(cls, data: bytes) -> 'ImgHeader':
        magic, asset_id, width, height, flags, p0, dim2, pix_len, p1 = \
            struct.unpack_from('<4sIHHIIIII', data, 0)
        if magic != cls.MAGIC:
            raise ValueError(f'bad magic {magic!r}')
        return cls(magic, asset_id, width, height, flags, p0, dim2, pix_len, p1)

    @property
    def is_4bpp(self) -> bool:
        return bool(self.flags & 0x00010000)

    @property
    def clut_entries(self) -> int:   # 16 or 256
        return 16 if self.is_4bpp else 256

    @property
    def clut_size(self) -> int:      # 64 or 1024
        return self.clut_entries * 4

    @property
    def raw_pixel_size(self) -> int: # decompressed byte count
        return (self.width * self.height + 1) // 2 if self.is_4bpp \
               else self.width * self.height

# ---------------------------------------------------------------------------


def decode_pixels(src: bytes, expected_size: int) -> bytes:
    # LZ77-style: token byte n
    #   n < 0x80  -> literal: copy the next n bytes verbatim
    #   n >= 0x80 -> back-ref: copy (n & 0x7F) bytes from output[-offset]
    #                offset=1 behaves like RLE (repeat last byte)
    out = bytearray()
    i = 0
    while i < len(src) and len(out) < expected_size:
        n = src[i]; i += 1
        if n & 0x80:
            count  = n & 0x7F
            offset = src[i]; i += 1
            base   = len(out)
            for k in range(count):
                out.append(out[base - offset + k])
        else:
            out.extend(src[i:i + n])
            i += n
    if len(out) != expected_size:
        raise ValueError(f'decoded {len(out)} bytes, expected {expected_size}')
    return bytes(out)



@dataclass
class ImgTexture:
    header:  ImgHeader
    clut:    bytes
    pixels:  bytes

    @classmethod
    def parse(cls, data: bytes) -> 'ImgTexture':
        hdr      = ImgHeader.parse(data)
        clut_off = ImgHeader.SIZE
        pix_off  = clut_off + hdr.clut_size
        clut     = data[clut_off : clut_off + hdr.clut_size]
        raw      = decode_pixels(data[pix_off : pix_off + hdr.pix_len],
                                 hdr.raw_pixel_size)
        if hdr.is_4bpp:
            arr      = np.frombuffer(raw, dtype=np.uint8)
            unpacked = np.empty(arr.size * 2, dtype=np.uint8)
            unpacked[0::2] = arr & 0x0F   # low nibble  -> even pixel
            unpacked[1::2] = arr >> 4     # high nibble -> odd pixel
            pixels = unpacked[:hdr.width * hdr.height].tobytes()
        else:
            pixels = raw
        return cls(hdr, clut, pixels)

    def to_pil(self) -> Image.Image:
        h, w  = self.header.height, self.header.width
        clut  = np.frombuffer(self.clut,   dtype=np.uint8).reshape(self.header.clut_entries, 4)
        pix   = np.frombuffer(self.pixels, dtype=np.uint8).reshape(h, w)
        return Image.fromarray(clut[pix], 'RGBA')


# ---------------------------------------------------------------------------

def extract(in_path: Path, out_path: Path, verbose: bool) -> None:
    tex = ImgTexture.parse(in_path.read_bytes())
    hdr = tex.header
    if verbose:
        bpp = 4 if hdr.is_4bpp else 8
        print(f'  {in_path.name}: {hdr.width}x{hdr.height} {bpp}bpp '
              f'clut={hdr.clut_entries} asset=0x{hdr.asset_id:08x} '
              f'flags=0x{hdr.flags:08x} pix_len={hdr.pix_len}')
    tex.to_pil().save(out_path, format="PNG", optimize=False)


def main(argv=None):
    p = argparse.ArgumentParser(description='Extract MGS .img textures')
    p.add_argument('inputs', nargs='+', help='.img file(s)')
    p.add_argument('-o', '--out-dir', default=None, help='output directory (default: beside each input file)')
    p.add_argument('-v', '--verbose', action='store_true')
    args = p.parse_args(argv)

    for s in args.inputs:
        in_path = Path(s)

        if args.out_dir:
            out_dir = Path(args.out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            out_dir = in_path.parent

        try:
            extract(in_path, out_dir / (in_path.stem + '.png'), args.verbose)
        except Exception as e:
            print(f'  {in_path.name}: ERROR - {e}', file=sys.stderr)


if __name__ == '__main__':
    main()
