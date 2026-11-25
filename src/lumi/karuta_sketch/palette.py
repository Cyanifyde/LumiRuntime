from __future__ import annotations

import numpy as np
from skimage.color import rgb2lab

PALETTE_VERSION = "karuta-64-v1"

_ROWS = (
    ("Black", "#000000"),
    ("Ink", "#131313"),
    ("Coal", "#1B1B1B"),
    ("Charcoal", "#272727"),
    ("Graphite", "#3D3D3D"),
    ("Slate", "#5D5D5D"),
    ("Ash", "#858585"),
    ("Silver", "#B4B4B4"),
    ("White", "#FFFFFF"),
    ("Cloud", "#C7CFDD"),
    ("Blue Grey", "#92A1B9"),
    ("Steel", "#657392"),
    ("Storm", "#424C6E"),
    ("Navy Grey", "#2A2F4E"),
    ("Midnight", "#1A1932"),
    ("Deep Night", "#0E071B"),
    ("Plum Black", "#1C121C"),
    ("Brown Black", "#391F21"),
    ("Umber", "#5D2C28"),
    ("Chestnut", "#8A4836"),
    ("Clay", "#BF6F4A"),
    ("Peach", "#E69C69"),
    ("Cream", "#F6CA9F"),
    ("Ivory", "#F9E6CF"),
    ("Ochre", "#EDAB50"),
    ("Burnt Orange", "#E07438"),
    ("Rust", "#C64524"),
    ("Brick", "#8E251D"),
    ("Orange Red", "#FF5000"),
    ("Orange", "#ED7614"),
    ("Amber", "#FFA214"),
    ("Gold", "#FFC825"),
    ("Yellow", "#FFEB57"),
    ("Lime Cream", "#D3FC7E"),
    ("Lime", "#99E65F"),
    ("Leaf", "#5AC54F"),
    ("Green", "#33984B"),
    ("Forest", "#1E6F50"),
    ("Deep Teal", "#134C4C"),
    ("Ocean Black", "#0C2E44"),
    ("Deep Blue", "#00396D"),
    ("Blue", "#0069AA"),
    ("Sky", "#0098DC"),
    ("Cyan", "#00CDF9"),
    ("Bright Cyan", "#0CF1FF"),
    ("Ice", "#94FDFF"),
    ("Pink White", "#FDD2ED"),
    ("Pink", "#F389F5"),
    ("Magenta", "#DB3FFD"),
    ("Violet", "#7A09FA"),
    ("Indigo", "#3003D9"),
    ("Deep Indigo", "#0C0293"),
    ("Blue Black", "#03193F"),
    ("Deep Purple", "#3B1443"),
    ("Purple", "#622461"),
    ("Mauve", "#93388F"),
    ("Orchid", "#CA52C9"),
    ("Rose", "#C85086"),
    ("Salmon", "#F68187"),
    ("Coral", "#F5555D"),
    ("Red", "#EA323C"),
    ("Crimson", "#C42430"),
    ("Wine", "#891E2B"),
    ("Dark Wine", "#571C27"),
)

PALETTE = tuple(_ROWS)
PALETTE_HEX = tuple(value for _, value in PALETTE)


def palette_rgb() -> np.ndarray:
    return np.asarray(
        [[int(value[index : index + 2], 16) for index in (1, 3, 5)] for value in PALETTE_HEX],
        dtype=np.uint8,
    )


def palette_lab() -> np.ndarray:
    rgb = palette_rgb().astype(np.float32)[None, :, :] / 255.0
    return rgb2lab(rgb)[0].astype(np.float32)
