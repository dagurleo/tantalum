# Tantalum brand assets

The mark is tantalum's periodic-table tile (73, Ta, 180.95) drawn as a cube in a
single line weight. All text in these files is converted to outlines, so none of
them needs a font installed.

## Which file to use

| File | Use |
|---|---|
| `svg/mark-l-on-dark.svg`, `svg/mark-l-on-light.svg` | 64 px and up: number, symbol and mass; line 4/120 |
| `svg/mark-m-on-*.svg` | 32 to 63 px: number and symbol; line 6/120 |
| `svg/mark-s-on-*.svg` | up to 31 px: symbol only; line 9/120 |
| `svg/mark-16-on-*.svg` | exactly 16 px, drawn on the pixel grid |
| `svg/lockup-on-*.svg` | the mark with the wordmark |
| `readme-banner-light.svg`, `readme-banner-dark.svg` | README header, transparent background |
| `favicon.svg` | browser tab icon; follows the browser's light or dark mode |
| `favicon.ico` | 16, 32 and 48 px, for browsers without SVG icons |
| `favicon-16.png`, `favicon-32.png` | the same as PNGs |
| `apple-touch-icon.png` | 180 px home-screen icon |
| `icon-512.png` | avatars |
| `social-card.png` (source: `social-card.svg`) | GitHub social preview, 1280 x 640 |

"on-dark" files are for dark backgrounds (light lines), "on-light" files for
light ones.

## Construction

The front face is 84 x 84 on a 120 grid, with a depth of 24 at 45 degrees and a
margin of 6. Smaller sizes drop detail and thicken the line rather than
shrinking everything evenly; the 16 px version is drawn pixel by pixel.

## Colors

| Name | Hex | Role |
|---|---|---|
| Graphite | `#0F1114` | dark background |
| Tantalum | `#8C97A6` | quiet text, the mass |
| Polish | `#D6DCE4` | lines and text on dark |
| Ink | `#1A1E24` | lines and text on light |
| Capacitor amber | `#EBA13A` (`#C0700C` on light) | the atomic number; the one accent |

## Type

[Geist](https://vercel.com/font) SemiBold for the wordmark and symbol, Geist Mono
for numbers. Both are under the SIL Open Font License.

## In HTML

```html
<link rel="icon" href="/favicon.ico" sizes="48x48">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
```
