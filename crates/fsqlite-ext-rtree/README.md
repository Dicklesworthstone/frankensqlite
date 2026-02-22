# fsqlite-ext-rtree

R-tree and Geopoly spatial index extension for FrankenSQLite.

## Overview

This crate implements the R-tree spatial indexing extension, providing multi-dimensional bounding box queries (1-5 dimensions) and the Geopoly polygon extension built on top of R-tree. It corresponds to SQLite's R*-tree module and Geopoly extension.

This is a leaf crate in the fsqlite workspace dependency graph. It depends on `fsqlite-types` and `fsqlite-error` and has no dependency on the core query engine, making it usable independently for spatial data structures.

## Key Types

- `RtreeIndex` - In-memory R*-tree spatial index supporting insert, delete, update, range queries, and custom geometry callbacks
- `RtreeConfig` - Configuration for an R*-tree virtual table (dimensions 1-5, coordinate type)
- `RtreeEntry` - A single entry in the R*-tree consisting of a rowid and a multi-dimensional bounding box
- `RtreeCoordType` - Coordinate type enum: `Float32` or `Int32`
- `MBoundingBox` - Multi-dimensional axis-aligned bounding box (1-5 dimensions) with overlap, union, volume, and enlargement operations
- `RtreeGeometry` (trait) - Custom geometry callback trait for defining spatial predicates beyond simple bounding-box overlap
- `RtreeQueryResult` - Result enum for geometry callbacks: `Include`, `Exclude`, or `PartiallyContained`
- `Point` - 2D point primitive (x, y)
- `BoundingBox` - 2D axis-aligned bounding box with point/box containment tests

## Key Functions

- `geopoly_blob` / `geopoly_blob_decode` - Encode/decode polygons to/from the Geopoly binary blob format
- `geopoly_json` / `geopoly_json_decode` - Convert polygons to/from GeoJSON-style coordinate arrays
- `geopoly_svg` - Render a polygon as an SVG path `d` attribute
- `geopoly_area` - Compute polygon area using the shoelace formula
- `geopoly_contains_point` - Point-in-polygon test using ray casting
- `geopoly_overlap` - Test whether two polygons overlap (edge intersection + containment)
- `geopoly_within` - Test whether one polygon is entirely within another
- `geopoly_bbox` / `geopoly_group_bbox` - Compute bounding boxes for single or grouped polygons
- `geopoly_regular` - Generate a regular polygon (triangle, hexagon, etc.)
- `geopoly_xform` - Apply a 2D affine transformation to polygon vertices
- `geopoly_ccw` - Normalize polygon winding to counter-clockwise

## Dependencies

- `fsqlite-types`
- `fsqlite-error`

## License

MIT
