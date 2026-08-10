//! Shared C imports for the bridge application
//!
//! The C headers are translated by the `addTranslateC` step in build.zig (see
//! src/c_includes.h) and exposed as the module "c". Because that is a real module
//! in the build graph, every importer shares one set of C types — the type-identity
//! problem that forced this file to exist under @cImport is now structural.
//!
//! Kept as a thin alias so call sites stay `c_imports.c`.

pub const c = @import("c");
