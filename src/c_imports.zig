//! Shared C imports for the bridge application
//!
//! This module provides a single, unified @cImport for all C libraries
//! to avoid type incompatibility issues when multiple modules import the same C headers.
//!

pub const c = @cImport({
    @cInclude("libpq-fe.h");
    @cInclude("time.h");
});
