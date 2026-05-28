//! Bindings and wrappers for Zstandard (zstd) compression library.
//! Purified for high-performance streaming without internal logging.
//!
//! Thread-safety
//! -----------
//! ZSTD_CCtx / ZSTD_DCtx are NOT thread-safe.
//! Each thread must own its own `Compressor` / `Decompressor`, or use `ContextPool`.
//!
//! Dictionary lifecycle:
//! 1. Dictionaries are trained and distributed as raw bytes ([]u8).
//! 2. Raw dictionaries are sent over the wire (e.g. via NATS).
//! 3. Each consumer digests raw dictionaries locally using
//!    `ZSTD_createCDict` / `ZSTD_createDDict`.
//! 4. Digested dictionaries are process-local and must NOT be serialized.

const std = @import("std");

// -----------------------------------
// === C API bindings (Minimal) ===
// -----------------------------------

extern "c" fn ZSTD_isError(code: usize) c_uint;
extern "c" fn ZSTD_getErrorName(code: usize) [*c]const u8;
extern "c" fn ZSTD_versionString() [*c]const u8;
extern "c" fn ZSTD_getFrameContentSize(src: *const anyopaque, srcSize: usize) u64;
extern "c" fn ZSTD_compressBound(srcSize: usize) usize;

pub const ZSTD_CCtx = opaque {};
pub const ZSTD_DCtx = opaque {};
pub const ZSTD_CDict = opaque {};
pub const ZSTD_DDict = opaque {};

extern "c" fn ZSTD_createCCtx() ?*ZSTD_CCtx;
extern "c" fn ZSTD_freeCCtx(cctx: *ZSTD_CCtx) usize;
extern "c" fn ZSTD_createDCtx() ?*ZSTD_DCtx;
extern "c" fn ZSTD_freeDCtx(dctx: *ZSTD_DCtx) usize;

extern "c" fn ZSTD_createCDict(dictBuffer: [*]const u8, dictSize: usize, compressionLevel: c_int) ?*ZSTD_CDict;
extern "c" fn ZSTD_freeCDict(cdict: *ZSTD_CDict) usize;
extern "c" fn ZSTD_createDDict(dictBuffer: [*]const u8, dictSize: usize) ?*ZSTD_DDict;
extern "c" fn ZSTD_freeDDict(ddict: *ZSTD_DDict) usize;

extern "c" fn ZSTD_compress2(
    cctx: *ZSTD_CCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
) usize;

extern "c" fn ZSTD_compress_usingCDict(
    cctx: *ZSTD_CCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
    cdict: *ZSTD_CDict,
) usize;

extern "c" fn ZSTD_decompressDCtx(
    dctx: *ZSTD_DCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
) usize;

extern "c" fn ZSTD_decompress_usingDDict(
    dctx: *ZSTD_DCtx,
    dst: [*]u8,
    dstCapacity: usize,
    src: [*]const u8,
    srcSize: usize,
    ddict: *ZSTD_DDict,
) usize;

extern "c" fn ZSTD_minCLevel() c_int;
extern "c" fn ZSTD_maxCLevel() c_int;

extern "c" fn ZSTD_CCtx_setParameter(cctx: *ZSTD_CCtx, param: ZSTD_cParameter, value: c_int) usize;
extern "c" fn ZSTD_CCtx_reset(cctx: *ZSTD_CCtx, reset: ZSTD_ResetDirective) usize;
extern "c" fn ZSTD_DCtx_reset(dctx: *ZSTD_DCtx, reset: ZSTD_ResetDirective) usize;

extern "c" fn ZDICT_trainFromBuffer(
    dictBuffer: [*]u8,
    dictBufferCapacity: usize,
    samplesBuffer: [*]const u8,
    samplesSizes: [*]const usize,
    nbSamples: c_uint,
) usize;

pub fn version() []const u8 {
    return std.mem.span(ZSTD_versionString());
}

pub fn errorName(code: usize) []const u8 {
    return std.mem.span(ZSTD_getErrorName(code));
}

// -------------------------------
// === STREAMING (C API Low-level) ===
// -------------------------------

/// Zstd streaming input buffer (ABI-compatible)
pub const ZSTD_inBuffer = extern struct {
    src: ?*const anyopaque,
    size: usize,
    pos: usize,
};

/// Zstd streaming output buffer (ABI-compatible)
pub const ZSTD_outBuffer = extern struct {
    dst: ?*anyopaque,
    size: usize,
    pos: usize,
};

/// Streaming compression directives
pub const ZSTD_EndDirective = enum(c_int) {
    ZSTD_e_continue = 0,
    ZSTD_e_flush = 1,
    ZSTD_e_end = 2,
};

extern "c" fn ZSTD_compressStream2(
    cctx: *ZSTD_CCtx,
    output: *ZSTD_outBuffer,
    input: *ZSTD_inBuffer,
    endOp: ZSTD_EndDirective,
) usize;

extern "c" fn ZSTD_decompressStream(
    dctx: *ZSTD_DCtx,
    output: *ZSTD_outBuffer,
    input: *ZSTD_inBuffer,
) usize;

extern "c" fn ZSTD_CStreamInSize() usize;
extern "c" fn ZSTD_CStreamOutSize() usize;
extern "c" fn ZSTD_DStreamInSize() usize;
extern "c" fn ZSTD_DStreamOutSize() usize;

/// Recommended buffer sizes (never fail)
pub fn compressStreamInSize() usize {
    return ZSTD_CStreamInSize();
}

pub fn compressStreamOutSize() usize {
    return ZSTD_CStreamOutSize();
}

pub fn decompressStreamInSize() usize {
    return ZSTD_DStreamInSize();
}

pub fn decompressStreamOutSize() usize {
    return ZSTD_DStreamOutSize();
}

// -----------------------------------
// === Errors + Helpers ===
// -----------------------------------

pub const Error = error{
    ZstdError,
    NotZstdFormat,
    SizeUnknown,
    InvalidCompressionLevel,
    InvalidDictionarySize,
    NoSamples,
    OutputTooLarge,
};

/// Pure error wrapper for Zstd size_t return codes.
fn wrap(code: usize) Error!usize {
    if (ZSTD_isError(code) != 0) return error.ZstdError;
    return code;
}

// -----------------------------------
// === Frame size helper ===
// -----------------------------------

const ZSTD_CONTENTSIZE_UNKNOWN: u64 = std.math.maxInt(u64);
const ZSTD_CONTENTSIZE_ERROR: u64 = std.math.maxInt(u64) - 1;

pub fn get_decompressed_size(compressed: []const u8) Error!usize {
    const size = ZSTD_getFrameContentSize(compressed.ptr, compressed.len);
    if (size == ZSTD_CONTENTSIZE_ERROR) return error.NotZstdFormat;
    if (size == ZSTD_CONTENTSIZE_UNKNOWN) return error.SizeUnknown;
    return @intCast(size);
}

// -----------------------------------
// === Enums & Configs ===
// -----------------------------------

const ZSTD_cParameter = enum(c_int) {
    ZSTD_c_compressionLevel = 100,
    ZSTD_c_strategy = 107,
};

pub const ZSTD_strategy = enum(c_int) {
    ZSTD_fast = 1,
    ZSTD_dfast = 2,
    ZSTD_greedy = 3,
    ZSTD_lazy = 4,
    ZSTD_lazy2 = 5,
    ZSTD_btlazy2 = 6,
    ZSTD_btopt = 7,
    ZSTD_btultra = 8,
    ZSTD_btultra2 = 9,
};

pub const ZSTD_ResetDirective = enum(c_int) {
    ZSTD_reset_session_only = 0,
    ZSTD_reset_parameters = 1,
    ZSTD_reset_session_and_parameters = 2,
};

pub const CompressionRecipe = enum {
    fast,
    balanced,
    binary,
    text,
    structured_data,
    maximum,

    pub fn getLevel(self: CompressionRecipe) c_int {
        return switch (self) {
            .fast => 1,
            .balanced => 3,
            .binary => 6,
            .text => 9,
            .structured_data => 9,
            .maximum => 22,
        };
    }

    pub fn getStrategy(self: CompressionRecipe) ZSTD_strategy {
        return switch (self) {
            .fast => .ZSTD_fast,
            .balanced => .ZSTD_dfast,
            .binary => .ZSTD_lazy2,
            .text => .ZSTD_btopt,
            .structured_data => .ZSTD_btultra,
            .maximum => .ZSTD_btultra2,
        };
    }
};

pub const CompressionConfig = struct {
    compression_level: ?c_int = null,
    recipe: ?CompressionRecipe = null,
};

pub const DecompressionConfig = struct {
    max_window_log: ?c_int = null, // reserved for future
};

// -----------------------------------
// === Compressor
// -----------------------------------

pub const Compressor = struct {
    ctx: *ZSTD_CCtx,

    pub fn init(config: CompressionConfig) !Compressor {
        const cctx = ZSTD_createCCtx() orelse return error.ZstdError;
        errdefer _ = ZSTD_freeCCtx(cctx);

        const level: c_int =
            if (config.compression_level) |lvl| lvl else if (config.recipe) |recipe| recipe.getLevel() else 3;

        if (level < ZSTD_minCLevel() or level > ZSTD_maxCLevel()) return error.InvalidCompressionLevel;

        _ = try wrap(ZSTD_CCtx_setParameter(cctx, .ZSTD_c_compressionLevel, level));
        if (config.recipe) |recipe| {
            _ = try wrap(ZSTD_CCtx_setParameter(cctx, .ZSTD_c_strategy, @intFromEnum(recipe.getStrategy())));
        }

        return .{ .ctx = cctx };
    }

    pub fn deinit(self: *Compressor) void {
        _ = ZSTD_freeCCtx(self.ctx);
    }

    pub fn resetSession(self: *Compressor) Error!void {
        _ = try wrap(ZSTD_CCtx_reset(self.ctx, .ZSTD_reset_session_only));
    }

    pub fn getUpperBound(src_len: usize) usize {
        // ZSTD_compressBound cannot fail.
        return ZSTD_compressBound(src_len);
    }

    pub fn compress(
        self: *Compressor,
        allocator: std.mem.Allocator,
        input: []const u8,
    ) ![]u8 {
        _ = try wrap(ZSTD_CCtx_reset(
            self.ctx,
            .ZSTD_reset_session_only,
        ));
        const bound = ZSTD_compressBound(input.len);
        const dst = try allocator.alloc(u8, bound);
        errdefer allocator.free(dst);

        const written = try wrap(
            ZSTD_compress2(
                self.ctx,
                dst.ptr,
                dst.len,
                input.ptr,
                input.len,
            ),
        );

        if (written < dst.len) _ = allocator.resize(dst, written);
        return dst[0..written];
    }

    pub fn compress_using_cdict(
        self: *Compressor,
        allocator: std.mem.Allocator,
        input: []const u8,
        cdict: *ZSTD_CDict,
    ) ![]u8 {

        //reset for pooled context safety
        _ = try wrap(ZSTD_CCtx_reset(self.ctx, .ZSTD_reset_session_only));

        const bound = ZSTD_compressBound(input.len);
        const out = try allocator.alloc(u8, bound);
        errdefer allocator.free(out);

        const written = try wrap(
            ZSTD_compress_usingCDict(
                self.ctx,
                out.ptr,
                bound,
                input.ptr,
                input.len,
                cdict,
            ),
        );
        if (written < out.len) _ = allocator.resize(out, written);
        return out[0..written];
    }

    // Low-level streaming compression step.
    ///
    /// Returns:
    /// - number of bytes still to flush (0 means done)
    ///
    /// Contract:
    /// - `ZSTD_CCtx_reset(..., ZSTD_reset_session_only)`
    ///   must be called *once* before starting a new stream.
    pub fn compressStream(
        self: *Compressor,
        output: *ZSTD_outBuffer,
        input: *ZSTD_inBuffer,
        endOp: ZSTD_EndDirective,
    ) Error!usize {
        return wrap(
            ZSTD_compressStream2(
                self.ctx,
                output,
                input,
                endOp,
            ),
        );
    }
};

// -----------------------------------
// === Decompressor
// -----------------------------------

pub const Decompressor = struct {
    ctx: *ZSTD_DCtx,

    pub fn init(config: DecompressionConfig) !Decompressor {
        _ = config;
        const dctx = ZSTD_createDCtx() orelse return error.ZstdError;
        return .{ .ctx = dctx };
    }

    pub fn deinit(self: *Decompressor) void {
        _ = ZSTD_freeDCtx(self.ctx);
    }

    pub fn resetSession(self: *Decompressor) Error!void {
        _ = try wrap(ZSTD_DCtx_reset(self.ctx, .ZSTD_reset_session_only));
    }

    pub fn decompress_known_size(
        self: *Decompressor,
        allocator: std.mem.Allocator,
        input: []const u8,
    ) ![]u8 {
        const output_size = try get_decompressed_size(input);
        const out = try allocator.alloc(u8, output_size);
        errdefer allocator.free(out);

        const written = try wrap(
            ZSTD_decompressDCtx(
                self.ctx,
                out.ptr,
                output_size,
                input.ptr,
                input.len,
            ),
        );
        if (written < out.len) _ = allocator.resize(out, written);
        return out[0..written];
    }

    pub fn decompress_using_ddict(
        self: *Decompressor,
        allocator: std.mem.Allocator,
        input: []const u8,
        ddict: *ZSTD_DDict,
    ) ![]u8 {
        const output_size = try get_decompressed_size(input);
        const out = try allocator.alloc(u8, output_size);
        errdefer allocator.free(out);

        const written = try wrap(
            ZSTD_decompress_usingDDict(
                self.ctx,
                out.ptr,
                output_size,
                input.ptr,
                input.len,
                ddict,
            ),
        );
        if (written < out.len) _ = allocator.resize(out, written);
        return out[0..written];
    }

    pub fn decompress_growable(
        self: *Decompressor,
        allocator: std.mem.Allocator,
        input_data: []const u8,
        max_output: usize,
    ) ![]u8 {
        try self.resetSession();

        var out_list = std.ArrayList(u8).init(allocator);
        errdefer out_list.deinit();

        const initial_guess = @max(input_data.len * 2, ZSTD_DStreamOutSize());
        try out_list.ensureTotalCapacity(initial_guess);

        var input = ZSTD_inBuffer{ .src = input_data.ptr, .size = input_data.len, .pos = 0 };

        while (input.pos < input.size) {
            const spare = out_list.unusedCapacitySlice();
            if (spare.len == 0) {
                try out_list.ensureTotalCapacity(out_list.capacity + (out_list.capacity / 2) + 1);
                continue;
            }

            var output = ZSTD_outBuffer{ .dst = spare.ptr, .size = spare.len, .pos = 0 };
            const r = try wrap(ZSTD_decompressStream(self.ctx, &output, &input));

            out_list.items.len += output.pos;

            if (out_list.items.len > max_output) return error.OutputTooLarge;

            // Supports concatenated frames if present in input.
            if (r == 0 and input.pos == input.size) break;
        }

        return out_list.toOwnedSlice();
    }

    /// Low-level streaming decompression step.
    ///
    /// Returns:
    /// - hint for how much more input is expected (0 = frame complete)
    ///
    /// Contract:
    /// - `ZSTD_DCtx_reset(..., ZSTD_reset_session_only)`
    ///   must be called *once* before starting a new stream.
    pub fn decompressStream(
        self: *Decompressor,
        output: *ZSTD_outBuffer,
        input: *ZSTD_inBuffer,
    ) Error!usize {
        return wrap(
            ZSTD_decompressStream(
                self.ctx,
                output,
                input,
            ),
        );
    }
};

// -----------------------------------
// === Context Pooling
// -----------------------------------

pub fn ContextPool(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        contexts: std.ArrayList(*T),
        mutex: std.Thread.Mutex = .{},
        sem: std.Thread.Semaphore,

        pub fn init(allocator: std.mem.Allocator, capacity: usize, config: anytype) !*Self {
            _ = config; // for future configurability (e.g. tuning)

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .contexts = std.ArrayList(*T).init(allocator),
                .sem = std.Thread.Semaphore{ .permits = capacity },
            };
            errdefer self.deinit();

            for (0..capacity) |_| {
                const ctx: *T = blk: {
                    if (T == ZSTD_CCtx) {
                        break :blk ZSTD_createCCtx() orelse return error.ZstdError;
                    } else if (T == ZSTD_DCtx) {
                        break :blk ZSTD_createDCtx() orelse return error.ZstdError;
                    } else {
                        @compileError("ContextPool only supports ZSTD_CCtx or ZSTD_DCtx");
                    }
                };

                // If append fails, free the ctx to avoid leaks.
                self.contexts.append(ctx) catch |e| {
                    if (T == ZSTD_CCtx) _ = ZSTD_freeCCtx(@ptrCast(ctx)) else _ = ZSTD_freeDCtx(@ptrCast(ctx));
                    return e;
                };
            }

            return self;
        }

        pub fn acquire(self: *Self) *T {
            self.sem.wait();
            self.mutex.lock();
            defer self.mutex.unlock();

            std.debug.assert(self.contexts.items.len > 0);
            return self.contexts.pop();
        }

        pub fn release(self: *Self, ctx: *T) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (T == ZSTD_CCtx) {
                _ = ZSTD_CCtx_reset(@ptrCast(ctx), .ZSTD_reset_session_only);
            } else {
                _ = ZSTD_DCtx_reset(@ptrCast(ctx), .ZSTD_reset_session_only);
            }

            self.contexts.append(ctx) catch unreachable;
            self.sem.post();
        }

        pub fn deinit(self: *Self) void {
            for (self.contexts.items) |ctx| {
                if (T == ZSTD_CCtx) _ = ZSTD_freeCCtx(@ptrCast(ctx)) else _ = ZSTD_freeDCtx(@ptrCast(ctx));
            }
            self.contexts.deinit();
            self.allocator.destroy(self);
        }
    };
}

// -----------------------------------
// === Dictionary Training (RAW) ===
// -----------------------------------

/// Train a portable (raw) Zstd dictionary from a pre-flattened corpus.
///
/// This function performs no allocation and produces bytes that are
/// safe to store, version, and distribute (e.g. via NATS).
///
/// The resulting dictionary must be digested locally using
/// `ZSTD_createCDict` / `ZSTD_createDDict` before use.
pub fn trainRawDictionary(
    dict_buffer: []u8,
    samples_buffer: []const u8,
    sample_sizes: []const usize,
) Error!usize {
    if (sample_sizes.len == 0) return error.NoSamples;
    return wrap(ZDICT_trainFromBuffer(
        dict_buffer.ptr,
        dict_buffer.len,
        samples_buffer.ptr,
        sample_sizes.ptr,
        @intCast(sample_sizes.len),
    ));
}

// -----------------------------------
// === Dictionary Support
// -----------------------------------

/// Wrapper around `train_dictionary_from_buffer()`.
///
/// - Allocates and flattens samples
/// - Trains the dictionary
/// - Returns an owned slice containing the trained dictionary
///
/// Suitable for one-off training.
///
/// Dictionary lifecycle:
/// 1. Dictionaries are trained and distributed as raw bytes ([]u8).
/// 2. Raw dictionaries are sent over the wire (e.g. via NATS).
/// 3. Each consumer digests raw dictionaries locally using
///    `ZSTD_createCDict` / `ZSTD_createDDict`.
/// 4. Digested dictionaries are process-local and must NOT be serialized.
pub fn train_dictionary(
    allocator: std.mem.Allocator,
    samples: []const []const u8,
    dict_size: usize,
) ![]u8 {
    if (samples.len == 0) return error.NoSamples;
    if (dict_size < 256 or dict_size > (1 << 20)) return error.InvalidDictionarySize;

    var total_size: usize = 0;
    for (samples) |s| total_size += s.len;

    const samples_buffer = try allocator.alloc(u8, total_size);
    defer allocator.free(samples_buffer);

    const sample_sizes = try allocator.alloc(usize, samples.len);
    defer allocator.free(sample_sizes);

    var offset: usize = 0;
    for (samples, 0..) |s, i| {
        @memcpy(samples_buffer[offset .. offset + s.len], s);
        sample_sizes[i] = s.len;
        offset += s.len;
    }

    const dict_buffer = try allocator.alloc(u8, dict_size);
    errdefer allocator.free(dict_buffer);

    const written = try train_dictionary_from_buffer(
        dict_buffer,
        samples_buffer,
        sample_sizes,
    );

    if (written < dict_buffer.len)
        _ = allocator.resize(dict_buffer, written);

    return dict_buffer[0..written];
}

pub const DictionaryManager = struct {
    allocator: std.mem.Allocator,
    cdicts: std.StringHashMap(*ZSTD_CDict),
    ddicts: std.StringHashMap(*ZSTD_DDict),

    pub fn init(allocator: std.mem.Allocator) DictionaryManager {
        return .{
            .allocator = allocator,
            .cdicts = std.StringHashMap(*ZSTD_CDict).init(allocator),
            .ddicts = std.StringHashMap(*ZSTD_DDict).init(allocator),
        };
    }

    pub fn deinit(self: *DictionaryManager) void {
        {
            var it = self.cdicts.iterator();
            while (it.next()) |e| _ = ZSTD_freeCDict(e.value_ptr.*);
            self.cdicts.deinit();
        }
        {
            var it = self.ddicts.iterator();
            while (it.next()) |e| _ = ZSTD_freeDDict(e.value_ptr.*);
            self.ddicts.deinit();
        }
    }

    pub fn hasTable(self: *DictionaryManager, table_name: []const u8) bool {
        return self.cdicts.contains(table_name);
    }

    pub fn loadTableCDict(self: *DictionaryManager, table_name: []const u8, raw_dict: []const u8, level: c_int) !void {
        if (level < ZSTD_minCLevel() or level > ZSTD_maxCLevel()) return error.InvalidCompressionLevel;

        const cdict = ZSTD_createCDict(raw_dict.ptr, raw_dict.len, level) orelse return error.ZstdError;
        errdefer _ = ZSTD_freeCDict(cdict);

        const prev = try self.cdicts.fetchPut(table_name, cdict);
        if (prev) |p| _ = ZSTD_freeCDict(p.value);
    }

    pub fn loadTableDDict(self: *DictionaryManager, table_name: []const u8, raw_dict: []const u8) !void {
        const ddict = ZSTD_createDDict(raw_dict.ptr, raw_dict.len) orelse return error.ZstdError;
        errdefer _ = ZSTD_freeDDict(ddict);

        const prev = try self.ddicts.fetchPut(table_name, ddict);
        if (prev) |p| _ = ZSTD_freeDDict(p.value);
    }

    pub fn getCDict(self: *DictionaryManager, table_name: []const u8) ?*ZSTD_CDict {
        return self.cdicts.get(table_name);
    }

    pub fn getDDict(self: *DictionaryManager, table_name: []const u8) ?*ZSTD_DDict {
        return self.ddicts.get(table_name);
    }
};

/// Train a Zstd dictionary from a pre-flattened samples buffer.
///
/// - `dict_buffer`: output buffer (capacity = max dictionary size)
/// - `samples_buffer`: concatenation of all samples
/// - `sample_sizes`: length of each sample, in order
///
/// Returns the number of bytes written into `dict_buffer`.
///
/// This function performs **no allocations** and is suitable for
/// large corpora or repeated training runs.
pub fn train_dictionary_from_buffer(
    dict_buffer: []u8,
    samples_buffer: []const u8,
    sample_sizes: []const usize,
) Error!usize {
    if (sample_sizes.len == 0) return error.NoSamples;

    const written = ZDICT_trainFromBuffer(
        dict_buffer.ptr,
        dict_buffer.len,
        samples_buffer.ptr,
        sample_sizes.ptr,
        @intCast(sample_sizes.len),
    );

    return try wrap(written);
}
